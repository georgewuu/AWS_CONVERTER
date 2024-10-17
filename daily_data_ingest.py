# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     formats: py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.11.1
#   kernelspec:
#     display_name: ipyparrallel
#     language: python
#     name: ipyparrallel
# ---

# ### Convertering Data with a Ephemeral Cluster
#
# Copyright (©) 2021
# Sotera Wireless, Inc.
#
# This program is the property of Sotera Wireless, Inc.
# Its contents are proprietary information and no part of it
# is to be disclosed to anyone except employees of Sotera
# Wireless, Inc., or as agreed in writing by Sotera Wireless, Inc.

import logging
import datetime
from time import sleep
from contextlib import suppress

# import sotera
from sotera.aws import get_pgsql_connection
from sotera.aws.ipcluster import (
    cluster_launch,
    cluster_teminate as cluster_terminate,
    cluster_get_client,
)

# import sotera.analysis.convert
from sotera.cluster.control import add_analysis, job_generator
from sotera.db.db_api import get_bucket_by_hid
from analytics.ingest.convert import (
    downloads_on,
    downloads_off,
    print_analysis_status,
    get_archived_lucene_indexes_for_hids,
    cluster_block_convert,
    make_metadata_merge_analysis,
    cluster_merge_metadata,
    get_max_block_num,
    insert_block,
    make_job,
    update_block_numbers,
    update_lucene_index_status_only,
    update_lucene_index_status,
    finalize_complete_session,
    populate_delete_vchk_analysis,
    cluster_delete_vchk,
)
from analytics.ingest.triage import post_session_triage
from analytics.ingest.site_health import populate_lir_table

logging.getLogger().setLevel(logging.INFO)
logging.info("Let's go")

pgsql_ = get_pgsql_connection("sciencedb2-admin")

# +
sites = ("SiteX",)
if sites is None:
    site_clause = ""
else:
    with pgsql_:
        with pgsql_.cursor() as cursor:
            tmp = "','".join(sites)
            cursor.execute(
                f""" SELECT pds_id FROM aa_site_data WHERE name IN ('{tmp}')  """
            )
            pds_ids = [r[0] for r in cursor]
    site_clause = f"AND  li.pds_id NOT IN ({','.join(f'{p}' for p in pds_ids)})"

sql = f""" SELECT distinct ds.hid
             FROM lucene_indexes li
       INNER JOIN device_sessions ds
               ON li.session_guid = ds.session_guid
                  AND li.device_session_id = ds.session_id
                  AND li.pds_id = ds.pds_id
            WHERE li.status = 'archived' 
                  -- {site_clause}
                  AND li.blocks IS NOT NULL"""

print(sql)

with pgsql_:
    with pgsql_.cursor() as cursor:
        cursor.execute(sql)
        hids = [r[0] for r in cursor]
        print(len(hids))
# -

exclude = []
with pgsql_:
    with pgsql_.cursor() as cursor:
        sql = f""" SELECT distinct hid
                     FROM lucene_indexes
                    WHERE hid in ({ ','.join(str(h) for h in hids)})
                      AND index_class = 'data'
                      AND status IN ('limbo','waiting_for_conversion')
        """
        cursor.execute(sql)
        exclude = [r[0] for r in cursor]

hids_to_convert = list(set(hids) - set(exclude))
print(len(hids), len(exclude), len(hids_to_convert))

with pgsql_, pgsql_.cursor() as cursor:
    cursor.execute(
        f"""SELECT site, count(sd.hid)
              FROM aa_session_data sd
             WHERE sd.hid IN {tuple(hids_to_convert)}
          GROUP BY site
          ORDER BY site"""
    )
    for row in cursor:
        print(row)


archived_lucene_indexes_data = get_archived_lucene_indexes_for_hids(
    pgsql_, "data", hids_to_convert
)
print(len(archived_lucene_indexes_data))

print(aid0 := (aid := add_analysis(pgsql_, "converter")))

cluster_name, _, _ = cluster_launch(
    "converter",
    instance_count=(ninstances := 4), #200),
    instance_type="m4.2xlarge",
    region="us-west-1",
    availability_zone="us-west-1a",
    num_nodes_per_instance=(nodes_per_instance:=1), #5),
    spot_price="2.5",
    nodb=True,
)
print(cluster_name)

downloads_off(pgsql_)

for li in archived_lucene_indexes_data[1:]:
    max_block_num = get_max_block_num(pgsql_, li["hid"])
    max_block_num = -1 if max_block_num is None else max_block_num
    hid = li["hid"]
    current_block_num = max_block_num + 1
    blocks = li["blocks"]
    bucket = get_bucket_by_hid(li["hid"], pgsql_)
    args = {
        "hid": li["hid"],
        "session_guid": li["session_guid"],
        "bucket": bucket,
        "block": None,
    }
    for block in blocks:
        with pgsql_, pgsql_.cursor() as cursor:
            # insert into aa_blocks
            insert_block(pgsql_, hid=li["hid"], block_number=current_block_num)
            block["num"] = current_block_num
            args["block"] = block
            # create the job in analysis_jobs
            make_job(pgsql_, aid, args)
            current_block_num += 1

    # update lucene_indexes table
    update_block_numbers(pgsql_, li["session_guid"], "data", li["pds_id"], blocks)
    # update LI status
    update_lucene_index_status_only(
        pgsql_, li["session_guid"], "data", li["pds_id"], "waiting_for_conversion"
    )
print("done")

# then move onto waveforms
archived_lucene_indexes_waveform = get_archived_lucene_indexes_for_hids(
    pgsql_, "waveform", hids_to_convert
)

print(len(archived_lucene_indexes_data), len(archived_lucene_indexes_waveform))

downloads_on(pgsql_)

# + tags=[]
lview = cluster_get_client(cluster_name).load_balanced_view()
while len(lview) < 0.9 * ninstances * nodes_per_instance:
    sleep(10)
sleep(60)
print(aid0, len(lview))
# -

len(lview)

# + tags=[]
result = lview.map_async(cluster_block_convert, job_generator(pgsql_, aid0))

# + tags=[] jupyter={"outputs_hidden": true}
with suppress(KeyboardInterrupt):
    result.wait_interactive()
# -

print_analysis_status(pgsql_, aid0)

print(aid1 := make_metadata_merge_analysis(pgsql_, aid))

result = lview.map_async(cluster_merge_metadata, job_generator(pgsql_, aid1))

# + tags=[] jupyter={"outputs_hidden": true}
with suppress(KeyboardInterrupt):
    result.wait_interactive()

pgsql_.close()


