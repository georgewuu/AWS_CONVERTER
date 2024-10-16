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

# + tags=[]
err1 = sum([0 if r else 1 for r in result])
print(err1, "Errors")
# -

print_analysis_status(pgsql_, aid1)

update_lucene_index_status(pgsql_, aid0)

with pgsql_, pgsql_.cursor() as cursor:
    complete_sessions = []
    cursor.execute(
        """WITH outer_cte AS (
                WITH inner_cte AS (
                     SELECT li.hid,
                            rank() OVER (
                                PARTITION BY li.hid
                                    ORDER BY li.device_session_id asc
                                ) AS rn,
                            array_agg(li.status) OVER (
                                PARTITION BY li.hid
                                    ORDER BY li.device_session_id desc
                                ) AS stats
                       FROM lucene_indexes li
                 INNER JOIN aa_session_data sd
                         ON li.hid = sd.hid
                      WHERE sd.status = 'complete'
                        AND li.index_class = 'data'
                   ORDER BY li.hid, li.device_session_id
                )
                SELECT hid, UNNEST(stats) AS status
                  FROM inner_cte
                 WHERE rn = 1 )
        SELECT distinct hid
          FROM outer_cte
      GROUP BY hid
        HAVING SUM(
                CASE WHEN status = 'converted' THEN 1 ELSE 0 END
               ) > 0
           AND SUM(
                 CASE WHEN status
                    NOT IN ('converted','index_gone')
                      THEN 1 ELSE 0
                 END
               ) = 0"""
    )
    complete_sessions = [r[0] for r in cursor]
njobs = len(complete_sessions)
print(f"aid = {aid} {len(complete_sessions)} complete sessions")
print(sorted(complete_sessions))

print(aid := add_analysis(pgsql_, "post_session_triage"))
with pgsql_, pgsql_.cursor() as cursor:
    cursor.execute(
        f"""INSERT 
             INTO analysis.analysis_jobs (aid, args)
           SELECT {aid} as aid,
                  jsonb_build_object(
                   'hid', sd.hid,
                   'time_zone', site.time_zone
                  ) AS args
            FROM session_management.aa_session_data sd
      INNER JOIN site_management.aa_site_data site 
              ON site.name = sd.site
           WHERE hid IN ({','.join((str(h) for h in complete_sessions))})"""
    )

result = lview.map_async(post_session_triage, job_generator(pgsql_, aid))

len(lview)

with suppress(KeyboardInterrupt):
    result.wait_interactive()

print_analysis_status(pgsql_, aid)

# + tags=[] jupyter={"outputs_hidden": true}
err1 = sum([0 if r else 1 for r in result])
print(err1, "Errors")
# -

sql = f"select args->>'hid' as hid from analysis_jobs where aid={aid} and is_complete=true"
with pgsql_:
    with pgsql_.cursor() as cursor:
        cursor.execute(sql.format(aid))
        complete_sessions = [r[0] for r in cursor]
print(f"aid={aid} {len(complete_sessions)} complete sessions")
print(sorted(complete_sessions))

with pgsql_, pgsql_.cursor() as cursor:
    cursor.execute(
        f"""SELECT args->>'hid' AS hid
      FROM analysis.analysis_jobs aj
     WHERE aid = {aid} AND is_complete IS FALSE"""
    )
    incomplete_sessions = [r[0] for r in cursor]
    unfinished = njobs - len(complete_sessions) + len(incomplete_sessions)
print(
    f"aid={aid} {njobs} jobs {len(complete_sessions)} complete {len(incomplete_sessions)} incomplete {unfinished} unfinished"
)
print(sorted(incomplete_sessions))

# +
sql = f"""SELECT count(*)
          FROM analysis.analysis_jobs aj
         WHERE aid = {aid} AND (is_complete=true or is_error = true) """

with pgsql_:
    with pgsql_.cursor() as cursor:
        cursor.execute(sql.format(aid))
        print(cursor.fetchall())
# -

s = """
update aa_session_data
set status = 'processed'
where hid in ({}) and status = 'complete' and site like 'Sotera:%'
""".format(
    ",".join(["{}".format(h) for h in incomplete_sessions])
)
with pgsql_:
    with pgsql_.cursor() as cursor:
        cursor.execute(s)

for hid in complete_sessions:
    finalize_complete_session(pgsql_, hid)

if len(incomplete_sessions) > 0:
    sql = f"SELECT hid from aa_session_data where hid in ({','.join(incomplete_sessions)}) and site = 'SiteN' "
    with pgsql_:
        with pgsql_.cursor() as cursor:
            cursor.execute(sql.format(aid))
            cc_sessions = [r[0] for r in cursor]
    print(len(cc_sessions), sorted(cc_sessions))

if len(incomplete_sessions) > 0:
    with pgsql_, pgsql_.cursor() as cursor:
        cursor.execute(
            f"""SELECT hid
                  FROM file_info
                 WHERE hid in ({','.join(incomplete_sessions)})
                   AND file_class = 'lucene_index'
              GROUP BY hid
             HAVING sum(file_size) < 1000000/4
                AND count(*) < 3"""
        )
        hids = [f"{r[0]}" for r in cursor.fetchall()]
        print(len(hids))
        if len(hids) > 0:
            cursor.execute(
                f"""UPDATE aa_session_data
                       SET status = 'zombie'
                     WHERE hid in ({','.join(hids)})"""
            )
        cursor.execute(
            f"""UPDATE aa_session_data
                    SET status = 'processed'
                    WHERE hid in ({','.join([f'{h}' for h in incomplete_sessions])})
                    AND status = 'complete'
                    AND site LIKE 'Sotera:%'"""
        )

# set any demo mode data to zombie
with pgsql_, pgsql_.cursor() as cursor:
    cursor.execute(
        """SELECT hid
             FROM aa_session_data
            WHERE hid IN (
                    SELECT DISTINCT hid
                      FROM ih_device_alarms_histograms
                     WHERE bin=117
                )
              AND display_percent_spo2 > 99
              AND display_percent_pr > 99
              AND status != 'zombie'"""
    )
    zombies = tuple([row[0] for row in cursor])
    print(zombies)
    if len(zombies) > 0:
        cursor.execute(
            f"""UPDATE aa_session_data
                   SET status = 'zombie'
                 WHERE hid IN ({','.join(f'{z}' for z in zombies)})"""
        )

# + tags=[]
# Insert new site/care_unit combinations fond in aa_session_data
with pgsql_, pgsql_.cursor() as cursor:
    cursor.execute(
        """INSERT
             INTO site_management.care_units
                  (site, raw_care_unit)
           SELECT distinct sd.site, sd.care_unit
             FROM session_management.aa_session_data sd
        LEFT JOIN site_management.care_units cu
               ON sd.site = cu.site
              AND sd.care_unit = cu.raw_care_unit
            WHERE sd.status = 'processed'
              AND sd.site LIKE 'Site%'
              AND sd.care_unit IS NOT NULL
              AND cu.site IS NULL
              AND cu.raw_care_unit IS NULL;
           UPDATE site_management.care_units
              SET num_beds=0
            WHERE num_beds IS NULL;
           UPDATE site_management.care_units
              SET mapped_care_unit='Unknown'
            WHERE mapped_care_unit IS NULL;"""
    )
# -

# update site totals
with pgsql_, pgsql_.cursor() as cursor:
    cursor.execute(
        """WITH site_totals AS (
                SELECT site,
                       SUM(duration)/60./60. AS total_hours,
                       COUNT(*) AS total_sessions
                  FROM aa_session_data
                 WHERE status = 'processed'
              GROUP BY site
            )
            UPDATE aa_site_data
               SET total_sessions=site_totals.total_sessions,
                   total_hours=site_totals.total_hours
              FROM site_totals
              WHERE aa_site_data.name=site_totals.site"""
    )

# update leading indicators report
with pgsql_, pgsql_.cursor() as cursor:
    hidstr = ",".join(str(h) for h in complete_sessions)
    print("Updating Leading_Indicators_Daily Table")
    cursor.execute(
        f"""SELECT DISTINCT site
              FROM aa_session_data
             WHERE hid IN ({hidstr})
          ORDER BY site"""
    )
    sites = [r[0] for r in cursor]
    for site in sorted(sites):
        cursor.execute(
            f"""SELECT min(date_stop)
                 FROM aa_session_data
                WHERE hid IN ({hidstr})
                  AND site LIKE '{site}'"""
        )
        start_date = cursor.fetchone()[0]
        stop_date = max(datetime.date.today(), start_date)
        print("{:8} {} - {}".format(site, start_date, stop_date))
        populate_lir_table(pgsql_, site, start_date, stop_date)

print(aid := add_analysis(pgsql_, name="delete-vchk"))
populate_delete_vchk_analysis(pgsql_, aid0, aid)

result = lview.map_async(cluster_delete_vchk, job_generator(pgsql_, aid))

result.wait_interactive()

r = cluster_terminate(cluster_name)

pgsql_.close()


