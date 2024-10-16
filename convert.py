"""***************************************************************************
 *                   Copyright (©) 2021
 *                   Sotera Wireless, Inc.
 *
 *   This program is the property of Sotera Wireless, Inc.
 *   Its contents are proprietary information and no part of it
 *   is to be disclosed to anyone except employees of Sotera
 *   Wireless, Inc., or as agreed in writing by Sotera Wireless, Inc.
 *
 *****************************************************************************"""

import logging
from json import dumps
from IPython.display import clear_output
from sotera.aws import get_boto3_session
from analytics.lib.utils import get_string_from_timestamp
from sotera.cluster.control import cluster_decorate, add_analysis


logger = logging.getLogger(__name__)


def downloads_on(pgsql_):
    with pgsql_, pgsql_.cursor() as cursor:
        cursor.execute(
            """UPDATE site_management.aa_site_data
                  SET ok_to_download = TRUE
                WHERE auto_download_active IS TRUE"""
        )
    logger.info("downloads on")


def downloads_off(pgsql_):
    with pgsql_, pgsql_.cursor() as cursor:
        cursor.execute("UPDATE site_management.aa_site_data SET ok_to_download = FALSE")
    logger.info("downloads off")


def print_analysis_status(pgsql_, aid):
    with pgsql_, pgsql_.cursor() as cursor:
        cursor.execute(
            f"""SELECT is_complete, is_error, count(*)
                                FROM analysis_jobs
                            WHERE aid={aid}
                            GROUP BY is_complete, is_error"""
        )
        values = cursor.fetchall()
        total = sum(x[2] for x in values)
        complete = sum(x[2] for x in values if x[0])
        error = sum(x[2] for x in values if x[1])
        incomplete = total - (complete + error)
        clear_output()
        logger.info(
            f"\n{'aid':>5} | {'complete':>10} | {'error':>5} | {'incomplete':>10}\n"
            f"{aid:5} | {complete:10} | {error:5} | {incomplete:10}"
        )
        if incomplete < 0:
            logger.warning("Unfinished jobs")
        if error > 0:
            logger.warning("!!! There were conversion errors !!!!")


@cluster_decorate()
def cluster_block_convert(pgsql_, aid, jobid, job):
    from uuid import uuid1
    from shutil import rmtree
    from tempfile import mkdtemp
    from os.path import join as joinpath, basename
    from sotera.io import find_tier, make_key
    from sotera.io.local import save_block
    from sotera.io.visi.convert import convert_block
    from sotera.io.cloud import download_file, upload_indexed_file

    fnlist = []
    dst = None
    chunk_tmpdir = None
    blockmap = job["block"].copy()
    chunk_tmpdir = mkdtemp()
    for chunk in blockmap["chunks"]:
        key_name = f"{job['hid']}/{chunk['file']}"
        localfn = joinpath(chunk_tmpdir, chunk["file"])
        download_file(job["bucket"], key_name, localfn)
        chunk["file"] = localfn

    data = convert_block(blockmap)
    dst = mkdtemp()
    metafn = f"meta-{uuid1()}.json"
    fnlist = save_block(dst, data, use_compression=True, metafn=metafn)

    for fn in fnlist:
        bfn = basename(fn)
        file_ext = fn.split(".")[-1]
        tier = find_tier(bfn)
        key_name = make_key(job["hid"], job["block"]["num"], bfn, tier)
        upload_indexed_file(
            fn,
            job["bucket"],
            key_name,
            job["hid"],
            block=job["block"]["num"],
            file_class="partial_metadata" if file_ext == "json" else "block_array",
            pgsql_=pgsql_,
        )

    if "TIME_SYNC" in data["__meta__"]["ARRAYS"].keys():
        with pgsql_, pgsql_.cursor() as cursor:
            cursor.execute(
                f"""UPDATE aa_blocks
                        SET device_id={data['__meta__']['DEVICES'][0]},
                            unix_start={int(data['__meta__']['T0'])},
                            unix_stop={int(data['__meta__']['T1'])},
                            session_guid = '{job['session_guid']}'
                    WHERE hid={job['hid']}
                        AND block_number={job['block']['num']}"""
            )

    if dst is not None:
        rmtree(dst, ignore_errors=True)
    if chunk_tmpdir is not None:
        rmtree(chunk_tmpdir, ignore_errors=True)

    return fnlist


def make_metadata_merge_analysis(pgsql_, aid, name="convert-merge-metadata"):
    aid1 = add_analysis(pgsql_, name=name)
    s = f"""
    with y as ( WITH x AS (
       SELECT (args->>'hid')::int AS hid, (args->'block'->>'num')::int AS block,
              jsonb_array_elements_text("returns") AS fn
       FROM analysis_jobs
       WHERE aid = {aid} AND is_complete=true)
     SELECT x.hid, x.block, min(fi.bucket) as bucket, jsonb_agg(fi.key) as partials
     FROM x
     INNER JOIN file_info fi ON fi.hid = x.hid AND fi.block = x.block
     WHERE x.fn LIKE '%meta%.json'
     AND  fi.key LIKE '%'||split_part(fn, '/', 4)||'%'
     AND fi.file_class = 'partial_metadata'
     GROUP BY x.hid, x.block )
    INSERT INTO analysis.analysis_jobs (aid,args)
    SELECT {aid1} AS aid, jsonb_build_object('hid',y.hid, 'block', y.block, 'bucket',
                       y.bucket, 'partials', y.partials, 'meta', fi.key) AS args
    FROM y
    LEFT JOIN file_info fi ON fi.hid = y.hid AND fi.block = y.block
           AND fi.file_class = 'metadata';
    """
    with pgsql_, pgsql_.cursor() as cursor:
        cursor.execute(s)
    return aid1


@cluster_decorate()
def cluster_merge_metadata(pgsql_, aid, jobid, job):
    from json import dump
    from os import remove
    from tempfile import mktemp
    from sotera.io.local import merge_metadata
    from sotera.io.cloud import load_json_from_s3, upload_indexed_file

    r = None
    hid = job["hid"]
    block = job["block"]
    bucket = job["bucket"]
    meta_key = job["meta"]
    partials = job["partials"]

    if meta_key is not None:
        meta = load_json_from_s3(bucket, meta_key)
    else:
        meta = None

    for pm_key in partials:
        pm = load_json_from_s3(bucket, pm_key)
        meta = merge_metadata(pm, meta)

    if meta is None or len(meta) == 0:
        r = f"""{hid}:{block} merge failed.
        {0 if meta is None else len(meta)} metadata files found"""
        raise ValueError

    elif meta is not None:
        fn = mktemp()
        with open(fn, "w") as fp:
            dump(meta, fp)
        key_name = f"tier2/{hid}/{block:04d}/meta.json"
        upload_indexed_file(fn, bucket, key_name, hid, block, "metadata", pgsql_=pgsql_)
        remove(fn)
        r = key_name
    return r


def get_archived_lucene_index_count(
    pgsql_, sites=None, start_time=None, min_device_session=None
):
    site_clause = (
        "" if sites is None else f"AND li.pds_id in ({','.join(f'{s}' for s in sites)})"
    )
    time_clause = "" if start_time is None else f"AND ds.start_time > '{start_time}'"
    device_session_clause = (
        ""
        if min_device_session is None
        else f"AND li.device_session_id > {min_device_session}"
    )

    with pgsql_, pgsql_.cursor() as cursor:
        cursor.execute(
            f""" SELECT sum(jsonb_array_length(li.blocks)) AS cnt
                   FROM lucene_indexes li
             INNER JOIN device_sessions ds
                     ON li.session_guid = ds.session_guid
                    AND li.device_session_id = ds.session_id
                    AND li.pds_id = ds.pds_id
                  WHERE li.status = 'archived'
                        {site_clause}
                        {time_clause}
                        {device_session_clause}
                    AND li.blocks IS NOT NULL """
        )
        return cursor.fetchone()[0] if cursor.rowcount > 0 else None


def get_archived_lucene_indexes_for_hids(pgsql_, index_class, hids):
    with pgsql_, pgsql_.cursor() as cursor:
        cursor.execute(
            f""" SELECT li.session_guid, li.hid, li.blocks, li.pds_id, ds.hid
                   FROM lucene_indexes li
             INNER JOIN device_sessions ds
                     ON li.session_guid = ds.session_guid
                    AND li.device_session_id = ds.session_id
                    AND li.pds_id = ds.pds_id
                  WHERE li.index_class = '{index_class}'
                    AND ds.hid IN ({','.join(str(h) for h in hids)})
                    AND li.status = 'archived'
                    AND li.blocks IS NOT NULL
               ORDER BY li.hid, li.device_session_id"""
        )
        return [
            {
                "session_guid": r[0],
                "hid": r[1],
                "blocks": r[2],
                "pds_id": r[3],
                "hid2": r[4],
            }
            for r in cursor
        ]


def get_archived_lucene_indexes(
    pgsql_, index_class, max_num, sites=None, start_time=None, min_device_session=None
):
    site_clause = (
        "" if sites is None else f"AND li.pds_id in ({','.join(f'{s}' for s in sites)})"
    )
    time_clause = "" if start_time is None else f"AND ds.start_time > '{start_time}'"
    device_session_clause = (
        ""
        if min_device_session is None
        else f"AND li.device_session_id > {min_device_session}"
    )
    with pgsql_, pgsql_.cursor() as cursor:
        cursor.execute(
            f"""SELECT li.session_guid, li.hid, li.blocks, li.pds_id, ds.hid
                  FROM lucene_indexes li
            INNER JOIN device_sessions ds
                    ON li.session_guid = ds.session_guid
                   AND li.device_session_id = ds.session_id
                   AND li.pds_id = ds.pds_id
                 WHERE li.index_class = '{index_class}'
                       {site_clause} {time_clause} {device_session_clause}
                  AND li.status = 'archived'
                  AND li.blocks IS NOT NULL
             ORDER BY li.hid, li.device_session_id
                LIMIT {max_num} """
        )
        return [
            {
                "session_guid": r[0],
                "hid": r[1],
                "blocks": r[2],
                "pds_id": r[3],
                "hid2": r[4],
            }
            for r in cursor
        ]


def get_max_block_num(pgsql_, hid):
    """ get max block number in blocks table for given hid """
    with pgsql_, pgsql_.cursor() as cursor:
        cursor.execute(
            f"""SELECT MAX(block_number) AS max_block_num
                 FROM aa_blocks
                WHERE hid = {hid}"""
        )
        return cursor.fetchone()[0]


def insert_block(pgsql_, hid, block_number):
    """ inserts a block into aa_blocks. """
    with pgsql_, pgsql_.cursor() as cursor:
        cursor.execute(
            f"""INSERT
                  INTO aa_blocks (hid, block_number)
                VALUES ({hid},{block_number})"""
        )


def make_job(pgsql_, aid, args):
    """adds a single job to a given analysis. args should be a dictionary containing
    all pertinent job info."""
    with pgsql_, pgsql_.cursor() as cursor:
        cursor.execute(
            f"""INSERT
                  INTO analysis_jobs (aid,args)
                VALUES ({aid},'{dumps(args)}')"""
        )


def update_block_numbers(pgsql_, session_guid, index_class, pds_id, blocks):
    """ update block numbers in blocks field of LI table. """
    with pgsql_, pgsql_.cursor() as cursor:
        cursor.execute(
            f"""UPDATE lucene_indexes
                   SET blocks_data = '{dumps(blocks)}'::jsonb
                 WHERE session_guid = '{session_guid}'
                   AND index_class = '{index_class}'
                   AND pds_id = {pds_id}"""
        )


def update_lucene_index_status_only(pgsql_, session_guid, index_class, pds_id, status):
    with pgsql_, pgsql_.cursor() as cursor:
        cursor.execute(
            f""" UPDATE lucene_indexes li
                  SET status = '{status}'
                WHERE session_guid = '{session_guid}'
                      AND index_class = '{index_class}'
                      AND pds_id = {pds_id}"""
        )


def get_companion_data_blocks(pgsql_, session_guid, pds_id):
    """ get blocks field for companion waveform LI """
    with pgsql_, pgsql_.cursor() as cursor:
        cursor.execute(
            f"""SELECT blocks_data
                  FROM lucene_indexes
                 WHERE session_guid = '{session_guid}'
                   AND index_class = 'data'
                   AND pds_id = {pds_id}
                 UNION
                SELECT blocks_data
                  FROM lids_backup
                 WHERE session_guid = '{session_guid}'
                   AND index_class = 'data'
                   AND pds_id = {pds_id}"""
        )
        return cursor.fetchone()[0]


def update_lucene_index_status(pgsql_, aid0, STEP=20000):
    with pgsql_, pgsql_.cursor() as cursor:
        cursor.execute(
            f""" SELECT aj.args->'hid',
            aj.args->'block'->'chunks'->0->'file'
                FROM analysis_jobs aj
                WHERE aid = {aid0}"""
        )
        list_ = []
        for row in cursor:
            hid = row[0]
            if row[1] is not None and len(row[1]) > 0:
                tmp = row[1].split("-")
                session_guid = "-".join(tmp[:5])
                index_class = tmp[5]
                list_.append((hid, session_guid, index_class))
        list_ = list(set(list_))
    N = 0
    while len(list_[N : N + STEP]) > 0:  # noqa: E203
        tmp = ",".join(
            f"({i[0]},'{i[1]}','{i[2]}')" for i in list_[N : N + STEP]  # noqa: E203
        )
        sql = f"""SELECT hid, blocks_data, session_guid, index_class
            FROM lucene_indexes
            WHERE status='waiting_for_conversion'
            AND (hid, session_guid, index_class) IN ({tmp})"""
        sql2 = """ SELECT jobid, is_complete
                    FROM analysis_jobs
                    WHERE aid = {} AND args @> '{{"hid":  {} }}'
                        AND args->'block'->'chunks'->0->>'file' LIKE '{}-{}%'
                        AND is_complete IS true"""

        sql3 = """UPDATE lucene_indexes set status='converted' where
        hid = {} and session_guid = '{}' and index_class = '{}'
        """

        with pgsql_, pgsql_.cursor() as cursor:
            cursor.execute(sql)
            for row in cursor:
                nblocks = len(row[1])
                with pgsql_.cursor() as cursor1:
                    cursor1.execute(sql2.format(aid0, row[0], row[2], row[3]))
                    nblocks_complete = sum([row1[1] for row1 in cursor1])
                    if nblocks_complete == nblocks:
                        cursor1.execute(sql3.format(row[0], row[2], row[3]))
                    else:
                        logger.warning(
                            f"{nblocks_complete} {nblocks} {row[0]} {[b['num'] for b in row[1]]} {row[2]} {row[3]}"
                        )
        N += STEP


def finalize_complete_session(pgsql_, hid):
    try:
        sql = f"""WITH x as (
                    SELECT hid,
                        cast(block#>>'{{num}}' as smallint) as block_number,
                        left(block#>>'{{chunks,0,file}}',36) as session_guid
                    FROM ( SELECT hid, jsonb_array_elements(blocks_data) as block
                        FROM lucene_indexes
                        WHERE index_class = 'data' and hid={hid}) x
                    ORDER by 1,2 )
                UPDATE aa_blocks as b
                SET session_guid = x.session_guid
                FROM X
                WHERE x.hid = b.hid AND x.hid = {hid}
                  AND x.block_number = b.block_number;"""
        with pgsql_, pgsql_.cursor() as cursor:
            cursor.execute(sql)
    except:  # noqa E722
        logger.exception(f"Error on setting block session_guid for {hid}")
        pass

    try:
        with pgsql_, pgsql_.cursor() as cursor:
            cursor.execute(
                f"""SELECT time_zone
                        FROM aa_site_data AS site
                INNER JOIN aa_session_data AS session
                        ON session.site =  site.name
                        WHERE session.hid = {hid}"""
            )
            tz = cursor.fetchone()[0]
            cursor.execute(
                f"""SELECT min(unix_start), max(unix_stop)
                        FROM aa_blocks
                        WHERE hid = {hid}"""
            )
            unix_start, unix_stop = cursor.fetchone()
            duration = unix_stop - unix_start

        date_start = get_string_from_timestamp(unix_start, tz, fmt="%Y-%m-%d")
        date_stop = get_string_from_timestamp(unix_stop, tz, fmt="%Y-%m-%d")
    except:  # noqa E722
        raise
    try:
        with pgsql_, pgsql_.cursor() as cursor:
            cursor.execute(
                f"""SELECT session_guid
                      FROM aa_blocks
                     WHERE hid = {hid}
                       AND block_number=0"""
            )
            uuid = cursor.fetchone()[0].split("-")[0]
    except:  # noqa E722
        logger.exception(f"Error on getting session_guid for {hid}")
        uuid = None

    try:
        with pgsql_, pgsql_.cursor() as cursor:
            cursor.execute(
                f"""UPDATE aa_session_data
                       SET date_start = '{date_start}',
                           date_stop = '{date_stop}',
                           duration = {duration}
                     WHERE hid = {hid}"""
                if uuid is None
                else f"""UPDATE aa_session_data
                            SET session_id = '{uuid}',
                                date_start = '{date_start}',
                                date_stop = '{date_stop}',
                                duration = {duration}
                          WHERE hid = {hid}"""
            )
    except:  # noqa E722
        logger.exception(f"Error on {hid}\n{sql}")
        raise


def populate_delete_vchk_analysis(pgsql_, aid0, aid):
    with pgsql_, pgsql_.cursor() as cursor:
        cursor.execute(
            f"""WITH a AS ( WITH z AS ( WITH y AS ( WITH x AS (
              SELECT args->>'bucket' AS bucket,
                     args->>'hid'||'/'||(json_array_elements(
                         (args->'block'->'chunks')::json)->>'file'
                     )::varchar AS key
                FROM analysis_jobs
               WHERE aid = {aid0}
                 AND is_complete IS TRUE
              )
              SELECT bucket,
                     1+count(*)/1000 AS cnt,
                     ARRAY_AGG(key) AS keys
                FROM x
            GROUP BY bucket
              )
              SELECT bucket,
                     UNNEST(keys) AS key,
                     cnt::int
                FROM y
              )
              SELECT bucket,
                     NTILE(cnt) OVER (PARTITION BY bucket), key FROM z
              )
              INSERT
                INTO analysis.analysis_jobs (aid,args)
              SELECT {aid} AS aid,
                     jsonb_build_object(
                        'bucket', bucket, 'keys', jsonb_agg(key)
                     ) AS args
                FROM a
            GROUP BY bucket, ntile
            ORDER BY bucket, ntile"""
        )


@cluster_decorate()
def cluster_delete_vchk(pgsql_, aid, jobid, job):
    return (
        get_boto3_session()
        .client("s3")
        .delete_objects(
            Bucket=job["bucket"], Delete={"Objects": [{"Key": i} for i in job["keys"]]}
        )
    )
