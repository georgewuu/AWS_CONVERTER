from json import dumps
from psycopg2.extras import DictCursor
from sotera.db.db_api import get_bucket_by_hid
from sotera.cluster.control import cluster_decorate, add_analysis


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


def get_archived_lucene_index_count(pgsql_):
    with pgsql_, pgsql_.cursor(cursor_factory=DictCursor) as cursor:
        cursor.execute(
            """SELECT sum(jsonb_array_length(blocks))
               FROM lucene_indexes
              WHERE status = 'archived'
                AND blocks IS NOT NULL"""
        )
        return cursor.fetchone()[0] if cursor.rowcount else None


def get_archived_lucene_indexes(pgsql_, index_class, max_num):
    with pgsql_, pgsql_.cursor(cursor_factory=DictCursor) as cursor:
        cursor.execute(
            f"""SELECT session_guid, hid, blocks, pds_id
                  FROM lucene_indexes
                 WHERE index_class = '{index_class}'
                   AND status = 'archived'
                   AND blocks IS NOT NULL
              ORDER BY hid, device_session_id
                 LIMIT {max_num}"""
        )
        lucene_indexes = cursor.fetchall()
    return [{key: val for key, val in row.items()} for row in lucene_indexes]


def get_max_block_num(pgsql_, hid):
    """ get max block number in blocks table for given hid """
    with pgsql_, pgsql_.cursor() as cursor:
        cursor.execute(
            f""" SELECT MAX(block_number)
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
    """ adds a single job to a given analysis. args should be a dictionary containing
        all pertinent job info. """

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


def update_lucene_index_status_only(pgsql_, session_guid, index_class, pds_id, status):
    with pgsql_, pgsql_.cursor() as cursor:
        cursor.execute(
            f"""UPDATE lucene_indexes li
                 SET status = '{status}'
               WHERE session_guid = '{session_guid}'
                 AND index_class = '{index_class}'
                 AND pds_id = {pds_id}"""
        )


def add_to_conversion_aid(pgsql_, aid, min_blocks=200, max_blocks=10000000000):
    num_blocks = get_archived_lucene_index_count(pgsql_)
    if num_blocks < min_blocks:
        return None
    else:
        # start with numerics
        # update blocks table (just hid and block number), re-write block numbers in
        # blocks field of LI table and add job for each block into analysis_jobs table
        archived_lucene_indexes_data = get_archived_lucene_indexes(
            pgsql_, "data", max_blocks
        )
        for li in archived_lucene_indexes_data:
            max_block_num = get_max_block_num(pgsql_, li["hid"])
            if max_block_num is None:
                # assures that if the hid is brand new,
                # we start block numbering at zero.
                max_block_num = -1

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
                # insert into aa_blocks
                insert_block(pgsql_, hid=li["hid"], block_number=current_block_num)
                block["num"] = current_block_num
                args["block"] = block
                # create the job in analysis_jobs
                make_job(pgsql_, aid, args)
                current_block_num += 1
            # update lucene_indexes table
            update_block_numbers(
                pgsql_, li["session_guid"], "data", li["pds_id"], blocks
            )
            # update LI status
            update_lucene_index_status_only(
                pgsql_,
                li["session_guid"],
                "data",
                li["pds_id"],
                "waiting_for_conversion",
            )

        # then move onto waveforms
        archived_lucene_indexes_waveform = get_archived_lucene_indexes(
            pgsql_, "waveform", max_blocks
        )
        for li in archived_lucene_indexes_waveform:
            blocks = li["blocks"]
            bucket = get_bucket_by_hid(li["hid"], pgsql_)
            args = {
                "hid": li["hid"],
                "session_guid": li["session_guid"],
                "bucket": bucket,
                "block": None,
            }
            companion_data_blocks = get_companion_data_blocks(
                pgsql_, li["session_guid"], li["pds_id"]
            )
            if (
                blocks is not None
                and companion_data_blocks is not None
                and len(blocks) == len(companion_data_blocks)
            ):
                for block, companion_block in zip(blocks, companion_data_blocks):
                    block["num"] = companion_block["num"]
                    args["block"] = block
                    make_job(pgsql_, aid, args)
                update_block_numbers(
                    pgsql_, li["session_guid"], "waveform", li["pds_id"], blocks
                )
                update_lucene_index_status_only(
                    pgsql_,
                    li["session_guid"],
                    "waveform",
                    li["pds_id"],
                    "waiting_for_conversion",
                )
        return aid


def cluster_make_metadata_merge(pgsql_, aid, name="convert-merge-metadata"):
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
