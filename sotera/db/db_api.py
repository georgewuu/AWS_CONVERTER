from collections import namedtuple
from psycopg2.extras import RealDictCursor
from sotera.util.time import get_string_from_timestamp


SessionInfo = namedtuple("SessionInfo", "hid status site tz care_unit room")


def get_session_info(pgsql_, hid):
    with pgsql_, pgsql_.cursor() as cursor:
        cursor.execute(
            f"""SELECT sess.hid, sess.status, sess.site, site.time_zone,
                       sess.care_unit, sess.room
                  FROM aa_session_data sess
            INNER JOIN aa_site_data site
                    ON sess.site = site.name
                 WHERE sess.hid = {hid}"""
        )
        return SessionInfo(*cursor.fetchone()) if cursor.rowcount else tuple()


# -- old --


def get_key_by_hid(hid, file_class, blockno, pgsql_):
    if blockno is None:
        sql = """SELECT bucket, key
                  FROM file_info
                 WHERE hid = {0} AND file_class='{2}'"""
    else:
        sql = """SELECT bucket, key
                   FROM file_info
                  WHERE hid = {0}
                    AND block = {1}
                    AND file_class='{2}'"""
    with pgsql_, pgsql_.cursor() as cursor:
        cursor.execute(sql.format(hid, blockno, file_class))
        return cursor.fetchone() if cursor.rowcount else (None, None)


def get_site_by_hid(hid, pgsql_):
    sql = "SELECT site FROM aa_session_data WHERE hid = {}"
    with pgsql_, pgsql_.cursor() as cursor:
        cursor.execute(sql.format(hid))
        return cursor.fetchone()[0] if cursor.rowcount else None


def get_site_info_by_hid(hid, pgsql_):
    sql = "SELECT site, time_zone FROM aa_session_data WHERE hid = {}"
    with pgsql_, pgsql_.cursor() as cursor:
        cursor.execute(sql.format(hid))
        return cursor.fetchone() if cursor.rowcount else (None, None)


def get_bucket_by_hid(hid, pgsql_):
    sql = """SELECT sd.bucket
               FROM aa_site_data sd
         INNER JOIN aa_session_data sess
                 ON sess.site = sd.name
              WHERE sess.hid = {}; """.format(
        hid
    )
    with pgsql_, pgsql_.cursor() as cursor:
        cursor.execute(sql)
        return cursor.fetchone()[0] if cursor.rowcount else None


def get_session_info_by_hid(hid, pgsql_):
    sql = "SELECT * FROM aa_session_data WHERE hid = {}"
    with pgsql_, pgsql_.cursor(RealDictCursor) as cursor:
        cursor.execute(sql.format(hid))
        return cursor.fetchone() if cursor.rowcount else tuple()


def get_hid_by_session_id(session_id, pgsql_):
    sql = "SELECT hid FROM aa_session_data WHERE session_id='{}'"
    with pgsql_, pgsql_.cursor() as cursor:
        cursor.execute(sql.format(session_id))
        return cursor.fetchone()[0] if cursor.rowcount else None


def get_blocks_by_hid(hid, pgsql_, humanize_time=True):
    sql = """SELECT b.hid,
                    b.block_number,
                    b.device_id,
                    b.unix_start,
                    b.unix_stop,
                    asd.time_zone
               FROM aa_blocks b
         INNER JOIN aa_session_data sd
                 ON sd.hid = b.hid
         INNER JOIN aa_site_data asd
                 ON asd.name = sd.site
              WHERE b.hid = {}
              ORDER by 1;""".format(
        hid
    )
    with pgsql_, pgsql_.cursor(RealDictCursor) as cursor:
        cursor.execute(sql.format(hid))
        list_ = cursor.fetchall()

    for item in list_:
        item["tStart"] = get_string_from_timestamp(
            item["unix_start"], item["time_zone"]
        )
        item["tStop"] = get_string_from_timestamp(item["unix_stop"], item["time_zone"])
    return list_


def get_block_arrays(hid, block, pgsql_):
    sql = """SELECT split_part(split_part(key, '/',4),'.',1)
               FROM file_info
              WHERE hid = {}
                AND block={}
                AND file_class = 'block_array'
            """.format(
        hid, block
    )
    with pgsql_, pgsql_.cursor() as cursor:
        cursor.execute(sql.format(hid))
        list_ = [row[0] for row in cursor]
    return list_
