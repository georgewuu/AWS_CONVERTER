from psycopg2.extras import DictCursor
import json
import pytz


def file_info_add_key(
    cursor, key, hid, block=None, file_class="misc", notes=None, allow_overwrite=False
):

    d_ = dict(
        key="'{}'".format(key.key),
        bucket="'{}'".format(key.bucket_name),
        hid=str(hid),
        file_class="'{}'".format(file_class),
        file_size=str(key.content_length),
        timestamp="'{}'".format(str(key.last_modified)),
    )

    if block is not None:
        d_["block"] = str(block)

    if notes is not None:
        d_["notes"] = json.dumps(notes)

    cursor.execute(
        f"""SELECT COUNT(*)
              FROM file_info
             WHERE key='{key.key}'
               AND bucket='{key.bucket_name}'"""
    )
    if cursor.fetchone()[0] and allow_overwrite:
        del d_["key"]
        del d_["bucket"]
        fields = ", ".join(["{}={}".format(*i) for i in d_.items()])
        sql = f"""UPDATE file_info
                    SET {fields}
                  WHERE key='{key.key}'
                    AND bucket='{key.bucket_name}'"""
    else:
        i, v = zip(*d_.items())
        sql = f"INSERT INTO file_info ({','.join(i)}) VALUES ({','.join(v)})"

    cursor.execute(sql)


def file_info_move_key(
    dest_hid,
    dest_block,
    dest_bucket,
    dest_key,
    src_hid,
    src_block,
    src_bucket,
    src_key,
    boto_,
    pgsql_,
):
    client = boto_.client("s3")
    client.copy_object(
        Bucket=dest_bucket,
        Key=dest_key,
        CopySource={"Bucket": src_bucket, "Key": src_key},
    )
    client.delete_object(Bucket=src_bucket, Key=src_key)
    with pgsql_, pgsql_.cursor() as cursor:
        cursor.execute(
            f"""UPDATE file_info
                  SET bucket = '{dest_bucket}',
                      key = '{dest_key}',
                      hid = {dest_hid},
                      block = {dest_block}
                WHERE bucket = '{src_bucket}' AND
                      key = '{src_key}' AND
                      hid = {src_hid} AND
                      block = {src_block}"""
        )


def get_sites(pgsql_):
    """ get all sites """
    with pgsql_, pgsql_.cursor(cursor_factory=DictCursor) as cursor:
        cursor.execute(
            """SELECT name
               FROM site_management.aa_site_data
              WHERE name LIKE 'Site%'
                    AND name not LIKE '%-%'
          ORDER BY name"""
        )
        return cursor.fetchall()


def get_care_units(pgsql_, site, cuClass="mapped_care_unit"):
    """ Get list of care units. """
    with pgsql_, pgsql_.cursor() as cursor:
        cursor.execute(
            f"""SELECT distinct {cuClass}
                  FROM site_management.care_units
                 WHERE site = '{site}'
              ORDER BY 1"""
        )
        return [r[0] for r in cursor] if cursor.rowcount else [None]


def get_pds_id(pgsql_, site):
    with pgsql_, pgsql_.cursor() as cursor:
        cursor.execute(
            f"""SELECT pds_id
                  FROM site_management.aa_site_data
                 WHERE name = '{site}'"""
        )
        return cursor.fetchone()[0] if cursor.rowcount else None


def get_site_timezone(pgsql_, site):
    with pgsql_, pgsql_.cursor() as cursor:
        cursor.execute(
            f"""SELECT time_zone
                  FROM site_management.aa_site_data
                 WHERE name = '{site}'"""
        )
        return pytz.timezone(cursor.fetchone()[0]) if cursor.rowcount else None
