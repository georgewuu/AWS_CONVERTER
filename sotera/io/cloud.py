import six
from time import sleep
import numpy as np
import logging
import collections
import warnings
import os
import json
import tempfile
import io

try:
    import lzma
except ImportError:
    from backports import lzma
try:
    import scipy.io
except ImportError:
    warnings.warn("scipy.io not imported in io.cloud", ImportWarning)
try:
    import joblib
except ImportError:
    warnings.warn("joblib not imported in io.cloud", ImportWarning)

import base64
from sotera import db
from . import WAVEFORMS

from . import local, arrays_to_get, array_key, array_file
import gzip

try:
    from sotera.aws import get_boto3_session
    from sotera.db.utils import file_info_add_key, file_info_move_key
except ImportError:
    pass
from sotera.db.db_api import get_session_info


def get_blocks(hid, pgsql_=None):
    with pgsql_, pgsql_.cursor() as cursor:
        cursor.execute(
            """SELECT block_number
                    FROM aa_blocks
                WHERE hid = {}
                ORDER BY block_number""".format(
                hid
            )
        )
        blks = [r[0] for r in cursor.fetchall()]
    return blks


def cloud_load_json(s3resource, bucket, key):
    logging.info("Loading json {}:{}".format(bucket, key))
    obj = s3resource.Object(bucket_name=bucket, key=key)
    with io.BytesIO(obj.get()["Body"].read()) as buffer1:
        data = json.load(buffer1)
    return data


def cloud_load_array(s3resource, bucket, key):
    logging.info("Loading array {}:{}".format(bucket, key))
    obj = s3resource.Object(bucket_name=bucket, key=key)
    with io.BytesIO(obj.get()["Body"].read()) as buffer1:
        with io.BytesIO(lzma.LZMAFile(buffer1, "rb").read()) as buffer2:
            data = np.load(buffer2)
    return data


def cloud_numpy_load(bucket, key, s3resource=None):
    if s3resource is None:
        s3resource = get_boto3_session().resource("s3")
    obj = s3resource.Object(bucket_name=bucket, key=key)
    with io.BytesIO(obj.get()["Body"].read()) as buffer:
        tmp = np.load(buffer)
        if type(tmp) is np.lib.npyio.NpzFile:
            data = {k: v for k, v in tmp.items()}
        else:
            data = tmp
    return data


def download_session_data_v0(
    hid, blockno=None, filename=None, dst=None, variable_names=None, pgsql_=None
):
    if blockno is None:
        file_class = "session_numerics"
    else:
        if blockno not in get_blocks(hid, pgsql_):
            raise ValueError(
                "The requested block, {}, is not present in session".format(blockno)
            )
        file_class = "full_block_data"
    bucket_name, key_name = db.db_api.get_key_by_hid(hid, file_class, blockno, pgsql_)
    if dst is None:
        dst = "."
    elif dst[0] == "~":
        dst = os.path.expanduser(dst)
    dst = os.path.realpath(dst)
    if not os.path.exists(dst):
        os.mkdir(dst)
    if filename is None:
        filename = os.path.join(dst, key_name.split("/")[-1])
    download_file(bucket_name, key_name, filename)
    return filename


def download_session_data_v1(
    hid, blockno, dst=None, variable_names=None, exclude_variables=None, pgsql_=None
):
    boto3_ = get_boto3_session()
    client = boto3_.client("s3")

    bucket_name, key_name = db.db_api.get_key_by_hid(hid, "metadata", blockno, pgsql_)
    if dst is None:
        dst = "."
    elif dst[0] == "~":
        dst = os.path.expanduser(dst)
    dst = os.path.realpath(dst)
    if not os.path.exists(dst):
        os.mkdir(dst)
    filelist = []
    metafn = os.path.join(dst, "meta.json")
    download_file(bucket_name, key_name, metafn, client=client)
    filelist.append(metafn)
    with open(metafn) as fp:
        meta = json.load(fp)
    if "ARRAYS" in meta.keys():
        if variable_names is None:
            variable_names = meta["ARRAYS"].keys()
        if exclude_variables is not None:
            variable_names = list(set(variable_names) - set(exclude_variables))
        for arr in arrays_to_get(variable_names, meta["ARRAYS"].keys()):
            fn = os.path.join(dst, array_file(arr))
            download_file(bucket_name, array_key(hid, blockno, arr), fn, client=client)
            filelist.append(fn)
    return filelist


def download_session_data(
    hid, blockno=None, filename=None, dst=None, variable_names=None, pgsql_=None
):
    if hid < 200000:
        data = download_session_data_v0(
            hid, blockno, filename, dst, variable_names, pgsql_
        )
    else:
        data = download_session_data_v1(
            hid, blockno, filename, dst, variable_names, pgsql_
        )
    return data


def load_session_data_v0(
    hid,
    blockno=None,
    variable_names=None,
    verify_compressed_data_integrity=False,
    pgsql_=None,
):
    dst = tempfile.mkdtemp()
    fn = download_session_data(
        hid, blockno, dst=dst, variable_names=variable_names, pgsql_=pgsql_
    )
    data = scipy.io.loadmat(
        fn,
        variable_names=variable_names,
        verify_compressed_data_integrity=verify_compressed_data_integrity,
    )
    os.remove(fn)
    os.rmdir(dst)
    for trash in ("__version__", "__header__", "__globals__"):
        if trash in data.keys():
            del data[trash]
    return data


def cloud_load_block_data_v1(
    hid, block, resource, pgsql_, variable_names=None, exclude_variables=None
):

    logging.info("Loading HID{}: BLOCK{}".format(hid, block))
    data = {}
    bucket, key = db.db_api.get_key_by_hid(hid, "metadata", block, pgsql_)
    block_arrays = db.db_api.get_block_arrays(hid, block, pgsql_)
    data["__meta__"] = cloud_load_json(resource, bucket, key)

    if (
        block_arrays is None
        and "ARRAYS" in data["__meta__"].keys()
        and len(data["__meta__"]["ARRAYS"]) > 0
    ):
        block_arrays = data["__meta__"]["ARRAYS"]

    if block_arrays is not None and len(block_arrays):
        if variable_names is None:
            variable_names = block_arrays
        if exclude_variables is not None:
            variable_names = list(set(variable_names) - set(exclude_variables))
        for arr in arrays_to_get(variable_names, block_arrays):
            tmp = cloud_load_array(resource, bucket, array_key(hid, block, arr))
            if tmp.shape[0] > 0:
                if local.do_inflate(arr, tmp):
                    data[arr] = local.inflate_array(tmp)
                else:
                    data[arr] = tmp

        if "TIME_SYNC" in data.keys():
            data = local.derive_timestamps(data)
            if data["TIME_SYNC"].shape[0] > 0 and data["TIME_SYNC"].shape[1] < 4:
                data["TIME_SYNC"] = np.c_[
                    data["TIME_SYNC"], np.zeros((data["TIME_SYNC"].shape[0],))
                ]

        data = local.inflate_ppg_arrays(data)
        data = local.inflate_ecg_arrays(data)

    for k in ("HIGGINS", "PWD_VERSION", "FILE_START_TIME", "DEVICES"):
        data[k] = data["__meta__"][k]
    for k in data["__meta__"]["CONSTANTS"]:
        data[k] = data["__meta__"]["CONSTANTS"][k]
    return data


def load_session_data_v1(
    hid, block=None, variable_names=None, resource=None, pgsql_=None
):
    all_blocks = get_blocks(hid, pgsql_)
    data = {}
    if isinstance(block, six.integer_types) and block in all_blocks:
        data = cloud_load_block_data_v1(
            hid, block, resource, pgsql_, variable_names=variable_names
        )
    elif isinstance(block, collections.abc.Iterable):
        for b in block:
            if isinstance(b, six.integer_types) and b in all_blocks:
                block_data = cloud_load_block_data_v1(
                    hid,
                    b,
                    resource,
                    pgsql_,
                    exclude_variables=WAVEFORMS,
                    variable_names=variable_names,
                )
                data = local.merge_blocks(data, block_data)
    elif block is None:
        for b in all_blocks:
            block_data = cloud_load_block_data_v1(
                hid,
                b,
                resource,
                pgsql_,
                exclude_variables=WAVEFORMS,
                variable_names=variable_names,
            )
            data = local.merge_blocks(data, block_data)
    else:
        raise ValueError(
            "Requested block number(s) {} could not be resolved".format(block)
        )
    return data


def load_session_data(
    hid,
    blockno=None,
    variable_names=None,
    verify_compressed_data_integrity=False,
    pgsql_=None,
):

    try:
        hid = int(hid)
    except ValueError:
        raise ValueError("hid must be convertable to int")

    try:
        if isinstance(blockno, collections.abc.Iterable):
            blockno = [int(b) for b in blockno]
        elif blockno is not None:
            blockno = int(blockno)
    except ValueError:
        raise ValueError("blockno must be convertable to int")

    if hid < 200000:
        data = load_session_data_v0(
            hid,
            blockno=blockno,
            variable_names=variable_names,
            verify_compressed_data_integrity=verify_compressed_data_integrity,
            pgsql_=pgsql_,
        )
    else:
        s3resource = get_boto3_session().resource("s3")
        data = load_session_data_v1(
            hid,
            block=blockno,
            variable_names=variable_names,
            resource=s3resource,
            pgsql_=pgsql_,
        )

    if pgsql_ is not None:
        data["__info__"] = get_session_info(pgsql_, hid)

    return data


def load_log_data(lid, bucket="pwd-remote-logs", device_id="all"):
    dst = tempfile.mkdtemp()
    fn = str(lid) + ".gz"

    download_file(bucket, fn, dst + "/" + fn)  # download log file

    data = local.load_log_file(dst, fn, device_id)  # parse and return data

    os.remove(dst + "/" + fn)
    os.rmdir(dst)

    return data


def load_log_data_raw(lid, bucket="pwd-remote-logs", device_id="all"):
    """ return file object for given log.
        make sure to run os.remove(dst+'/'+fn) when done. """
    dst = "/mnt/scratch"
    fn = str(lid) + ".gz"

    download_file(bucket, fn, dst + "/" + fn)  # download log file

    return gzip.open(dst + "/" + fn, "rb")


def download_file(bucket, key, filename, client=None):
    """ download the file at bucket/key to filename.
        filename should contain the full path as well.
        key should contain all 'sub-directories' in S3 hierarchy. """
    # try direct connection to s3 first
    if client is None:
        client = get_boto3_session().client("s3")
    client.download_file(Bucket=bucket, Key=key, Filename=filename)


def upload_file(filename, bucket, key, client=None):
    """ upload the file at filename to bucket/key.
        filename should contain the full path to the file.
        key should contain all 'sub-directories' in S3 hierarchy. """
    if client is None:
        client = get_boto3_session().client("s3")
    client.upload_file(Filename=filename, Bucket=bucket, Key=key)


def show_files(bucket, prefix=""):
    """ show files at given bucket and prefix."""
    boto3_ = get_boto3_session()
    client = boto3_.client("s3")
    response = client.list_objects(Bucket=bucket, Prefix=prefix)
    print("{:30}  {:>14}  {}".format("file", "size(bytes)", "storage class"))
    print("-" * 61)
    for f in response["Contents"]:
        print("{:30} |{:14} |{}".format(f["Key"], f["Size"], f["StorageClass"]))


def upload_indexed_file(
    filename,
    bucket_name,
    key_name,
    hid,
    block=None,
    file_class="misc",
    notes=None,
    pgsql_=None,
):
    """ upload a file to s3 and insert info about it into the pgsql file_info table"""
    boto_ = get_boto3_session()
    s3_resource = boto_.resource("s3")
    key_obj = s3_resource.Object(bucket_name, key_name)
    for i in range(5):  # try to upload 5 times
        try:
            key_obj.upload_file(filename)
        except:  # noqa E722
            sleep(1)
        else:
            break

    with pgsql_, pgsql_.cursor() as cursor:
        file_info_add_key(
            cursor,
            key_obj,
            hid,
            block=block,
            file_class=file_class,
            notes=notes,
            allow_overwrite=True,
        )


def load_json_from_s3(bucket_name, key):
    data = None
    fn = tempfile.mktemp()
    download_file(bucket_name, key, fn)
    with open(fn) as fp:
        data = json.load(fp)
    os.remove(fn)
    return data


def dump_sklearn_model(clf, model_name, params, pgsql_):
    tmpfn = tempfile.mktemp()
    f = open(tmpfn)
    str1 = f.read()
    encoded = base64.b64encode(str1)  # encode binary into base64
    with pgsql_, pgsql_.cursor() as cursor:
        cursor.execute(
            f"""INSERT
                  INTO sklearn_models
                       (model_name, model, params)
                VALUES ('{model_name}','{encoded}','{json.dumps(params)}')"""
        )
    os.remove(tmpfn)


def load_sklearn_model(model_name, pgsql_):
    with pgsql_, pgsql_.cursor() as cur:
        sql = """SELECT model, params
                    FROM sklearn_models
                    WHERE model_name = '{}' """.format(
            model_name
        )
        cur.execute(sql)
        data = cur.fetchone()
    model = data[0]
    params = data[1]
    model2 = base64.b64decode(model)  # decode base64 back to binary
    tmpfn = tempfile.mktemp()
    with open(tmpfn, "wb") as fp:
        fp.write(model2)
    clf = joblib.load(tmpfn)
    os.remove(tmpfn)
    return clf, params


def move_block(hid, dest_block, src_block, boto_, pgsql_):
    with pgsql_, pgsql_.cursor() as cursor:
        sql = f"""INSERT
                    INTO aa_blocks (hid,block_number)
                VALUES ({hid}, {dest_block})
            ON CONFLICT (hid, block_number) DO NOTHING"""
        cursor.execute(sql)

        sql = f"""SELECT bucket, key
                    FROM file_info
                    WHERE hid = {hid}
                        AND block = {src_block}"""

        cursor.execute(sql)

        for bucket, src_key in cursor:
            tier, hid, block, fn = src_key.split("/")
            dest_key = f"{tier}/{hid}/{dest_block:04d}/{fn}"
            file_info_move_key(
                hid,
                dest_block,
                bucket,
                dest_key,
                hid,
                src_block,
                bucket,
                src_key,
                boto_,
                pgsql_,
            )

        sql0 = f"""SELECT device_id, unix_start, unix_stop, session_guid
                    FROM aa_blocks
                    WHERE hid = {hid} AND
                        block_number = {src_block} """

        cursor.execute(sql0)
        device_id, unix_start, unix_stop, session_guid = cursor.fetchone()

        sql1 = f"""DELETE FROM aa_blocks
                    WHERE hid = {hid}
                        AND block_number = {src_block};

                INSERT INTO aa_blocks (hid, block_number,
                                        device_id, unix_start,
                                        unix_stop, session_guid)
                    VALUES ({hid}, {dest_block}, {device_id},
                            {unix_start}, {unix_stop},
                            {"NULL" if session_guid is None else session_guid} )
                ON CONFLICT (hid, block_number)
                DO UPDATE
                        SET device_id = {device_id}, unix_start={unix_start},
                            unix_stop = {unix_stop},
                            session_guid={"NULL" if session_guid is None
                                        else session_guid}
                    WHERE aa_blocks.hid = {hid}
                        AND aa_blocks.block_number = {dest_block} """
        cursor.execute(sql1)


def move_hid_and_block(dest_hid, dest_block, src_hid, src_block, boto_, pgsql_):
    with pgsql_, pgsql_.cursor() as cursor:
        sql = f"""SELECT site.bucket
                    FROM session_management.aa_session_data sd
                INNER JOIN site_management.aa_site_data site ON site.name =  sd.site
                    WHERE sd.hid = {dest_hid}"""
        cursor.execute(sql)
        dest_bucket = cursor.fetchone()[0]

        sql2 = f"""INSERT INTO aa_blocks (hid,block_number )
                    VALUES ({dest_hid}, {dest_block} )
                ON CONFLICT (hid, block_number) DO NOTHING"""
        cursor.execute(sql2)

    with pgsql_, pgsql_.cursor() as cursor:
        sql = f"""SELECT bucket, key
                    FROM file_info
                    WHERE hid = {src_hid} AND block = {src_block}"""
        cursor.execute(sql)
        for src_bucket, src_key in cursor:
            tier, hid, block, fn = src_key.split("/")
            dest_key = f"{tier}/{dest_hid}/{dest_block:04d}/{fn}"
            file_info_move_key(
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
            )

        sql0 = f"""SELECT device_id, unix_start, unix_stop, session_guid
                        FROM aa_blocks
                    WHERE hid = {src_hid}
                            AND block_number = {src_block} """
        cursor.execute(sql0)
        device_id, unix_start, unix_stop, session_guid = cursor.fetchone()

        sql1 = f"""DELETE
                        FROM aa_blocks
                    WHERE hid = {src_hid}
                        AND block_number = {src_block};

                    INSERT
                        INTO aa_blocks (hid,block_number, device_id, unix_start,
                                        unix_stop, session_guid)
                    VALUES ({dest_hid}, {dest_block}, {device_id},
                            {unix_start}, {unix_stop},
                            '{"NULL" if session_guid is None else session_guid}' )
                ON CONFLICT (hid, block_number) DO UPDATE
                        SET device_id = {device_id}, unix_start = {unix_start},
                            unix_stop = {unix_stop},
                            session_guid = '{"NULL" if session_guid is None
                                            else session_guid}'
                    WHERE aa_blocks.hid = {dest_hid}
                        AND aa_blocks.block_number = {dest_block}"""

        cursor.execute(sql1)


def find_block_keys(client, hid, block, bucket):
    keys = []
    r = client.list_objects_v2(
        Bucket=bucket, Prefix="tier1/{hid}/{block:04d}".format(hid=hid, block=block)
    )
    if "Contents" in r.keys():
        keys = [k["Key"] for k in r["Contents"]]
    r = client.list_objects_v2(
        Bucket=bucket, Prefix="tier2/{hid}/{block:04d}".format(hid=hid, block=block)
    )
    if "Contents" in r.keys():
        keys += [k["Key"] for k in r["Contents"]]
    return keys


def delete_block(pgsql_, client, hid, block, bucket=None):
    if bucket is None:
        with pgsql_, pgsql_.cursor() as cursor:
            cursor.execute(
                f"""SELECT bucket
                                    FROM aa_session_data sd
                            INNER JOIN aa_site_data site
                                ON sd.site = site.name
                            WHERE sd.hid = {hid} """
            )
            bucket = cursor.fetchone()[0]
    keys = find_block_keys(client, hid, block, bucket)
    for key in keys:
        client.delete_object(Bucket=bucket, Key=key)

    with pgsql_, pgsql_.cursor() as cursor:
        cursor.execute(
            f"""DELETE
                  FROM file_info
                 WHERE hid = {hid}
                   AND block = {block};
                DELETE
                  FROM aa_blocks
                 WHERE hid = {hid}
                   AND block_number = {block}"""
        )
