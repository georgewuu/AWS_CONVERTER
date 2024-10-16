import requests

try:
    from urllib.parse import urlencode
except ImportError:
    from urllib import urlencode

from .. import resources

try:
    __root__ = resources["api"]["root"]
except:
    __root__ = "https://science.cloud.soterawireless.com/API"

try:
    __token__ = resources["api"]["key"]
except:
    __token__ = None


# base request function
def do_raw(resource, args=None, token=__token__):
    if args:
        url = "{}/{}/?token={}&{}".format(__root__, resource, token, urlencode(args))
    else:
        url = "{}/{}/?token={}".format(__root__, resource, token)
    r = requests.get(url)
    if r.ok:
        if r.text == "access denied":
            raise RuntimeError("API server error: access denied")
        else:
            return r
    else:
        raise RuntimeError("API server error: {}".format(r.text))


def do(resource, args=None, token=__token__):
    # if type(args) is dict and 'site' in args.keys():
    #    if args['site'] not in do('getSites'):
    #        raise ValueError('{} is not a known site'.format(site))
    return do_raw(resource, args, token).json()


def do_post_raw(resource, args=None, token=__token__):
    url = "{}/{}/?token={}".format(__root__, resource, token)
    r = requests.post(url, json=args)
    if r.ok:
        if r.text == "access denied":
            raise RuntimeError("API server error: access denied")
        else:
            return r
    else:
        raise RuntimeError("API server error: {}".format(r.text))


# session info
def get_key_by_hid(hid, file_class, block=None):
    args = dict(hid=hid, file_class=file_class)
    if block:
        args["block"] = block
    r = do("getBucketKeyByHidFileClassBlock", args)
    return r["bucket"], r["key"]


def download_file(bucket, key, fp):
    """ use the API to download a file from S3 """
    args = dict(bucket=bucket, key=key)
    r = do_raw("downloadFile", args)
    response = requests.get(r.text, stream=True)
    if response.ok:
        for block in response.iter_content(chunk_size=1024):
            fp.write(block)
    else:
        raise RuntimeError("Unable to download file")


# annotations


# def update_annotation(annotation_id, annotator, annotation_type_id, t_start, t_stop, notes):
#     pass

# def get_annotation(annotation_id):
#     pass

get_session_info = lambda hid: do("getSessionInfo", dict(hid=hid))
delete_annotation = lambda annotation_id: do(
    "deleteAnnotation", dict(annotation_id=annotation_id)
)
get_user_by_key = lambda key: do("getUserByKey", dict(key=key))
