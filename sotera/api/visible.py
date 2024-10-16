import sys
from functools import partial
from .hidden import get_key_by_hid, do_post_raw, do as _do

# globals
_ann_map = None
_ann_id_map = None


def get_site_by_hid(hid, pgsql_=None):
    """Get the site name for a session given the session id (hid)

    Parameters
    ----------
    hid : int
       Session id

    Returns
    -------
    The site name as string
    """
    if pgsql_:
        return db.get_site_by_hid(hid, pgsql_)
    else:
        return _do("getSiteByHid", dict(hid=hid))


def get_blocks_by_hid(hid, pgsql_=None):
    """Get the site name for a session given the session id (hid)

    Parameters
    ----------
    hid : int
       Session id

    Returns
    -------
    A list of dictionaries describing a block, having format:

       {
        'block_number': 0,
        'device_id': 622623,
        'hid': 40000,
        'tStart': u'2016-01-20 13:35:53 CST',
        'tStop': u'2016-01-20 15:00:19 CST'},
       }

    """
    if pgsql_:
        return db.get_blocks_by_hid(hid, pgsql_)
    else:
        return _do("getBlocksByHid", dict(hid=hid))


get_sites = partial(_do, "getSites")
get_sites.__doc__ = "Get a list of all sites"


def get_timezone_by_site(site):
    """Get the timezone for a site

    Parameters
    ----------
    site : string
       Site name

    Returns
    -------
    The a string representation of the timezone, e.g. US/Pacific
    """
    return _do("getTimezoneBySite", dict(site=site))


get_site_defaults = lambda site: _do("getSiteDefaults", dict(site=site))


def get_blocks_by_deviceid_and_time(
    device_id,
    start=None,
    stop=None,
    tz=None,
    output_timestamp=True,
    page_size=10000,
    count=False,
):
    """Find blocks by device id and (optionally time)

    Parameters
    ----------
    device id : int
        Device id
    start : None or int or string, optional
        Start time of search range. By default, 0 (timestamp). If a
        string is given it will be parsed to determinate a timestamp.
    stop : None or int or string, optional
        Stop time of search range. By default, MAX_INT (timestamp). If a
        string is given it will be parsed to determinate a timestamp.
    tz : None or string, optional
        Timezone to use when converting times given in strings to timestamps. By default,
        the timezone of the given site
    output_timestamp : boolean, optional
        If True (default) return times as a timestamp, otherwise return as a string
    page_size : int, optional
        Number of blocks to request from the database at a time, default 10000

    Returns
    -------
    A generator object for iterating over a list of dictionaries having the format:

       {
        'block_number': 0,
        'device_id': 622623,
        'hid': 40000,
        'tStart': u'2016-01-20 13:35:53 CST',
        'tStop': u'2016-01-20 15:00:19 CST'},
       }

    Notes
    -----
    The function returns a generator object in order to support searchs that return
    very long (e.g. hundreds of thousands) of blocks. If know your search will return
    a large number of results you should iterate over the generator, like:

      >> for block in sotera.api.get_blocks_by_site_and_time(site, start, stop):
             do_something_with_block(block)

    If you are not concerned about the number of results, you can create a list directly
    with:

      >> blocks = list( sotera.api.get_blocks_by_site_and_time(site, start, stop) )

    """

    # count : boolean, optional
    # Return a count of the number of blocks that match the query, rather then the
    # blocks themselves

    if start is None and stop is None:
        # if both start and start are none get all
        start = 0
        stop = sys.maxsize

    args = dict(device_id=device_id, unix_start=start)
    args = _check_times(start, stop, tz, args)

    if output_timestamp:
        args["outputAsTimestamp"] = output_timestamp

    # if count:
    #    args['returnCount'] = True
    #    return _do( 'getHidBlocksByDeviceIdUnixTime', args )

    args["pageSize"] = page_size
    args["offset"] = 0
    r = _do("getHidBlocksByDeviceIdUnixTime", args)
    while len(r):
        for item in r:
            yield item
        args["offset"] += page_size
        r = _do("getHidBlocksByDeviceIdUnixTime", args)


def get_hids_by_deviceid_and_time(
    device_id, start=None, stop=None, tz=None, page_size=10000, count=False
):
    """Find hids by device id and (optionally) time

    Parameters
    ----------
    device id : int
        Device id
    start : None or int or string, optional
        Start time of search range. By default, 0 (timestamp). If a
        string is given it will be parsed to determinate a timestamp.
    stop : None or int or string, optional
        Stop time of search range. By default, MAX_INT (timestamp). If a
        string is given it will be parsed to determinate a timestamp.
    tz : None or string, optional
        Timezone to use when converting times given in strings to timestamps. By default,
        the timezone of the given site
    output_timestamp : boolean, optional
        If True (default) return times as a timestamp, otherwise return as a string
    page_size : int, optional
        Number of blocks to request from the database at a time, default 10000

    Returns
    -------
    The function returns a generator object in order to support searchs that return
    very long (e.g. thousands) of session ids. If know your search will return
    a large number of results you should iterate over the generator, like:

      >> for hids in sotera.api.get_hids_by_deviceid_and_time(site, start, stop):
             do_something_with_session_id(hid)

    If you are not concerned about the number of results, you can create a list directly
    with:

      >> hids = list( sotera.api.get_hids_by_deviceid_and_time(site, start, stop) )

    """
    # count : boolean, optional
    # Return a count of the number of sessions that match the query, rather then the
    # hids themselves

    if start is None and stop is None:
        # if both start and start are none get all
        start = 0
        stop = sys.maxsize

    args = dict(device_id=device_id, unix_start=start, outputAsTimestamp=False)
    args = _check_times(start, stop, tz, args)

    # if count:
    #    args['returnCount'] = True
    #    return _do( 'getHidsByDeviceIdUnixTime', args )

    args["pageSize"] = page_size
    args["offset"] = 0
    r = _do("getHidsByDeviceIdUnixTime", args)
    while len(r):
        for item in r:
            yield item
        args["offset"] += page_size
        r = _do("getHidsByDeviceIdUnixTime", args)


def get_blocks_by_site_and_time(
    site,
    start=None,
    stop=None,
    tz=None,
    output_timestamp=True,
    page_size=10000,
    count=False,
):
    """Find blocks by site and (optionally) time

    Parameters
    ----------
    site : string
        Name of the site
    start : None or int or string, optional
        Start time of search range. By default, 0 (timestamp). If a
        string is given it will be parsed to determinate a timestamp.
    stop : None or int or string, optional
        Stop time of search range. By default, MAX_INT (timestamp). If a
        string is given it will be parsed to determinate a timestamp.
    tz : None or string, optional
        Timezone to use when converting times given in strings to timestamps. By default,
        the timezone of the given site
    output_timestamp : boolean, optional
        If True (default) return times as a timestamp, otherwise return as a string
    page_size : int, optional
        Number of blocks to request from the database at a time, default 10000

    Returns
    -------
    A generator object for iterating over a list of dictionaries have the format:

       {
        'block_number': 0,
        'device_id': 622623,
        'hid': 40000,
        'tStart': u'2016-01-20 13:35:53 CST',
        'tStop': u'2016-01-20 15:00:19 CST'},
       }

    Notes
    -----
    The function returns a generator object in order to support searchs that return
    very long (e.g. hundreds of thousands) of blocks. If know your search will return
    a large number of results you should iterate over the generator, like:

      >> for block in sotera.api.get_blocks_by_site_and_time(site, start, stop):
             do_something_with_block(block)

    If you are not concerned about the number of results, you can create a list directly
    with:

      >> blocks = list( sotera.api.get_blocks_by_site_and_time(site, start, stop) )

    """

    # count : boolean, optional
    # Return a count of the number of blocks that match the query, rather then the
    # blocks themselves

    if start is None and stop is None:
        # if both start and start are none get all
        start = 0
        stop = sys.maxsize
    args = dict(site=site, unix_start=start)
    if tz is None:
        tz = _do("getTimezoneBySite", dict(site=site))
    args = _check_times(start, stop, tz, args)
    if output_timestamp:
        args["outputAsTimestamp"] = output_timestamp

    # if count:
    #    args['returnCount'] = True
    #    return _do( 'getHidBlocksBySiteUnixTime', args )

    args["pageSize"] = page_size
    args["offset"] = 0
    r = _do("getHidBlocksBySiteUnixTime", args)
    while len(r):
        for item in r:
            yield item
        args["offset"] += page_size
        r = _do("getHidBlocksBySiteUnixTime", args)


def get_hids_by_site_and_time(
    site, start=None, stop=None, tz=None, count=False, page_size=10000
):
    """Find session id by site and (optionally time)

    Parameters
    ----------
    site : string
        Name of the site
    start : None or int or string, optional
        Start time of search range. By default, 0 (timestamp). If a
        string is given it will be parsed to determinate a timestamp.
    stop : None or int or string, optional
        Stop time of search range. By default, MAX_INT (timestamp). If a
        string is given it will be parsed to determinate a timestamp.
    tz : None or string, optional
        Timezone to use when converting times given in strings to timestamps. By default,
        the timezone of the given site
    output_timestamp : boolean, optional
        If True (default) return times as a timestamp, otherwise return as a string
    page_size : int, optional
        Number of blocks to request from the database at a time, default 10000

    Returns
    -------
    The function returns a generator object in order to support searchs that return
    very long (e.g. thousands) of session ids. If know your search will return
    a large number of results you should iterate over the generator, like:

      >> for hids in sotera.api.get_hids_by_site_and_time(site, start, stop):
             do_something_with_session_id(hid)

    If you are not concerned about the number of results, you can create a list directly
    with:

      >> hids = list( sotera.api.get_hids_by_site_and_time(site, start, stop) )


    """

    # count : boolean, optional
    # Return a count of the number of sessions that match the query, rather then the
    # hids themselves

    if start is None and stop is None:
        # if both start and start are none get all
        start = 0
        stop = sys.maxsize
    args = dict(site=site, unix_start=start, outputAsTimestamp=False)
    if tz is None:
        tz = _do("getTimezoneBySite", dict(site=site))
    args = _check_times(start, stop, tz, args)

    # if count:
    #    args['returnCount'] = True
    #    return _do( 'getHidsBySiteUnixTime', args )

    args["pageSize"] = page_size
    args["offset"] = 0
    r = _do("getHidsBySiteUnixTime", args)
    while len(r):
        for item in r:
            yield item
        args["offset"] += page_size
        r = _do("getHidsBySiteUnixTime", args)
    # return list(set([b['hid'] for b in get_blocks_by_site_and_time(site,start,stop,tz,output_timestamp=False)]))


def _check_times(start, stop, tz, args):
    """Check to make sure the given comination of start,
    stop, and tz make are consistent.

    """

    if stop:
        if type(start) is str and type(stop) is not str:
            raise ValueError("stop time must be a string is start time is a string")
        args["unix_stop"] = stop

    if tz:
        args["tz"] = tz
    elif type(start) == str:
        raise ValueError("Timezone must be set if start time is a string")
    return args


def get_annotations(annotator, hid, blockno=None):
    """Find annotations for a given session and, optionally, block

    Parameters
    ----------
    annotator : string
        Annotator to find
    hid : int
        Session id
    blockno : int, optional
        Session block number

    Returns
    -------
    A list of dictionaries having the format:
        {
           'id': 142868,
           'annotation_type_id': 1005,
           't_start': 300.036,
           't_stop': -1.0,
           'notes': None
         }

    """
    args = dict(user=annotator, hid=hid)
    if blockno is None:
        # if no block is provided iterate over concatinate
        # annotations from all blocks
        anns = []
        for block in get_blocks_by_hid(hid):
            args["block_number"] = block["block_number"]
            anns += _do("getAnnotationsByUserHidBlockNumber", args)
    else:
        args["block_number"] = blockno
        anns = _do("getAnnotationsByUserHidBlockNumber", args)

    return anns


def get_annotation_type(ann_type_id=None):
    args = dict(annotationTypeId=ann_type_id)
    if ann_id:
        return _do("getAnnotationTypeById", args)[0]
    else:
        return _do("getAnnotationTypeById")


def get_annotation_map(source):
    global _ann_map
    if _ann_map is None:
        _ann_map = dict(
            (a[0], a[1]) for a in _do("getAnnotationTypes", dict(source=source))
        )
    return _ann_map


def get_annotation_id_map(source):
    global _ann_id_map
    if _ann_id_map is None:
        _ann_id_map = dict(
            (a[1], a[0]) for a in _do("getAnnotationTypes", dict(source=source))
        )
    return _ann_id_map


def get_annotated_blocks(annotator, code=None, from_date=None):
    args = dict(annotator=annotator)
    if code:
        if type(code) == str:
            args["annotationTypeId"] = get_annotation_map()[code]
        elif type(code) == int:
            args["annotationTypeId"] = code
        else:
            raise ValueError("Annotation code can be an int or a string")
    if from_date:
        args["fromDate"] = from_date
    return _do("getAnnotatedHidsBlocksByAnnotatorAnnotationTypeId", args)


def insert_annotations(
    hid,
    blockno,
    annotator,
    annotation_id,
    start_time,
    stop_time=-1,
    notes="",
    filename=None,
):
    if filename is None:
        bucket, filename = get_key_by_hid(hid, "full_block_data", blockno)
    args = dict(
        higginsId=hid,
        blockNumber=blockno,
        fileName=filename,
        annotator=annotator,
        annotationTypeId=annotation_id,
        tStart=start_time,
        tStop=stop_time,
        notes=notes,
    )
    return do_post_raw("insertAnnotation", args)
