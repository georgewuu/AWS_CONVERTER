import os
import logging

try:
    import ujson
except ImportError:
    import json as ujson
import asyncio
import aiohttp
import sqlite3
import aiosqlite
import time
import contextlib
from numpy import array, interp
from intervaltree.interval import Interval
from io import BytesIO
from binascii import a2b_base64
from sotera.io import visi
from sotera.util.time import get_timestamp

logger = logging.getLogger(__name__)


def int_or_none(i):
    return i if i is None else int(i)


def mk_data_url(pds, query):
    A = f"sessionGroupId:{query['sessionGroupId']}"
    B = f"timestamp:[{query['timestamp1']} TO {query['timestamp2']}]"
    C = " OR ".join((f"packetType:{p}" for p in query["packetTypes"]))
    query_str = f"{A} AND {B} AND ({C})"
    url = (
        f"http://{pds['host']}:{pds['port']}/solr/{query['node']}/select?"
        f"indent=off&q={query_str}"
    )
    return url


def mk_log_url(pds, q):
    url = (
        f"http://{pds['host']}:{pds['port']}/solr/log/select?indent=off&"
        f"q=(packetType:{q['pid']})AND(timestamp:[{q['t0']} TO {q['t1']}])"
    )
    if q["device"] is not None:
        url += f" AND (deviceId:{q['device']})"
    return url


def mk_pass_thru_url(pds, query):
    A = f"deviceId:{query['device']}"
    B = f"timestamp:[{query['timestamp1']} TO {query['timestamp2']}]"
    C = " OR ".join((f"packetType:{p}" for p in query["packetTypes"]))
    query_str = f"{A} AND {B} AND ({C})"
    url = f"http://{pds['host']}:{pds['port']}/solr/log/select?indent=off&q={query_str}"
    return url


def spool_packets_from_doc(doc):
    stream_ = BytesIO(a2b_base64(doc["packet"]))
    for id_, sn, tm, device, segment, content, raw in visi.packets.spool_packets(
        stream_
    ):
        yield int_or_none(id_), int_or_none(sn), int_or_none(tm), int_or_none(
            device
        ), int_or_none(segment), content, raw, doc["timestamp"]
    stream_.close()


async def fetch_json_response(url, client):
    async with client.get(url) as response:
        r = await response.text()
        return ujson.loads(r)["response"]


async def solr_fetch(url, idx, client, step=1000):
    page_url = "&".join(
        (url, f"rows={step}", f"start={idx}", "wt=json", "sort=timestamp+ASC")
    )
    r = await fetch_json_response(page_url, client)
    return r["docs"]


async def get_query_num_packets(url, client):
    r = await fetch_json_response("&".join((url, "wt=json")), client)
    return r["numFound"]


async def make_solr_query(url, client, start=0, step=1000, min_packets=0):
    num_packets = await get_query_num_packets(url, client)
    if num_packets > min_packets - 1:
        return [
            solr_fetch(url, idx, client, step=step)
            for idx in range(start, num_packets, step)
        ]
    else:
        return []


def spool_raw_from_doc(doc):
    for id_, sn, tm, device, segment, content, raw, doc_ts in spool_packets_from_doc(
        doc
    ):
        yield id_, sn, tm, device, segment, raw


def spool_log_from_doc(doc, use_log_sn=False):
    try:
        last = (0, 0, 0, 0, 0, "none", None)
        for (
            id_,
            sn_,
            tm,
            device,
            segment,
            content,
            raw,
            doc_ts,
        ) in spool_packets_from_doc(doc):
            if device is None:
                device = doc["deviceId"]
            if segment is None:
                segment = 0
            if tm is None or tm < 100000 or tm > 4070937600000:
                logger.warning(f"Bad log time: {tm} USING solr doc time")
                tm = doc_ts
            if id_ == 42:
                sn = int(content[1].split()[0]) if use_log_sn else sn_
                yield id_, sn, tm, device, segment, content[1], raw
            elif id_ == 257:
                sn = sn_
                if content[1] is None:
                    if content[0][3] is None:
                        log_as_text = (
                            "Malformed log analytics packet - "
                            f"wrong size {content[0][0]}"
                        )
                    else:
                        log_as_text = (
                            f"Malformed log analytics packet: subtype={content[0][3]}"
                        )
                    logger.warning(log_as_text)
                else:
                    log_as_text = visi.logs.format_data_log(content[0], content[1])
                yield id_, sn, tm, device, segment, log_as_text, raw
                last = (id_, sn, tm, device, segment, log_as_text, content)
    except KeyError:
        logger.exception(
            f"Error parsing packets from solr doc\nLast packet parsed was id={last[0]}"
            f" tm={last[2]} device={last[3]} log={last[5]} content={last[6]}\n"
        )


DEFAULT_QUERY_RATE = 0.5  # query every 0.5 seconds
DEFAULT_WAVEFORM_STEP = 500
DEFAULT_DATA_STEP = 50
DEFAULT_EXPORT_PACKETS = (
    4,
    18,
    19,
    20,
    21,
    36,
    42,
    43,
    49,
    54,
    58,
    184,
    185,
    186,
    188,
    189,
    190,
    191,
    193,
    194,
    205,
    208,
    211,
    221,
    227,
    228,
    229,
    230,
    231,
    232,
    233,
    234,
    235,
    236,
    237,
    238,
    239,
    240,
    241,
    242,
    243,
    244,
    245,
    246,
    247,
    248,
    249,
    250,
    251,
    252,
    253,
    254,
    255,
    256,
    258,
)
DEFAULT_LOG_PACKETS = (42, 257)
DEFAULT_PASS_THRU_PACKETS = (
    3001,
    3002,
    3003,
    3004,
    3005,
    3006,
    3007,
    3008,
    3009,
    3010,
    3011,
    3012,
    3013
)
DEFAUlT_LOG_STEP = 200

create_log_packet_table = """ CREATE TABLE packets (
                                 cnt INTEGER PRIMARY KEY AUTOINCREMENT,
                                  id INT,
                                  sn INT,
                                  tm INT,
                              device INT,
                             segment INT,
                             message TEXT,
                                 raw BLOB  ) """

create_data_packet_table = """ CREATE TABLE packets (
                                  cnt INTEGER PRIMARY KEY AUTOINCREMENT,
                                   id INT,
                                   sn INT,
                                   tm INT,
                               device INT,
                              segment INT,
                                 raw BLOB ) """


def fix_timestr(s):
    return s.replace("T", " ").replace("Z", "") if s is not None else None


def fix_device_session(session, pds):

    session["groupId"] = session["groupId"] - pds["settings"].get("id_offset", 0)

    for k in ("deviceID", "stopTime"):
        if k not in session.keys():
            session[k] = None
    session["startTime"] = fix_timestr(session["startTime"])
    session["t0"] = int(
        1000
        * (get_timestamp(session["startTime"], zone="UTC", fmt="%Y-%m-%d %H:%M:%S.%f"))
    )
    if session["stopTime"]:
        session["stopTime"] = fix_timestr(session["stopTime"])
    else:
        session["stopTime"] = ""

    session["t1"] = int(
        1000
        * (get_timestamp(session["stopTime"], zone="UTC", fmt="%Y-%m-%d %H:%M:%S.%f"))
    )
    return session


async def aio_retrieve_session_logs(session, pds, export_path):

    query = {
        "t0": session["t0"] - 60 * 1000,
        "t1": session["t1"] + 60 * 1000,
        "device": session["deviceID"],
        "pid": None,
    }

    log_cachedb = os.path.join(export_path, f"{session['sessionGUID']}-logs.db")
    with contextlib.suppress(FileNotFoundError):
        os.remove(log_cachedb)

    async with aiosqlite.connect(log_cachedb) as sqlite_:
        await sqlite_.execute(create_log_packet_table)
        queue = asyncio.Queue()
        queue_future = asyncio.ensure_future(save_logs_worker(queue, sqlite_))

        async with aiohttp.ClientSession() as client:
            for pid in DEFAULT_LOG_PACKETS:
                base_url = mk_log_url(pds, query.update({"pid": pid}) or query)
                async with client.get(f"{base_url}&wt=json") as response:
                    r = await response.text()
                num_packets = ujson.loads(r)["response"]["numFound"]
                logger.info(
                    f"{num_packets} log packets {query['t0']} - {query['t1']}"
                    f" from {query['device']} to download"
                )

                if num_packets > 0:
                    logger.debug(f"getting {num_packets} packets")
                    sem = asyncio.Semaphore(1)
                    tasks = []
                    step = pds["settings"].get("log-step", DEFAUlT_LOG_STEP)
                    for idx in range(0, num_packets, step):
                        tasks.append(
                            asyncio.ensure_future(
                                fetch_packets(
                                    client,
                                    f"{base_url}&rows={step}&start={idx}"
                                    "&wt=json&sort=timestamp+ASC",
                                    sem,
                                    queue,
                                    rate=pds["settings"].get(
                                        "rate", DEFAULT_QUERY_RATE
                                    ),
                                )
                            )
                        )
                    await asyncio.gather(*tasks)
                else:
                    logger.info(
                        f"No {'logs' if pid == 42 else 'analytics packets'} availble"
                    )
        await queue.put({"done": True})  # tell worker to finish
        await queue_future  # wait for worker to finish
    return log_cachedb


def spool_raw_from_logdb(cursor, session, timesyncs):
    sql_ = f"""SELECT DISTINCT id, sn, tm, device, segment, raw
                 FROM packets
                WHERE device  = {session['deviceID']}
                      AND tm >= {session['t0']}
                      AND tm <= {session['t1']}
             ORDER BY tm, sn """
    cursor.execute(sql_)
    for id, _sn, tm, device, _segment, raw in cursor:
        sn = None
        for v in timesyncs.values():
            if v["interval"].contains_point(tm):
                sn = int(interp(tm, v["array"][:, 1], v["array"][:, 0]))
                break
        if sn is not None:
            yield id, sn, tm, device, v["segment"], raw


async def make_timestamp_mapping(sqlite_, session):
    # pull back time sync packets to make timestamp to sn mapping
    timesyncs = {}
    sql = """SELECT segment, sn, tm, device
               FROM packets
              WHERE id = 4
           ORDER BY segment, sn"""

    async with sqlite_.execute(sql) as cursor:
        async for row in cursor:
            device_id = row[3]
            try:
                timesyncs[row[0]].append([row[1], row[2]])
            except KeyError:
                timesyncs[row[0]] = [[row[1], row[2]]]
    for seg in timesyncs.keys():
        tmp = array(timesyncs[seg])
        timesyncs[seg] = {
            "array": tmp,
            "segment": seg,
            "interval": Interval(tmp[0, 1], tmp[-1, 1]),
        }

    return timesyncs, device_id


async def save_logs_worker(queue, sqlite_):
    logger.debug("entering save_logs_worker()")
    sql = """ INSERT
                INTO packets (id, sn, tm, device, segment, message, raw)
              VALUES (?,?,?,?,?,?,?) """
    active = True
    while active:
        response = await queue.get()
        if "done" in response.keys():
            active = False
        elif "docs" in response.keys():
            logger.debug(f"Writting {len(response['docs'])} docs")
            for doc in response["docs"]:
                for tuple_ in spool_log_from_doc(doc, use_log_sn=True):
                    await sqlite_.execute(sql, tuple_)
            await sqlite_.commit()
    logger.debug("exiting save_logs_worker()")


async def save_data_worker(queue, sqlite_):
    logger.debug("entering save_data_worker()")
    sql = """INSERT
            INTO packets (id, sn, tm, device, segment, raw)
          VALUES (?,?,?,?,?,?) """
    active = True
    while active:
        response = await queue.get()
        if "done" in response.keys():
            active = False
        elif "docs" in response.keys():
            logger.debug(f"Writting {len(response['docs'])} docs")
            for doc in response["docs"]:
                for tuple_ in spool_raw_from_doc(doc):
                    await sqlite_.execute(sql, tuple_)
            await sqlite_.commit()
    logger.debug("exiting save_data_worker()")


last_hit = 0


async def fetch_packets(client, url, sem, queue, rate=0.5):
    global last_hit
    async with sem:
        async with await client.get(url) as response:
            logger.debug(f"fetch {url}")
            elapse = time.time() - last_hit
            logger.debug(f"elapse = {elapse}")
            if elapse < rate:
                await asyncio.sleep(rate - elapse)
            r = await response.text()
            last_hit = time.time()
            r = ujson.loads(r)
            await queue.put(r["response"])


async def aio_retrieve_session_data(session, pds, export_path):
    query = {
        "timestamp1": f"{session['t0']}",
        "timestamp2": f"{session['t1']}",
        "sessionGroupId": session["groupId"],
        "packetTypes": pds["settings"].get("export-packets", DEFAULT_EXPORT_PACKETS),
        "node": None,
    }

    packet_cachedb = os.path.join(export_path, f"{session['sessionGUID']}-data.db")
    with contextlib.suppress(FileNotFoundError):
        os.remove(packet_cachedb)

    rval = "incomplete"
    async with aiosqlite.connect(packet_cachedb) as sqlite_:

        # create the table in the sqlite db where packets will be saved
        await sqlite_.execute(create_data_packet_table)

        # The solr documents will be queued for writting to the db as they are retrieved
        # from the server: (1) create the queue; (2) and the start the worker task that
        # reads from the queue and saves to sqlitedb
        queue = asyncio.Queue()
        queue_future = asyncio.ensure_future(save_data_worker(queue, sqlite_))

        for node, step in (
            ("waveform", pds["settings"].get("waveform-step", DEFAULT_WAVEFORM_STEP)),
            ("data", pds["settings"].get("data-step", DEFAULT_DATA_STEP)),
        ):
            rval = "incomplete"
            base_url = mk_data_url(pds, query.update({"node": node}) or query)
            async with aiohttp.ClientSession() as client:

                # First see if there are any packets to get (the session could be gone
                # or too small to bother
                num_packets = 0
                url = f"{base_url}&wt=json"
                async with client.get(url) as response:
                    logger.debug(f"{response.status} {url}")
                    r = await response.text()
                    r = ujson.loads(r).get("response", None)
                    num_packets = 0 if r is None else r["numFound"]
                    logger.info(
                        f"{session['sessionGUID']} {node} {num_packets}"
                        "packets to download"
                    )

                if num_packets > 3:
                    # there are enough packets to download
                    logger.info(
                        f"Downloading {num_packets} {node} packets to cache {0.:3.0f}%"
                    )
                    sem = asyncio.Semaphore(1)
                    tasks = []
                    for idx in range(0, num_packets, step):
                        tasks.append(
                            asyncio.ensure_future(
                                fetch_packets(
                                    client,
                                    f"{base_url}&rows={step}&start={idx}"
                                    "&wt=json&sort=timestamp+ASC",
                                    sem,
                                    queue,
                                    rate=pds["settings"].get(
                                        "rate", DEFAULT_QUERY_RATE
                                    ),
                                )
                            )
                        )
                        await asyncio.gather(*tasks)

                else:
                    logger.info(f"too few packets [node] {num_packets} to fetch")
                    rval = "FileNotFound"

        await queue.put({"done": True})  # tell worker to finish
        await queue_future  # wait for worker to finish

        if rval == "incomplete":
            timesyncs, device_id = await make_timestamp_mapping(sqlite_, session)
        else:
            timesyncs, device_id = None, None

    return rval, packet_cachedb, timesyncs, device_id


async def aio_retrieve_pass_thru_data(session, pds, export_path):
    logger.info("retrieve_pass_thru")
    query = {
        "timestamp1": session["t0"] - 60 * 1000,
        "timestamp2": session["t1"] + 60 * 1000,
        "device": session["deviceID"],
        "packetTypes": DEFAULT_PASS_THRU_PACKETS,
    }
    packet_cachedb = os.path.join(export_path, f"{session['sessionGUID']}-data.db")
    async with aiosqlite.connect(packet_cachedb) as sqlite_:
        # The solr documents will be queued for writting to the db as they are retrieved
        # from the server: (1) create the queue; (2) and the start the worker task that
        # reads from the queue and saves to sqlitedb

        queue = asyncio.Queue()
        queue_future = asyncio.ensure_future(save_data_worker(queue, sqlite_))
        base_url = mk_pass_thru_url(pds, query)
        step = DEFAULT_DATA_STEP
        async with aiohttp.ClientSession() as client:
            # there are enough packets to download
            logger.info(f"{base_url}&wt=json")
            async with client.get(f"{base_url}&wt=json") as response:
                r = await response.text()
                num_packets = ujson.loads(r)["response"]["numFound"]
                logger.info(
                    f"{num_packets} passthrough packets {num_packets} to download"
                )

            if num_packets > 0:
                logger.info(f"Downloading passthrough packets to cache {0.:3.0f}%")
                sem = asyncio.Semaphore(1)
                tasks = []
                for idx in range(0, num_packets, step):
                    tasks.append(
                        asyncio.ensure_future(
                            fetch_packets(
                                client,
                                f"{base_url}&rows={step}&start={idx}"
                                "&wt=json&sort=timestamp+ASC",
                                sem,
                                queue,
                                rate=DEFAULT_QUERY_RATE,
                            )
                        )
                    )
                    await asyncio.gather(*tasks)
                else:
                    logger.info(f"too few packets [node] {num_packets} to fetch")
        await queue.put({"done": True})  # tell worker to finish
        await queue_future  # wait for worker to finish

    return


async def aio_download_session(session, pds, export_path):
    logger.info(f"Exporting {session['sessionGUID']}")

    rval, packet_cachedb, timesyncs, device_id = await aio_retrieve_session_data(
        session, pds, export_path
    )

    if rval == "incomplete" and device_id is not None:
        logger.info(f"Downloading device {session['deviceID']} logs to cache")

        if session["deviceID"] is None:
            session["deviceID"] = device_id

        await aio_retrieve_pass_thru_data(session, pds, export_path)

        # get log packets
        log_cachedb = await aio_retrieve_session_logs(session, pds, export_path)

        # now copy over the packets logs over to data packets
        sql = """INSERT
                   INTO packets (id, sn, tm, device, segment, raw)
                 VALUES (?,?,?,?,?,?)"""
        sqlite_ = sqlite3.connect(packet_cachedb)
        logsqlite_ = sqlite3.connect(log_cachedb)
        cursor = logsqlite_.cursor()
        sqlite_.executemany(sql, spool_raw_from_logdb(cursor, session, timesyncs))
        sqlite_.commit()
        logsqlite_.close()
        rval = "complete"

    if rval == "FileNotFound":
        with contextlib.suppress(FileNotFoundError):
            with contextlib.suppress(UnboundLocalError):
                os.remove(packet_cachedb)
        with contextlib.suppress(FileNotFoundError):
            with contextlib.suppress(UnboundLocalError):
                os.remove(log_cachedb)

    return rval
