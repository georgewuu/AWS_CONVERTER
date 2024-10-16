import os.path
from functools import partial
import numpy as np
import logging
from datetime import datetime
from . import packet_map, analytics_map
from . import packets
from . import logs
from .constants import k_ppg_dc, k_ppg_filt, k_ecg, k_ip, k_ambient, replace_constants
from ..local import optimize

logger = logging.getLogger(__name__)


def not_array(k):
    return k in ("_sn", "__meta__")


def is_analytics(k):
    return k in (
        ("ANALYTICS_RSSI", 1),
        ("ANALYTICS_CAN", 2),
        ("ANALYTICS_SCREEN", 3),
        ("ANALYTICS_CONNECTION_TRACKER", 4),
        ('ANALYTICS_DISCONNECT_REASON', 5),
        ("LOGS", 42),
    )


def is_analytics_with_sn(k):
    return k in (("SPO2_CTRL2", 99),)


def data_initialize():
    data = {
        "packet_counts": {},
        "_sn": {},
        "__meta__": {
            "PWD_VERSION": "0.0.0.0",
            "HIGGINS": "2.0.0",
            "ARRAYS": {},
            "FILE_START_TIME": None,
            "CONSTANTS": {
                "INT_NAN": np.iinfo(np.int32).min,
                "k_ppg_dc": k_ppg_dc,
                "k_ppg_filt": k_ppg_filt,
                "k_ecg": k_ecg,
                "k_ip": k_ip,
                "k_ambient": k_ambient,
            },
        },
    }
    return data


def data_populate_for_conversion(data):
    for spec in packet_map.values():
        if "array" in spec.keys():
            is_waveform = "waveform" in spec.keys()
            key = (spec["array"], spec["id"])
            data[key] = []
            if is_waveform:
                data["_sn"][key] = []
    for spec in analytics_map.values():
        if "array" in spec.keys():
            key = (spec["array"], spec["id"])
            data[key] = []
    return data


def _data_consume_waveform_v0(sn, data, content, key, spp, rows):
    data["_sn"][key].append(sn + np.arange(start=0, stop=rows * spp, step=spp))
    data[key].append(list(content[2:]))


def _data_consume_waveform_v1(sn, data, content, key, spp, rows):
    if content[2] > 0:
        spp = (500 * rows) / content[2]
    data["_sn"][key].append(sn + np.arange(start=0, stop=rows * spp, step=spp))
    data[key].append(list(content[4:]))


def _data_consume_waveform_v2(sn, data, content, key, id_):
    header, samples = content
    packet_map[id_]["waveform"]["rows"] = (
        header[4] / packet_map[id_]["waveform"]["channels"]
    )
    packet_map[id_]["waveform"]["sf"] = header[3]
    spp = 500 / header[3]
    stop = header[4] * spp / packet_map[id_]["waveform"]["channels"]
    data["_sn"][key].append(sn + np.arange(start=0, stop=stop, step=spp))
    data[key].append(list(samples))


def _data_consume_numeric(sn, data, content, key):
    data[key].append([content[0], sn] + list(content[2:]))


def _data_consume_pass_thru_numeric(sn, data, content, key):
    data[key].append([content[0], sn] + list(content[3:]))


def _data_consume_timesync(sn, data, content, key):
    data[key].append([content[0], sn, content[2], content[1]])


def _data_consume_alarms(sn, data, content, key):
    data[("ALARM_STATUS", 100000001)].append(
        [0, sn, content[0][3], content[0][4], content[0][5], content[0][6]]
    )
    if content[0][6] == 0:
        data[key].append([0, sn] + [999, 0, 0, sn])
    else:
        for a in content[1]:
            data[key].append([0, sn] + list(a))


def _data_consume_alarm_limits(sn, data, content, key):
    for al in content[1]:
        data[key].append([0, sn] + list(al))


def _data_consume_sw_version(sn, data, content):
    data["__meta__"]["PWD_VERSION"] = content[2]


def _init_data_consume_func_dict():
    dict_ = {}
    for id_ in packet_map:
        spec = packet_map[id_]
        if "array" in spec.keys():
            key = (spec["array"], spec["id"])
            if "waveform" in spec.keys():
                if spec["waveform"]["version"] == 0:
                    rows = spec["waveform"]["rows"]
                    # channels = spec['waveform']['channels']
                    spp = 500 / spec["waveform"]["sf"]
                    dict_[id_] = partial(
                        _data_consume_waveform_v0, key=key, spp=spp, rows=rows
                    )
                elif spec["waveform"]["version"] == 1:
                    rows = spec["waveform"]["rows"]
                    # channels = spec['waveform']['channels']
                    spp = 500 / spec["waveform"]["sf"]
                    dict_[id_] = partial(
                        _data_consume_waveform_v1, key=key, spp=spp, rows=rows
                    )
                elif spec["waveform"]["version"] == 2:
                    # channels = spec['waveform']['channels']
                    dict_[id_] = partial(_data_consume_waveform_v2, id_=id_, key=key)
            elif id_ == 184:  # ALARMS, ALARM_STATUS
                dict_[id_] = partial(_data_consume_alarms, key=key)
            elif id_ == 186:  # ALARMS_LIMITS
                dict_[id_] = partial(_data_consume_alarm_limits, key=key)
            elif id_ == 209:  # version info
                dict_[id_] = _data_consume_sw_version
            elif id_ == 42:  # logs
                dict_[id_] = partial(logs._data_consume_log, key=key)
            elif id_ == 4:  # time sycn
                dict_[id_] = partial(_data_consume_timesync, key=key)
            elif 2999 < id_ < 4000:
                dict_[id_] = partial(_data_consume_pass_thru_numeric, key=key)
            else:
                dict_[id_] = partial(_data_consume_numeric, key=key)
    dict_[257] = partial(logs._data_consume_analytics, key=key)
    return dict_


def data_consume_packet(data, pid, content):
    try:
        data["packet_counts"][pid] += 1
    except KeyError:
        data["packet_counts"][pid] = 1


def _data_reshape_waveform(data, key, rows, channels):
    logger.info(f"data reshape waveform {key}")
    tmp = data["_sn"][key]
    tmp = np.array(tmp, dtype="<f8").astype(np.int32)
    tmp = tmp.reshape((tmp.shape[0] * tmp.shape[1],))
    data[key] = np.array(data[key], dtype="<f8")
    # reshape data array
    if channels > 1:
        length = int(data[key].shape[0] * rows)
        data[key] = data[key].reshape((length, channels))
    else:
        data[key] = data[key].reshape((data[key].shape[0] * data[key].shape[1],))
    # merge sequence number, time, and data vectors
    data[key] = np.c_[tmp, np.zeros_like(tmp), data[key]]


def _data_reshape_misc(data, key):
    logger.info(f"data reshape misc {key}")
    data[key] = np.array(data[key], dtype="<f8")
    tmp = data[key][:, 1].copy()
    data[key][:, 1] = data[key][:, 0]
    data[key][:, 0] = tmp


def _data_reshape_logs(data, key):
    logger.info(f"data reshape logs {key}")
    data[key] = np.array(data[key])
    data[key] = np.c_[np.zeros((data[key].shape[0],)), data[key]]


def _data_reshape_analytics(data, key):
    logger.info(f"data reshape analytics {key}")
    data[key] = np.array(data[key], dtype="<f8")
    data[key] = np.c_[np.zeros((data[key].shape[0],)), data[key]]


def _init_data_reshape_func_dict():
    dict_ = {}
    for id_ in packet_map:
        spec = packet_map[id_]
        if "array" in spec.keys():
            key = (spec["array"], spec["id"])
            if "waveform" in spec.keys() and "rows" in spec["waveform"].keys():
                rows = spec["waveform"]["rows"]
                channels = spec["waveform"]["channels"]
                dict_[key] = partial(
                    _data_reshape_waveform, key=key, rows=rows, channels=channels
                )
            elif key == ("LOGS", 42):
                dict_[key] = partial(_data_reshape_logs, key=key)
            else:
                dict_[key] = partial(_data_reshape_misc, key=key)
        else:
            key = (None, spec["id"])
            dict_[key] = partial(_data_reshape_misc, key=key)
    for id_ in analytics_map:
        spec = analytics_map[id_]
        key = (spec["array"], spec["id"])
        dict_[key] = partial(_data_reshape_analytics, key=key)

    return dict_


def data_finalize_reshape(data):
    data_reshape_function_dict = _init_data_reshape_func_dict()
    keys = list(data.keys())
    for key in keys:
        if not_array(key):
            continue
        arrlen = len(data[key])
        if arrlen > 0:
            try:
                if key in data_reshape_function_dict.keys():
                    data_reshape_function_dict[key](data)
            except:  # noqa
                logger.exception(
                    "Error reshaping {} {} ({})".format(key[0], key[1], arrlen)
                )
        else:
            del data[key]


def data_finalize_unmangle_ecg_waveforms(data):
    """Unmagling ECG Waveforms"""
    for k in data.keys():
        if k[0][:3] == "ECG":
            data[k][:, 2] = np.right_shift(data[k][:, 2].astype(np.int32), 8)


def data_finalize_unmangle_nibp_numeric(data):
    """Unmagling NIBP Numerics"""
    if ("BP_PKT", 18) in data.keys():
        data[("BP_PKT", 18)] = data[("BP_PKT", 18)][:, (0, 1, 3, 4, 5, 2, 6, 7, 8)]
        # find new BP, if any
        tmp = data[("BP_PKT", 18)][data[("BP_PKT", 18)][:, -1] == 0, :]
        if tmp.shape[0] > 0:
            # new BP, create BP array with one entry per inflation
            data[("BP", None)] = tmp[np.r_[True, np.diff(tmp[:, 5]) > 0], :]


def data_finalize_unmangle_cnibp_numeric(data):
    """Unmagling CNIBP Numerics"""
    for k in data.keys():
        if k[0] == "CNIBP":
            data[k] = data[k][:, (0, 1, 3, 4, 5, 7, 6, 8, 9, 2)]


def data_finalize_unmangle_hr_numeric(data):
    """Unmagling HR Numerics"""

    if ("HR_RHYTHM", 227) in data.keys():
        data[("HR", None)] = data[("HR_RHYTHM", 227)][:, (0, 1, 2)]
    elif ("HR_SCI", 210) in data.keys():
        data[("HR", None)] = data[("HR_SCI", 210)][:, (0, 1, 2)]


def data_finalize_merge_pres_waveforms(data):

    """Merging Pressure waveforms"""
    try:
        OSC_KEY = None
        for k in data.keys():
            if k[0] == "OSC":
                OSC_KEY = k
        P1_KEY = None
        for k in data.keys():
            if k[0] == "P1":
                P1_KEY = k
        P2_KEY = None
        for k in data.keys():
            if k[0] == "P2":
                P2_KEY = k
        OSC = data[OSC_KEY]
        P1 = data[P1_KEY]
        P2 = data[P2_KEY]
    except KeyError:
        return

    min_sqn = min(
        OSC[0, 0].astype(np.int32), P1[0, 0].astype(np.int32), P2[0, 0].astype(np.int32)
    )
    max_sqn = max(
        OSC[-1, 0].astype(np.int32),
        P1[-1, 0].astype(np.int32),
        P2[-1, 0].astype(np.int32),
    )

    PRES = -1 * np.ones((max_sqn - min_sqn + 1, 5))
    PRES[:, 0] = np.arange(min_sqn, max_sqn + 1)

    vals, i = np.unique(OSC[:, 0], return_index=True)  # remove dups
    OSC = OSC[i, :]
    i = np.in1d(PRES[:, 0], OSC[:, 0])
    PRES[i, 1] = OSC[:, 1]
    PRES[i, 2] = OSC[:, 2]

    vals, i = np.unique(P1[:, 0], return_index=True)  # remove dups
    P1 = P1[i, :]
    i = np.in1d(PRES[:, 0], P1[:, 0])
    PRES[i, 1] = P1[:, 1]
    PRES[i, 3] = P1[:, 2]

    vals, i = np.unique(P2[:, 0], return_index=True)  # remove dups
    P2 = P2[i, :]
    i = np.in1d(PRES[:, 0], P2[:, 0])
    PRES[i, 1] = P2[:, 1]
    PRES[i, 4] = P2[:, 2]

    i = (
        (PRES[:, 1] == -1)
        * (PRES[:, 2] == -1)
        * (PRES[:, 3] == -1)
        * (PRES[:, 4] == -1)
    )
    PRES = PRES[~i, :]

    data[("PRES", None)] = PRES
    del data[OSC_KEY], data[P1_KEY], data[P2_KEY]


def data_finalize_replace_values(data):
    """Replacing encoded values"""
    for key in data.keys():
        if key[0] in ("HR", "HR_SCI", "HR_RHYTHM", "RR", "TEMP", "PR", "SPO2"):
            data[key] = replace_constants(data[key], 2)
        elif key[0] in ("BP_PKT", "BP", "CNIBP"):
            data[key] = replace_constants(data[key], (2, 3, 4))


def data_finalize_time_conversions(data, min_sn, max_sn, time_sync=None):
    """Creating time vectors"""
    # re-arrange TIME_SYNC columns
    time_sync_key = ("TIME_SYNC", 4)
    if time_sync_key in data.keys():
        tmp = data[time_sync_key][:, 1].copy()
        data[time_sync_key][:, 1] = data[time_sync_key][:, 2] / 1000.0
        data[time_sync_key][:, 2] = tmp
        tsync = data[time_sync_key].copy()
    else:
        tsync = time_sync

    if tsync is None:
        raise ValueError("Finalize time: no time sync available")

    # find first and last sqn in an array in block
    sqn0 = tsync[0, 0]
    sqnN = tsync[-1, 0]
    for k in data.keys():
        logger.info(f"{k} - making time")
        if k == ("SOFTWARE_VERSION", 259):
            logger.info(f"{data[k]}")
        if not_array(k) or k == time_sync_key:
            logger.debug("Skipping time finalize for {}".format(k))
            continue
        if is_analytics_with_sn(k):
            logger.debug("Finalizing time for ANALYTICS WITH SN {}".format(k))
            tmp = np.empty((data[k].shape[0], data[k].shape[1] - 1))
            tmp[:, 0] = data[k][:, 2]
            tmp[:, 2:] = data[k][:, 3:]
            data[k] = tmp
            idx = np.argsort(data[k][:, 0])  # sort on sequence numbers
            data[k] = data[k][idx, :]  # in the block
            idx = data[k][:, 0] > 0
            data[k] = data[k][idx, :]  # exclude bad sqn
            if data[k].shape[0] > 1:
                sqnN = data[k][-1, 0] if data[k][-1, 0] > sqnN else sqnN
                sqn0 = data[k][0, 0] if min_sn < data[k][0, 0] < sqn0 else sqn0
                logger.warning(f"{k}({data[k].shape}): sqn0 = {sqn0}, sqnN = {sqnN}")

        elif is_analytics(k):
            logger.debug("Finalizing time for ANALYTICS {}".format(k))
            idx = np.argsort(data[k][:, 1])  # sort on timestamps
            data[k] = data[k][idx, :]  # in the block

        # scan the rest of the arrays for sequence numbers before and after
        # the time-sync packets first and last
        elif k != time_sync_key and data[k].shape[1] > 1:
            logger.debug("Finalizing time for {}".format(k))
            idx = np.argsort(data[k][:, 0])  # sort on sequence numbers
            data[k] = data[k][idx, :]  # in the block
            idx = data[k][:, 0] > 0
            data[k] = data[k][idx, :]  # exclude bad sqn
            if data[k].shape[0] > 1:
                sqnN = data[k][-1, 0] if data[k][-1, 0] > sqnN else sqnN
                sqn0 = data[k][0, 0] if data[k][0, 0] < sqn0 else sqn0
                logger.warning(f"{k}({data[k].shape}): sqn0 = {sqn0}, sqnN = {sqnN}")

        else:
            logger.warning("Unable to finalize time for {}".format(k))

    # there should not be sequence numbers < min_sn
    if sqn0 < min_sn:
        msg = "Finalize time: invalid sn: snq0 ({}) < min_sn ({})".format(sqn0, min_sn)
        raise ValueError(msg)

    # there can be sequence numbers > max_sn
    if sqnN > max_sn:
        max_sn = sqnN

    # create a time sync array augmented for any sequence number that fall before
    # time <-> sqn mapping using time syncs and
    if max_sn > tsync[-1, 0]:
        # tack on extra points to time
        times = np.r_[tsync[:, 1], tsync[-1, 1] + 2 * (max_sn - tsync[-1, 0]) / 1000.0]
        sqnums = np.r_[tsync[:, 0], max_sn]
    else:
        times = tsync[:, 1].copy()
        sqnums = tsync[:, 0].copy()

    if min_sn < tsync[0, 0]:
        times = np.r_[tsync[0, 1] + 2 * (min_sn - tsync[0, 0]) / 1000.0, times]
        sqnums = np.r_[min_sn, sqnums]

    for k in data.keys():
        if not_array(k):
            continue
        if is_analytics(k) and data[k].shape[1] > 1:
            tm = data[k][:, 1].astype(float) / 1000.0
            sn = np.round(np.interp(tm, times, sqnums)).astype(int)
            data[k][:, 0] = sn
            data[k][:, 1] = tm
            # get rid of rows outsite the time range of this block
            data[k] = data[k][(sn >= min_sn) * (sn <= max_sn), :]
        elif is_analytics_with_sn(k) and data[k].shape[1] > 1:
            # get rid of rows outsite the time range of this block
            sn = data[k][:, 0]
            data[k][:, 1] = np.interp(data[k][:, 0], sqnums, times)
            data[k] = data[k][(sn >= min_sn) * (sn <= max_sn), :]

        elif k != time_sync_key and data[k].shape[1] > 1:
            data[k][:, 1] = np.interp(data[k][:, 0], sqnums, times)
    return times[0], times[-1]


def data_finalize_fake_time(data, min_sn, max_sn):
    min_time = 2 * min_sn / 1000.0
    max_time = 2 * max_sn / 1000.0
    for k in data.keys():
        if not_array(k):
            continue
        if is_analytics(k) and data[k].shape[1] > 1:
            continue
        if data[k].shape[1] > 1:
            idx = np.argsort(data[k][:, 0])  # sort sequence numbers
            data[k] = data[k][idx, :]  # in the block
            sn = data[k][:, 0].astype(int)
            data[k][:, 1] = 2 * sn / 1000.0
            min_time = min(min_time, data[k][:, 1].min())
            max_time = max(max_time, data[k][:, 1].max())
    return min_time, max_time


def data_finalize_meta_data(data, min_time, max_time, devices):
    # add min time (t0)
    data["__meta__"]["T0"] = 0 if min_time is None else min_time
    start_time = datetime.fromtimestamp(data["__meta__"]["T0"])
    data["__meta__"]["FILE_START_TIME"] = start_time.strftime("%D %H:%M:%S.%f")

    # add max time (t1)
    data["__meta__"]["T1"] = 1 if max_time is None else max_time
    stop_time = datetime.fromtimestamp(data["__meta__"]["T1"])
    data["__meta__"]["FILE_STOP_TIME"] = stop_time.strftime("%D %H:%M:%S.%f")

    data["__meta__"]["DEVICES"] = list(devices.keys())
    return data


def data_finalize_array_names(data):
    new_dict = {}
    for key in data.keys():
        if type(key) == tuple:
            new_dict[key[0]] = data[key]
        else:
            new_dict[key] = data[key]
    return new_dict


def packet_ok(pid, sn, device, min_sn, max_sn):
    if pid in (42, 257):
        return True
    elif sn is not None and sn >= min_sn and sn <= max_sn:
        return True
    return False


def convert_block(
    blockmap, time_sync=None, do_optimize=True, chunk_path=None, blocknum=-1
):
    data_consume_function_dict = _init_data_consume_func_dict()
    data = data_initialize()
    data = data_populate_for_conversion(data)
    max_sn = int(blockmap["max_sn"])
    min_sn = 0 if blocknum == 0 else int(blockmap["min_sn"])
    devices = {}
    for chunk in blockmap["chunks"]:
        fn = (
            chunk["file"]
            if chunk_path is None
            else os.path.join(chunk_path, chunk["file"])
        )
        with open(fn, "rb") as fp:
            for pid, sn, tm, device, segment, content, string in packets.spool_packets(
                fp
            ):
                if packet_ok(pid, sn, device, min_sn, max_sn):
                    try:
                        func = data_consume_function_dict[pid]
                    except KeyError:
                        pass
                    else:
                        if content is not None:
                            func(sn if sn is not None else tm, data, content)
                if device:
                    try:
                        devices[device] += 1
                    except KeyError:
                        devices[device] = 1

    data_finalize_reshape(data)
    data_finalize_unmangle_ecg_waveforms(data)
    data_finalize_unmangle_nibp_numeric(data)
    data_finalize_unmangle_cnibp_numeric(data)
    data_finalize_unmangle_hr_numeric(data)

    if ("TIME_SYNC", 4) in data.keys():
        min_time, max_time = data_finalize_time_conversions(data, min_sn, max_sn)
    elif time_sync is not None:
        min_time, max_time = data_finalize_time_conversions(
            data, min_sn, max_sn, time_sync
        )
    else:
        min_time, max_time = data_finalize_fake_time(data, min_sn, max_sn)

    data_finalize_merge_pres_waveforms(data)
    data_finalize_replace_values(data)

    if "_sn" in data.keys():
        del data["_sn"]
    data = data_finalize_array_names(data)
    data = data_finalize_meta_data(data, min_time, max_time, devices)
    if do_optimize:
        data = optimize(data)

    return data
