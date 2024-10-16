import os
import numpy as np
import lzma
import json
import gzip
import re
import datetime
from sotera.io import OTHER_KEYS, ECG_KEYS, PPG_KEYS, INTONLY, arrays_to_get


def save_metadata(folder, meta, fn="meta.json"):
    fn = "{}/{}".format(folder, fn)
    with open(fn, "w") as fp:
        json.dump(meta, fp)
    return fn


def load_metadata(folder, fn="meta.json"):
    meta = None
    fn = "{}/{}".format(folder, fn)
    with open(fn, "r") as fp:
        meta = json.load(fp)
    return meta


def merge_metadata(partial_meta, meta=None):
    if meta is None:
        meta = {"ARRAYS": {}, "DEVICES": []}

    for k in partial_meta.keys():
        if k != "ARRAYS":
            meta[k] = partial_meta[k]

    for k in partial_meta["ARRAYS"].keys():
        meta["ARRAYS"][k] = partial_meta["ARRAYS"][k]

    meta["DEVICES"] += partial_meta["DEVICES"]
    meta["DEVICES"] = list(set(meta["DEVICES"]))
    return meta


def save_arrays(folder, data, use_compression=True):
    fn_list = []
    for name in data.keys():
        if name == "__meta__":
            continue

        if use_compression:
            fn = "{}/{}.npy.xz".format(folder, name)
            with lzma.open(fn, "wb") as fp:
                np.save(fp, data[name])
        else:
            fn = "{}/{}.npy".format(folder, name)
            with open(fn, "wb") as fp:
                np.save(fp, data[name])

        fn_list.append(fn)
    return fn_list


def load_array(folder, name):
    try:
        fn = "{}/{}.npy.xz".format(folder, name)
        with lzma.open(fn, "rb") as cfp:
            arr = np.load(cfp)
    except IOError:
        fn = "{}/{}.npy".format(folder, name)
        with open(fn, "rb") as cfp:
            arr = np.load(cfp)
    return arr


def load_log_file(folder, name, device_id):
    fn = "{}/{}".format(folder, name)
    data = []
    with gzip.open(fn, "rb") as f:
        for line in f:
            y = line.split("\t")
            if len(y) > 1:
                try:
                    new_row = []
                    m = re.search(
                        "deviceID=([0-9]{6}).*Date=([0-9]{2} [A-Z][a-z]{2} "
                        "[0-9]{4} [0-9]{2}:[0-9]{2}:[0-9]{2})",
                        y[0],
                    )
                    new_row.extend([int(m.groups()[0]), m.groups()[1]])
                    for item in y[1:]:
                        new_row.append(item.strip())
                    new_row[1] = datetime.datetime.strptime(
                        new_row[1], "%d %b %Y %H:%M:%S"
                    )
                    if device_id != "all" and device_id != int(m.groups()[0]):
                        continue
                    data.append(new_row)
                except:  # noqa E722
                    continue

    return data


def save_block(folder, data, use_compression=True, metafn="meta.json"):
    if not os.path.isdir(folder):
        os.mkdir(folder)
    fn_list = save_arrays(folder, data, use_compression=use_compression)
    if "__meta__" in data.keys():
        fn_list.append(save_metadata(folder, data["__meta__"], fn=metafn))
    return fn_list


def load_block(folder, arrays=None, metafn="meta.json", exclude_variables=None):
    data = dict(__meta__=load_metadata(folder, fn=metafn))
    if arrays is None:
        arrays = list(data["__meta__"]["ARRAYS"].keys())
    if exclude_variables is not None:
        arrays = list(set(arrays) - set(exclude_variables))
    for array_name in arrays_to_get(arrays, data["__meta__"]["ARRAYS"].keys()):
        arr = load_array(folder, array_name)
        if do_inflate(array_name, arr):
            data[array_name] = inflate_array(arr)
        else:
            data[array_name] = arr

    if "TIME_SYNC" in data.keys():
        data = derive_timestamps(data)
        if data["TIME_SYNC"].shape[0] > 0 and data["TIME_SYNC"].shape[1] < 4:
            data["TIME_SYNC"] = np.c_[
                data["TIME_SYNC"], np.zeros((data["TIME_SYNC"].shape[0],))
            ]

    data = inflate_ppg_arrays(data)
    data = inflate_ecg_arrays(data)

    for k in ("HIGGINS", "PWD_VERSION", "FILE_START_TIME", "DEVICES"):
        data[k] = data["__meta__"][k]
    for k in data["__meta__"]["CONSTANTS"]:
        data[k] = data["__meta__"]["CONSTANTS"][k]
    return data


def merge_blocks(data, block_data, check=True):
    if check and "TIME_SYNC" not in block_data.keys():
        return data
    for var in block_data.keys():
        if type(block_data[var]) is np.ndarray:
            if (
                len(block_data[var].shape) > 1 and
                block_data[var].shape[1] > 1 and
                block_data[var].shape[0] > 0
            ):
                arr = var
                if (
                    var == "CNIBP_CAL_PKT"
                    and "CNIBP_CAL_PKT" in data.keys()
                    and block_data[var].shape[1] != data[var].shape[1]
                ):
                    # handle case where there are two differnt
                    # sized CNIBP packets in one file
                    arr = "CNIBP_CAL_PKT_V2"
                block_data[var] = block_data[var]
                try:
                    data[arr] = np.vstack((data[arr], block_data[var]))
                except KeyError:
                    data[arr] = block_data[var]

    if "__metas__" not in data.keys():
        data["__metas__"] = []
    try:
        data["__metas__"].append(block_data["__meta__"])
    except KeyError:
        if check:
            raise

    if "BP" in data.keys() and data["BP"].shape[0]:
        # exclude dubplicate BP readings from the same inflation
        idx = np.r_[True, np.diff(data["BP"][:, 5]) != 0]
        if np.sum(idx) > 0:
            data["BP"] = data["BP"][idx, :]
    return data


def do_inflate(arr, data):
    if arr == "TIME_SYNC":
        return False
    elif arr == "ALARM_LIMITS" and data.shape[1] > 3:
        return False
    elif arr == "ALARM_STATUS" and data.shape[1] > 5:
        return False
    return True


def inflate_array(opt):
    arr = np.hstack((opt[:, (0,)], np.zeros((opt.shape[0], 1)), opt[:, 1:]))
    return arr


def derive_timestamps(data):
    TS = data["TIME_SYNC"]
    # find first and last sqn in an array in block
    sqn0 = int(TS[0, 0])
    sqnN = int(TS[-1, 0])
    t0 = float(TS[0, 1])
    if sqnN < sqn0:
        # reset or device swap, remove last time sync because
        # it is from the next segment
        TS = TS[:-1, :]
    for k in data.keys():
        # scan the rest of the arrays for sequence numbers before and after
        # the time-sync packets first and last
        if k[0] != "_" and k != "TIME_SYNC" and data[k].shape[1] > 1:
            sqnN = int(data[k][-1, 0]) if int(data[k][-1, 0]) > sqnN else sqnN
            sqn0 = int(data[k][0, 0]) if int(data[k][0, 0]) < sqn0 else sqn0
    # create a time sync array augmented for any sequence number that fall
    # before
    # time <-> sqn mapping using time syncs and
    if sqnN > TS[-1, 0]:
        # tack on extra points to time
        times = np.r_[TS[:, 1], TS[-1, 1] + 2 * (sqnN - TS[-1, 0]) / 1000.0]
        sqnums = np.r_[TS[:, 0], sqnN]
    else:
        times = TS[:, 1].copy()
        sqnums = TS[:, 0].copy()
    if sqn0 < TS[0, 0]:
        times = np.r_[TS[0, 1] + 2 * (sqn0 - TS[0, 0]) / 1000.0, times]
        sqnums = np.r_[sqn0, sqnums]
    for k in data.keys():
        if k[0] != "_" and k != "TIME_SYNC" and data[k].shape[1] > 1:
            data[k][:, 1] = np.interp(data[k][:, 0].astype(float), sqnums, times)
            # exclude data that is > 30 sec before the first time sync packet
            i = data[k][:, 1].astype(float) > t0 - 30
            data[k] = data[k][i, :]
    data["T0"] = times[0]
    data["T1"] = times[-1]
    return data


def inflate_ecg_arrays(data):
    if "ECG" in data.keys():
        int_nan = data["__meta__"]["CONSTANTS"]["INT_NAN"]
        ECG = data["ECG"]
        for k in data["__meta__"]["ARRAYS"]["ECG"]["columns"]:
            c = data["__meta__"]["ARRAYS"]["ECG"]["columns"][k]
            data[k] = np.c_[ECG[:, 0], ECG[:, 1], ECG[:, c + 1]]
            data[k] = data[k][~(data[k][:, 2] == int_nan), :]
    return data


def inflate_ppg_arrays(data):
    if "PPG" in data.keys():
        int_nan = data["__meta__"]["CONSTANTS"]["INT_NAN"]
        PPG = data["PPG"]
        for k in data["__meta__"]["ARRAYS"]["PPG"]["columns"]:
            c = data["__meta__"]["ARRAYS"]["PPG"]["columns"][k]
            data[k] = np.c_[PPG[:, 0], PPG[:, 1], PPG[:, c + 1]]
            data[k] = data[k][~(data[k][:, 2] == int_nan), :]
    return data


def deflate_array(data, name):
    if name in INTONLY:
        opt = np.delete(data[name], 1, 1).astype(np.int32)
    else:
        opt = np.delete(data[name], 1, 1)
    meta = dict(shape=opt.shape)
    return opt, meta


def deflate_ecg_arrays(data, keys):
    meta = dict(columns={})
    sn_min = min([data[k][:, 0].min().astype(np.int32) for k in keys])
    sn_max = max([data[k][:, 0].max().astype(np.int32) for k in keys])
    ecg = np.ones((sn_max - sn_min + 1, len(keys) + 1)) * np.iinfo(np.int32).min
    ecg[:, 0] = np.arange(sn_min, sn_max + 1)
    c = 1
    for k in ECG_KEYS:
        try:
            if np.sum(np.diff(data[k][:, 0]) == 0):
                # duplicate sample(s)!
                sn, i = np.unique(data[k][:, 0], return_index=True)
                idx = np.in1d(ecg[:, 0], sn)
                ecg[idx, c] = data[k][i, 2]
            else:
                idx = np.in1d(ecg[:, 0], data[k][:, 0])
                ecg[idx, c] = data[k][:, 2]
            meta["columns"][k] = c
            del idx
        except KeyError:
            pass
        else:
            c += 1
    ecg = ecg.astype(np.int32)
    meta["shape"] = ecg.shape
    return ecg, meta


def deflate_ppg_arrays(data, keys):
    meta = dict(columns={})
    sn_min = min([data[k][:, 0].min().astype(np.int32) for k in keys])
    sn_max = max([data[k][:, 0].max().astype(np.int32) for k in keys])
    ppg = np.ones((sn_max - sn_min + 1, len(keys) + 1)) * np.iinfo(np.int32).min
    ppg[:, 0] = np.arange(sn_min, sn_max + 1)
    c = 1
    for k in PPG_KEYS:
        try:
            if np.sum(np.diff(data[k][:, 0]) == 0):
                # duplicate sample(s)!
                sn, i = np.unique(data[k][:, 0], return_index=True)
                idx = np.in1d(ppg[:, 0], sn)
                ppg[idx, c] = data[k][i, 2]
            else:
                idx = np.in1d(ppg[:, 0], data[k][:, 0])
                ppg[idx, c] = data[k][:, 2]
            meta["columns"][k] = c
            del idx
        except KeyError:
            pass
        else:
            c += 1
    ppg = ppg.astype(np.int32)
    meta["shape"] = ppg.shape
    return ppg, meta


def optimize(data):
    # optimize data
    if "TIME_SYNC" in data.keys():
        data["__meta__"]["ARRAYS"]["TIME_SYNC"] = {"shape": data["TIME_SYNC"].shape}
    ecg_arrays = [k for k in data.keys() if k in ECG_KEYS]
    if len(ecg_arrays):
        data["ECG"], data["__meta__"]["ARRAYS"]["ECG"] = deflate_ecg_arrays(
            data, ecg_arrays
        )
    ppg_arrays = [k for k in data.keys() if k in PPG_KEYS]
    if len(ppg_arrays):
        data["PPG"], data["__meta__"]["ARRAYS"]["PPG"] = deflate_ppg_arrays(
            data, ppg_arrays
        )
    keys = list(data.keys())
    for key in keys:
        if key in OTHER_KEYS:
            data[key], data["__meta__"]["ARRAYS"][key] = deflate_array(data, key)
        elif key in ECG_KEYS:
            del data[key]
        elif key in PPG_KEYS:
            del data[key]
    return data
