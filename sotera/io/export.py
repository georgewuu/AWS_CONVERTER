import csv
import pandas
import numpy as np
from itertools import count
from collections import OrderedDict
from sotera.util.time import get_string_from_timestamp
from sotera.io.visi import constants
from .local import inflate_ppg_arrays, inflate_ecg_arrays


def numeric_value(arr, t, WINDOW, col):
    val = None
    idx = (arr[:, 1] >= t) * (arr[:, 1] < t + WINDOW)
    if np.sum(idx) > 0:
        i = arr[idx, 2] > -1
        if np.sum(i) > 0:
            # there are measurements
            val = f"{int( np.median( arr[idx, col][i] ) )}"

        else:
            # there are not measurements
            if np.sum((arr[idx, col] < 0) * (arr[idx, col] > -4)) > 0:
                # there are XX meansurements
                val = "XX"
            else:
                val = "++" if np.sum(arr[idx, 2] == -4) > 0 else "--"
    return val


def alarm_value(arr, t, WINDOW, col):
    val = None
    idx = (arr[:, 1] >= t) * (arr[:, 1] < t + WINDOW)
    if np.sum(idx) > 0:
        val = " ".join(
            constants.alarm_codes[int(c)]
            for c in set(arr[idx, col].tolist())
            if c < 999
        )
    return val


def posture_value(arr, t, WINDOW, col):
    val = None
    idx = (arr[:, 1] >= t) * (arr[:, 1] < t + WINDOW)
    if np.sum(idx) > 0:
        val = " ".join(
            constants.posture_codes[int(c)] for c in set(arr[idx, col].tolist())
        )
    return val


def ltaa_value(arr, t, WINDOW, col):
    val = None
    idx = (arr[:, 1] >= t) * (arr[:, 1] < t + WINDOW)
    if np.sum(idx) > 0:
        val = " ".join(constants.ltaa_codes[int(c)] for c in set(arr[idx, 6].tolist()))
    return val


def export_columns(device=True, alarms=True, posture=True, ltaa=True):
    cols = OrderedDict()
    cols["Time"] = None
    if device:
        cols["Device"] = None
    cols["SPO2"] = {"array": "SPO2", "col": 2, "func": numeric_value}
    cols["HR"] = {"array": "HR", "col": 2, "func": numeric_value}
    cols["PR"] = {"array": "PR", "col": 2, "func": numeric_value}
    cols["RR"] = {"array": "RR", "col": 2, "func": numeric_value}
    cols["CNIBP_SYS"] = {"array": "CNIBP", "col": 2, "func": numeric_value}
    cols["CNIBP DIA"] = {"array": "CNIBP", "col": 3, "func": numeric_value}
    cols["CNIBP MAP"] = {"array": "CNIBP", "col": 4, "func": numeric_value}
    cols["BP SYS"] = {"array": "BP", "col": 2, "func": numeric_value}
    cols["BP DIA"] = {"array": "BP", "col": 3, "func": numeric_value}
    cols["BP MAP"] = {"array": "BP", "col": 4, "func": numeric_value}
    if alarms:
        cols["ALARMS"] = {"array": "ALARMS", "col": 2, "func": alarm_value}
    if posture:
        cols["POSTURE"] = {"array": "POSTURE_PKT", "col": 6, "func": posture_value}
    if ltaa:
        cols["LTAA"] = {"array": "HR_RHYTHM", "col": 6, "func": ltaa_value}
    return cols


def numerics_export(data, columns=None, WINDOW=15, timezone=None, add_header=True):
    """
    Write standard visi numerics data to cc text export file
    """

    if columns is None:
        columns = export_columns()

    arrays_in_data = list(
        set(
            c["array"]
            for c in columns.values()
            if c is not None and c["array"] in data.keys()
        )
    )

    if len(arrays_in_data) == 0:
        yield None

    tmin = np.floor(
        min((data[k][0, 1] for k in arrays_in_data))
    )  # earliest second in file
    tmax = np.ceil(max((data[k][-1, 1] for k in arrays_in_data)))  # last second in file
    times = np.arange(tmin, tmax + WINDOW, WINDOW)  # get timestamps for all averages

    # use meta data info
    if "__metas__" in data.keys():
        L = len(data["__metas__"])
        devices = np.zeros_like(times)
        for i in range(L):
            last_t1 = data["__metas__"][i - 1]["T1"]
            t1 = data["__metas__"][i]["T1"]
            device = data["__metas__"][i]["DEVICES"][0]
            if i == 0 and L == 1:
                devices[:] = device
            elif i == 0:
                idx = times <= t1
                devices[idx] = device
            elif i == (L - 1):
                idx = times > last_t1
                devices[idx] = device
            else:
                idx = (times > last_t1) * (times <= t1)
                devices[idx] = device
    else:
        devices = None

    if add_header:
        yield list(columns.keys())

    for t in times:
        row = [None] * len(columns)
        for i, col_name in enumerate(columns.keys()):
            if col_name == "Time":
                if timezone is not None:
                    row[i] = get_string_from_timestamp(t + WINDOW / 2.0, timezone)
                else:
                    row[i] = f"{int(t+WINDOW/2.)}"
                continue
            elif col_name == "Device" and devices is not None:
                idx = (times >= t) * (times < t + WINDOW)
                row[i] = f"{int(devices[idx][0])}"
                continue

            array_name = columns[col_name]["array"]
            if array_name in arrays_in_data:
                func_ = columns[col_name]["func"]
                val = func_(data[array_name], t, WINDOW, columns[col_name]["col"])
                row[i] = val.strip() if type(val) is str else val

        if row[2:] != [None] * 2 * len(columns):
            yield row


def numerics_to_csv(data, filename, **kwargs):
    """
    Write standard visi numerics data to csv text export file
    """
    csvfile = open(filename, "w")
    csvwriter = csv.writer(csvfile, delimiter=",")
    for row in numerics_export(data, **kwargs):
        csvwriter.writerow(row)
    csvfile.close()
    return True


def numerics_to_xls_worksheet(data, worksheet, **kwargs):

    """
    Write standard visi numerics data to cc text export file
    """
    for nrow, row in enumerate(numerics_export(data, **kwargs)):
        for ncol, item in enumerate(row):
            worksheet.write(nrow, ncol, item)


def scrub(
    data, time_idx=1, tc=lambda t: t, zero_time=False, export_alarms=False, export_ltaa=False, pgsql_=None
):
    scrubbed = {}

    if "ECG" in data.keys() and "ECG_II" not in data.keys():
        data = inflate_ecg_arrays(data)

    for k in ("ECG_I", "ECG_II", "ECG_III"):
        if k in data.keys():
            ecg = data[k]
            scrubbed[k] = np.c_[tc(ecg[:, time_idx]), constants.k_ecg * ecg[:, 2]]

    if "PPG" in data.keys() and "IR_FILT" not in data.keys():
        data = inflate_ppg_arrays(data)

    if "IR_FILT" in data.keys():
        scrubbed["IR_PPG"] = np.c_[
            tc(data["IR_FILT"][:, time_idx]),
            constants.k_ppg_filt * data["IR_FILT"][:, 2],
        ]

    if "RED_FILT" in data.keys():
        scrubbed["RED_PPG"] = np.c_[
            tc(data["RED_FILT"][:, time_idx]),
            constants.k_ppg_filt * data["RED_FILT"][:, 2],
        ]

    for k in ("ACC_ECG", "ACC_ARM", "ACC_WRT"):
        if k in data.keys():
            scrubbed[k] = np.c_[
                tc(data[k][:, time_idx]),
                data[k][:, 2] / 256.0,
                data[k][:, 3] / 256.0,
                data[k][:, 4] / 256.0,
            ]
    if "IP" in data.keys():
        scrubbed["IP"] = np.c_[
            tc(data["IP"][:, time_idx]), data["IP"][:, 2] * constants.k_ip
        ]
    if "PRES" in data.keys():
        scrubbed["BP_PRES"] = np.c_[
            tc(data["PRES"][:, time_idx]),
            data["PRES"][:, 2] / 8.0,
            data["PRES"][:, 3] / 8.0,
        ]
    for k in ("SPO2", "PR", "RR"):
        if k in data.keys():
            scrubbed[k] = np.c_[tc(data[k][:, time_idx]), data[k][:, 2]]

    k = 'HR'
    if k in data.keys():
        if export_ltaa:
            scrubbed[k] = np.c_[tc(data[k][:, time_idx]), data[k][:, 2], data[k][:, 6]]
        else:
            scrubbed[k] = np.c_[tc(data[k][:, time_idx]), data[k][:, 2]]

    for k in ("BP", "CNIBP"):
        if k in data.keys():
            scrubbed[k] = np.c_[
                tc(data[k][:, time_idx]), data[k][:, 2], data[k][:, 3], data[k][:, 4]
            ]
    if "TEMP" in data.keys():
        scrubbed["TEMP"] = np.c_[
            tc(data["TEMP"][:, time_idx]), data["TEMP"][:, 2] / 100.0
        ]

    if export_alarms and pgsql_ is not None and "ALARMS" in data.keys():
        with pgsql_, pgsql_.cursor() as cursor:
            cursor.execute("select code, cipher from enums.alarm_code_type")
            ac = dict(cursor.fetchall())
        EXPORT_ALARMS = []
        cnts = {}
        for row in data["ALARMS"]:
            try:
                cnts[(row[2], row[5])] += 1
            except KeyError:
                cnts[(row[2], row[5])] = 1
                EXPORT_ALARMS.append([tc(row[time_idx]), ac[row[2]]])
        tmp = pandas.DataFrame(EXPORT_ALARMS)
        scrubbed["ALARMS"] = tmp.as_matrix()

    if zero_time:
        min_t = np.inf
        for key in sorted(scrubbed.keys()):
            if scrubbed[key][:, 0].min() < min_t:
                min_t = scrubbed[key][:, 0].min()
        for key in sorted(scrubbed.keys()):
            scrubbed[key][:, 0] = scrubbed[key][:, 0] - min_t
    return scrubbed


def ecg_to_whaleteq_txt(fn, ECG, fs=500, leads=("Lead I", "Lead II", "Lead III")):
    """Export ECG to the WhaleTeq simulator txt format"""
    with open(fn, "w", newline="\r\n") as fp:
        print(f"{fs}", file=fp)
        print(f"{ECG.shape[0]}", file=fp)
        print("start", file=fp)
        for i, lead in zip(count(2), leads):
            print(f"{lead}", file=fp)
            for samp in ECG[:, i].astype(np.float):
                if not np.isnan(samp):
                    v = 1000 * constants.k_ecg * samp
                    v = int(v) if -5000 < v < 5000 else int(np.sign(v)) * 4999
                print(f"{v}", file=fp)
