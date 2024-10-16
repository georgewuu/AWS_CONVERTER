import numpy as np
from psycopg2.extras import DictCursor

AA_PARAMS = {
    "SPO2": ["LOW"],
    "PR": ["LOW", "HIGH"],
    "HR": ["LOW", "HIGH"],
    "RR": ["LOW", "HIGH"],
    "BP_SYS": ["LOW", "HIGH"],
    "BP_DIA": ["LOW", "HIGH"],
    "BP_MAP": ["LOW", "HIGH"],
    "CNIBP_SYS": ["LOW", "HIGH"],
    "CNIBP_DIA": ["LOW", "HIGH"],
    "CNIBP_MAP": ["LOW", "HIGH"],
    "TEMP": ["LOW", "HIGH"],
}

LDR = {
    "SPO2": {
        "HIGH": {"MIN": 101, "MAX": 101, "INC": 1},
        "LOW": {"MIN": 60, "MAX": 100, "INC": 1},
    },
    "PR": {
        "HIGH": {"MIN": 120, "MAX": 180, "INC": 1},
        "LOW": {"MIN": 25, "MAX": 50, "INC": 1},
    },
    "HR": {
        "HIGH": {"MIN": 120, "MAX": 180, "INC": 1},
        "LOW": {"MIN": 25, "MAX": 50, "INC": 1},
    },
    "RR": {
        "HIGH": {"MIN": 15, "MAX": 50, "INC": 1},
        "LOW": {"MIN": 0, "MAX": 15, "INC": 1},
    },
    "BP_SYS": {
        "HIGH": {"MIN": 160, "MAX": 210, "INC": 1},
        "LOW": {"MIN": 65, "MAX": 95, "INC": 1},
    },
    "BP_MAP": {
        "HIGH": {"MIN": 100, "MAX": 180, "INC": 1},
        "LOW": {"MIN": 55, "MAX": 65, "INC": 1},
    },
    "BP_DIA": {
        "HIGH": {"MIN": 100, "MAX": 155, "INC": 1},
        "LOW": {"MIN": 35, "MAX": 60, "INC": 1},
    },
    "CNIBP_SYS": {
        "HIGH": {"MIN": 180, "MAX": 210, "INC": 1},
        "LOW": {"MIN": 65, "MAX": 95, "INC": 1},
    },
    "CNIBP_MAP": {
        "HIGH": {"MIN": 145, "MAX": 180, "INC": 1},
        "LOW": {"MIN": 55, "MAX": 65, "INC": 1},
    },
    "CNIBP_DIA": {
        "HIGH": {"MIN": 115, "MAX": 155, "INC": 1},
        "LOW": {"MIN": 35, "MAX": 45, "INC": 1},
    },
    "TEMP": {
        "HIGH": {"MIN": 92, "MAX": 109, "INC": 1},
        "LOW": {"MIN": 32, "MAX": 92, "INC": 1},
    },
    "DELAY": {"MIN": 0, "MAX": 600, "INC": 5},
}

update_rates_pre_combo = {
    "SPO2": 3.0,
    "PR": 3.0,
    "HR": 5.0,
    "RR": 5.0,
    "TEMP": 6.0,
    "CNIBP_MAP": 5.0,
    "CNIBP_DIA": 5.0,
    "CNIBP_SYS": 5.0,
    "TIME_SYNC": 60.0,
    "HR_A": 1.0,
}
update_rates_combo = {
    "SPO2": 3.0,
    "PR": 3.0,
    "HR": 1.0,
    "RR": 5.0,
    "TEMP": 6.0,
    "CNIBP_MAP": 5.0,
    "CNIBP_DIA": 5.0,
    "CNIBP_SYS": 5.0,
    "TIME_SYNC": 60.0,
    "HR_A": 1.0,
}

bin_defaults = [
    0,
    30,
    60,
    90,
    120,
    150,
    180,
    210,
    240,
    270,
    300,
    360,
    420,
    480,
    540,
    600,
    660,
    720,
    780,
    840,
    900,
    1200,
    1500,
    1800,
    2100,
    2400,
    2700,
    3000,
    3300,
    3600,
    4200,
    4800,
    5400,
    6000,
    6600,
    7200,
    np.inf,
]

bin_labels = [
    30,
    60,
    90,
    120,
    150,
    180,
    210,
    240,
    270,
    300,
    360,
    420,
    480,
    540,
    600,
    660,
    720,
    780,
    840,
    900,
    1200,
    1500,
    1800,
    2100,
    2400,
    2700,
    3000,
    3300,
    3600,
    4200,
    4800,
    5400,
    6000,
    6600,
    7200,
    7201,
]


def time_in_alarms_histogram(pg_write_cursor, hid, kind, name, Alarms):
    sql = """INSERT INTO aa_time_in_alarm_histograms
             (hid, param, alarm_type, bin_id, frequency, time_over_threshold)
             VALUES (%s,%s,%s,%s,%s,%s)"""

    alarm_durations = Alarms[:, 1] - Alarms[:, 0] - Alarms[:, 2]
    frequencies, bins = np.histogram(alarm_durations, bins=bin_defaults)

    for i in range(1, bins.shape[0]):
        if frequencies[i - 1] > 0:
            bin_time = np.sum(
                alarm_durations[
                    (alarm_durations >= bins[i - 1]) * (alarm_durations < bins[i])
                ]
            )
            pg_write_cursor.execute(
                sql,
                (
                    hid,
                    name,
                    kind,
                    int(bin_labels[i - 1]),
                    int(frequencies[i - 1]),
                    int(bin_time),
                ),
            )


def find_session_times(time_sync, param_times, update_rate):
    if time_sync.shape[1] < 1:
        return None

    times = {}

    times["TIME_SYNC"] = {"active": 0, "dropout": 0}

    reset_idx = np.r_[True, np.diff(time_sync[:, 0]) < 0]
    epochs = np.cumsum(reset_idx) - 1
    reset_times = time_sync[reset_idx, 1]
    boundries = np.r_[reset_times, time_sync[-1, 1]]

    for i in range(1, boundries.shape[0]):
        j = i - 1
        tmp = time_sync[epochs == j, :]

        do = np.diff(tmp[:, 1]) > update_rate
        do_chg = np.r_[do, False] * 1 + np.r_[False, do] * -1
        dot = np.c_[tmp[do_chg == 1, 1] + update_rate, tmp[do_chg == -1, 1]]

        try:
            dropout_times = np.vstack((dropout_times, dot))
        except NameError:
            dropout_times = dot.copy()

        times["TIME_SYNC"]["active"] += np.sum(tmp[-1, 1] - tmp[0, 1])
        times["TIME_SYNC"]["dropout"] += np.sum(dot[:, 1] - dot[:, 0])

    times["TIME_SYNC"]["on-network"] = (
        times["TIME_SYNC"]["active"] - times["TIME_SYNC"]["dropout"]
    )
    if times["TIME_SYNC"]["active"] > 0:
        times["TIME_SYNC"]["on-network-percentage"] = (
            100.0 * times["TIME_SYNC"]["on-network"]
        ) / times["TIME_SYNC"]["active"]
    else:
        times["TIME_SYNC"]["on-network-percentage"] = -1

    times.update(param_times)

    on_networks = []
    actives = []
    for k in "TIME_SYNC", "PR", "SPO2":
        if k in times.keys():
            on_networks.append(times[k]["on-network"])
            actives.append(times[k]["active"])

    times["WT"] = {}
    times["WT"]["on-network"] = max(on_networks) if len(on_networks) > 0 else -1
    times["WT"]["active"] = max(actives) if len(actives) > 0 else -1

    on_networks = []
    actives = []
    for k in "TEMP", "HR", "RR":
        if k in times.keys():
            on_networks.append(times[k]["on-network"])
            actives.append(times[k]["active"])

    times["CABLE"] = {}
    times["CABLE"]["on-network"] = max(on_networks) if len(on_networks) > 0 else -1
    times["CABLE"]["active"] = max(actives) if len(actives) > 0 else -1

    times["SESSION"] = {}
    times["SESSION"]["on-network"] = max(
        times["WT"]["on-network"], times["CABLE"]["on-network"]
    )
    times["SESSION"]["active"] = max(times["WT"]["active"], times["CABLE"]["active"])

    try:
        dropout_times
    except NameError:
        dropout_times = None

    return times, dropout_times


def find_parameter_times(param, update_rate=5, slop=0.1):
    if param.shape[1] < 1:
        return None, None, None, None

    time = {"active": 0, "xx": 0, "dropout": 0}
    reset_idx = np.r_[True, np.diff(param[:, 0]) < 0]
    epochs = np.cumsum(reset_idx) - 1
    reset_times = param[reset_idx, 1]
    boundries = np.r_[reset_times, param[-1, 1]]

    for i in range(1, boundries.shape[0]):
        j = i - 1
        t0 = boundries[i - 1]
        t1 = boundries[i]

        tmp = param[epochs == j, :]

        do = np.diff(tmp[:, 1]) > (update_rate + slop)
        do_chg = np.r_[do, False] * 1 + np.r_[False, do] * -1
        dot = np.c_[tmp[do_chg == 1, 1] + update_rate, tmp[do_chg == -1, 1]]

        xx_idx = (tmp[:, 2] < 0) * (tmp[:, 2] > -4)
        xx_chg = np.diff(np.r_[False, xx_idx] * 1)

        if xx_idx[-1]:
            xx_chg[-1] = 0 if xx_chg[-1] == 1 else -1
        xxt = np.c_[tmp[xx_chg == 1, 1], tmp[xx_chg == -1, 1]]
        xxt = np.c_[xxt, np.zeros(xxt.shape[0])]

        for i, (t0, t1, foo) in enumerate(xxt):
            idx = (dot[:, 0] >= t0) * (dot[:, 1] <= t1)
            cnt = np.sum(idx)
            if cnt > 0:
                # dropout time inside xx period
                xxt[i, 2] = np.sum((d1 - d0 for d0, d1 in dot[idx, :]))

        try:
            dropout_times = np.vstack((dropout_times, dot))
        except NameError:
            dropout_times = dot.copy()

        try:
            xx_times = np.vstack((xx_times, xxt))
        except NameError:
            xx_times = xxt.copy()

        time["active"] += np.sum(tmp[-1, 1] - tmp[0, 1])
        time["xx"] += np.sum(xxt[:, 1] - xxt[:, 0])
        time["dropout"] += np.sum(dot[:, 1] - dot[:, 0])

    try:
        xx_times
    except NameError:
        xx_times = None

    try:
        dropout_times
    except NameError:
        dropout_times = None

    time["on-network"] = time["active"] - time["dropout"]
    if time["active"] > 0:
        time["on-network-percentage"] = (100.0 * time["on-network"]) / time["active"]
    else:
        time["on-network-percentage"] = -1

    if time["on-network"] > 0:
        time["display-percentage"] = (100.0 * (time["on-network"] - time["xx"])) / time[
            "on-network"
        ]
    else:
        time["display-percentage"] = -1

    return time, xx_times, reset_times, dropout_times


def revise_spo2_xx_codes(data):
    SPO2_ = data["SPO2"].copy()
    if "ALARMS" in data.keys():
        ua = data["ALARMS"][np.r_[True, np.diff(data["ALARMS"][:, -1]) != 0], :]
        spo2_ok = SPO2_[:, 2] > 0
        spo2_ok = spo2_ok.astype(int)
        delta_spo2_ok = np.diff(np.r_[False, spo2_ok])
        spo2_on = delta_spo2_ok == 1
        off = [[t, 0] for t in ua[ua[:, 2] == 33, 1]]
        on = [[t, 1] for t in SPO2_[spo2_on, 1]]
        if len(on) > 0 and len(off) > 0:
            offon = np.vstack((off, on))
            offon = offon[np.argsort(offon[:, 0]), :]
            i = np.diff(np.r_[1, offon[:, 1]])
            left = offon[i == -1, 0]
            right = offon[i == 1, 0]
            if len(left) - 1 == len(right):
                right = np.r_[right, SPO2_[-1, 1]]
            regions = np.c_[left, right]
            for t0, t1 in regions:
                r = (
                    (SPO2_[:, 1] >= t0)
                    * (SPO2_[:, 1] < t1)
                    * (SPO2_[:, 2] < 0)
                    * (SPO2_[:, 2] > -4)
                )
                SPO2_[r, 2] = -10
    return SPO2_


def find_spo2_times(param, update_rate=3, slop=0.1):
    if param.shape[1] < 1:
        return None, None, None, None

    time = {
        "active": 0,
        "xx": 0,
        "dropout": 0,
        "xx_sensor_off": 0,
        "xx_sensor_on": 0,
        "display-percentage": -1,
        "display-percentage-sensor-on": -1,
        "sensor-off-percentage": -1,
        "on-network-percentage": -1,
    }

    reset_idx = np.r_[True, np.diff(param[:, 0]) < 0]
    epochs = np.cumsum(reset_idx) - 1
    reset_times = param[reset_idx, 1]
    boundries = np.r_[reset_times, param[-1, 1]]

    for i in range(1, boundries.shape[0]):
        j = i - 1
        t0 = boundries[i - 1]
        t1 = boundries[i]

        tmp = param[epochs == j, :]

        do = np.diff(tmp[:, 1]) > (update_rate + slop)
        do_chg = np.r_[do, False] * 1 + np.r_[False, do] * -1
        dot = np.c_[tmp[do_chg == 1, 1] + update_rate, tmp[do_chg == -1, 1]]

        xx_idx = ((tmp[:, 2] < 0) * (tmp[:, 2] > -4)) + (tmp[:, 2] == -10)
        xx_chg = np.diff(np.r_[False, xx_idx] * 1)

        if xx_idx[-1]:
            xx_chg[-1] = 0 if xx_chg[-1] == 1 else -1
        xxt = np.c_[tmp[xx_chg == 1, 1], tmp[xx_chg == -1, 1]]
        xxt = np.c_[xxt, np.zeros(xxt.shape[0])]

        for i, (t0, t1, foo) in enumerate(xxt):
            idx = (dot[:, 0] >= t0) * (dot[:, 1] <= t1)
            cnt = np.sum(idx)
            if cnt > 0:
                # dropout time inside xx period
                xxt[i, 2] = np.sum((d1 - d0 for d0, d1 in dot[idx, :]))

        off_idx = tmp[:, 2] == -10
        off_chg = np.diff(np.r_[False, off_idx] * 1)

        if off_idx[-1]:
            off_chg[-1] = 0 if off_chg[-1] == 1 else -1
        offt = np.c_[tmp[off_chg == 1, 1], tmp[off_chg == -1, 1]]
        offt = np.c_[offt, np.zeros(offt.shape[0])]

        for i, (t0, t1, foo) in enumerate(offt):
            idx = (dot[:, 0] >= t0) * (dot[:, 1] <= t1)
            cnt = np.sum(idx)
            if cnt > 0:
                # dropout time inside xx period
                offt[i, 2] = np.sum((d1 - d0 for d0, d1 in dot[idx, :]))

        try:
            dropout_times = np.vstack((dropout_times, dot))
        except NameError:
            dropout_times = dot.copy()

        try:
            xx_times = np.vstack((xx_times, xxt))
        except NameError:
            xx_times = xxt.copy()

        try:
            off_times = np.vstack((off_times, offt))
        except NameError:
            off_times = offt.copy()

        time["active"] += np.sum(tmp[-1, 1] - tmp[0, 1])
        time_xx = np.sum(xxt[:, 1] - xxt[:, 0])
        time["xx"] += time_xx
        time_xx_sensor_off = np.sum(offt[:, 1] - offt[:, 0])
        time["xx_sensor_off"] += time_xx_sensor_off
        time["xx_sensor_on"] += time_xx - time_xx_sensor_off
        time["dropout"] += np.sum(dot[:, 1] - dot[:, 0])

    try:
        xx_times
    except NameError:
        xx_times = None

    try:
        dropout_times
    except NameError:
        dropout_times = None

    try:
        off_times
    except NameError:
        off_times = None

    time["on-network"] = time["active"] - time["dropout"]

    if time["active"] > 0:
        time["on-network-percentage"] = (100.0 * time["on-network"]) / time["active"]

    if time["on-network"] > 0:
        time["display-percentage"] = (100.0 * (time["on-network"] - time["xx"])) / time[
            "on-network"
        ]
        time["display-percentage-sensor-on"] = (
            100.0 * (time["on-network"] - time["xx_sensor_on"])
        ) / time["on-network"]
        time["sensor-off-percentage"] = (100.0 * time["xx_sensor_off"]) / time[
            "on-network"
        ]

    return time, xx_times, reset_times, dropout_times, off_times


def time_in_xx_histogram(pg_write_cursor, hid, name, xx_times):

    sql = """INSERT INTO aa_time_in_xx_histograms
             (hid, param, bin_id, frequency, time_in_xx, dropout_time)
             VALUES (%s,%s,%s,%s,%s,%s)"""

    if xx_times.shape[0] > 0:
        xx_durations = xx_times[:, 1] - xx_times[:, 0]
        frequencies, bins = np.histogram(xx_durations, bins=bin_defaults)

        for i in range(1, bins.shape[0]):
            if frequencies[i - 1] > 0:
                idx = (xx_durations >= bins[i - 1]) * (xx_durations < bins[i])
                bin_time = np.sum(xx_durations[idx])
                drop_time = np.sum(xx_times[idx, 2])
                pg_write_cursor.execute(
                    sql,
                    (
                        hid,
                        name,
                        int(bin_labels[i - 1]),
                        int(frequencies[i - 1]),
                        int(bin_time),
                        int(drop_time),
                    ),
                )


def time_in_dropout_histogram(pg_write_cursor, hid, name, dropout_times):

    sql = """INSERT INTO aa_time_in_dropout_histograms
             (hid, param, bin_id, frequency, time_in_dropout)
             VALUES (%s,%s,%s,%s,%s)"""

    if dropout_times.shape[0] > 0:
        dropout_durations = dropout_times[:, 1] - dropout_times[:, 0]
        frequencies, bins = np.histogram(dropout_durations, bins=bin_defaults)
        for i in range(1, bins.shape[0]):
            if frequencies[i - 1] > 0:
                bin_time = np.sum(
                    dropout_durations[
                        (dropout_durations >= bins[i - 1])
                        * (dropout_durations < bins[i])
                    ]
                )
                pg_write_cursor.execute(
                    sql,
                    (
                        hid,
                        name,
                        int(bin_labels[i - 1]),
                        int(frequencies[i - 1]),
                        int(bin_time),
                    ),
                )


def find_alarms(
    pg_write_cursor, hid, kind, name, param, limit_range, delay_range, save_list=()
):

    sql = """INSERT INTO aa_alarms
             (hid, param, alarm_type, threshold, delay, alarms, time_over_threshold)
             VALUES (%s,%s,%s,%s,%s,%s,%s)"""

    Alarms = {}
    for limit in range(limit_range[0], limit_range[1] + limit_range[2], limit_range[2]):
        a = process_aa_limit_fcn(kind, param, limit)
        if limit in save_list:
            Alarms[limit] = None if a is None else a.copy()
        for delay in range(
            delay_range[0], delay_range[1] + delay_range[2], delay_range[2]
        ):
            num_alarms, time_over_threshold = process_aa_delay_fcn(a, delay)
            if num_alarms > 0:
                pg_write_cursor.execute(
                    sql,
                    (hid, name, kind, limit, delay, num_alarms, time_over_threshold),
                )
    return Alarms


def process_aa_limit_fcn(kind, param, ALARM_LIMIT):
    """This is the 'Brains' that runs the Alarms Algorithm over the data."""

    XX_Time = CHECK_ALARM = Alarm_Bucket = NORM = 0
    LEAKRATE = 30  # seconds/second
    ALARMING = 1
    XX = -1
    ConditionArray = Alarms = None

    # Find State Changes
    if kind == "HIGH":
        alarm_idxs = (param[:, 2] >= ALARM_LIMIT) + (param[:, 2] == -4)
        num_alarm_idxs = np.sum(alarm_idxs)
        if num_alarm_idxs > 0:
            norm_idxs = ((param[:, 2] < ALARM_LIMIT) * (param[:, 2] >= 0)) + (
                param[:, 2] == -5
            )
            xx_idxs = (param[:, 2] == -1) + (param[:, 2] == -2) + (param[:, 2] == -3)
        else:
            return Alarms

    elif kind == "LOW":
        alarm_idxs = ((param[:, 2] <= ALARM_LIMIT) * (param[:, 2] >= 0)) + (
            param[:, 2] == -5
        )
        num_alarm_idxs = np.sum(alarm_idxs)
        if num_alarm_idxs > 0:
            norm_idxs = param[:, 2] > ALARM_LIMIT
            xx_idxs = (
                (param[:, 2] == -1) + (param[:, 2] == -2.0) + (param[:, 2] == -3.0)
            )
        else:
            return Alarms

    # Create State Change Array
    if num_alarm_idxs > 0:
        alarmConditionIdx = np.nonzero(alarm_idxs)[0]
        alarm_first_start_idx = alarmConditionIdx[0]
        alarm_start_idx = np.r_[
            alarm_first_start_idx,
            alarmConditionIdx[np.r_[False, np.diff(alarmConditionIdx) > 1]],
        ]
        alarm_start_idx = np.c_[alarm_start_idx, np.ones(alarm_start_idx.shape[0])]

        try:
            ConditionArray = np.vstack((ConditionArray, alarm_start_idx))
        except ValueError:
            ConditionArray = alarm_start_idx

    if np.sum(norm_idxs) > 0:
        normConditionIdx = np.nonzero(norm_idxs)[0]
        norm_first_start_idx = normConditionIdx[0]
        norm_start_idx = np.r_[
            norm_first_start_idx,
            normConditionIdx[np.r_[False, np.diff(normConditionIdx) > 1]],
        ]
        norm_start_idx = np.c_[norm_start_idx, np.zeros(norm_start_idx.shape[0])]

        try:
            ConditionArray = np.vstack((ConditionArray, norm_start_idx))
        except ValueError:
            ConditionArray = norm_start_idx

    if np.sum(xx_idxs) > 0:
        xxConditionIdx = np.nonzero(xx_idxs)[0]
        xx_first_start_idx = xxConditionIdx[0]
        xx_start_idx = np.r_[
            xx_first_start_idx,
            xxConditionIdx[np.r_[False, np.diff(xxConditionIdx) > 1]],
        ]
        xx_start_idx = np.c_[xx_start_idx, np.ones(xx_start_idx.shape[0]) * -1]
        try:
            ConditionArray = np.vstack((ConditionArray, xx_start_idx))
        except ValueError:
            ConditionArray = xx_start_idx

    add_end_idx = [param.shape[0] - 1, 0]
    ConditionArray = np.vstack((ConditionArray, add_end_idx))
    ConditionArray = ConditionArray[
        np.argsort(ConditionArray[:, 0]),
    ]

    # Initial Condition
    i = 0
    if np.size(ConditionArray) > 0:
        if ConditionArray[0, 1] == 1:
            StartAlrm = param[int(ConditionArray[i, 0]), 1]
            NUMERIC_CONDITION = ALARMING

        elif ConditionArray[0, 1] == 0:
            StartNorm = param[int(ConditionArray[i, 0]), 1]
            NUMERIC_CONDITION = NORM

        elif ConditionArray[0, 1] == -1:
            StartXX = param[int(ConditionArray[i, 0]), 1]
            NUMERIC_CONDITION = XX

        else:
            # Could not determine Numeric Type!
            return Alarms

    # Run Through data
    for i in range(1, ConditionArray.shape[0]):
        _switch_val = NUMERIC_CONDITION
        if False:  # switch
            pass
        elif _switch_val == ALARMING:
            if i == ConditionArray.shape[0] - 1:  # End Condition
                StopAlrm = param[int(ConditionArray[i, 0]), 1]
                CHECK_ALARM = 1

            elif ConditionArray[i, 1] == -1:
                StopAlrm = StartXX = param[int(ConditionArray[i, 0]), 1]
                NUMERIC_CONDITION = XX

            elif ConditionArray[i, 1] == 0:
                StopAlrm = StartNorm = param[int(ConditionArray[i, 0]), 1]
                NUMERIC_CONDITION = NORM
                CHECK_ALARM = 1

        elif _switch_val == XX:
            if i == ConditionArray.shape[0] - 1:  # End Condition
                StopXX = param[int(ConditionArray[i, 0]), 1]
                XX_Time = XX_Time + StopXX - StartXX
                if ConditionArray[int((i - 2)), 1] == 1:
                    StopAlrm = StopXX
                    CHECK_ALARM = 1

            elif ConditionArray[i, 1] == 0:
                StopXX = StartNorm = param[int(ConditionArray[i, 0]), 1]
                NUMERIC_CONDITION = NORM
                XX_Time = XX_Time + StopXX - StartXX
                if i > 1:
                    if ConditionArray[i - 2, 1] == 1:
                        StopAlrm = StopXX
                        CHECK_ALARM = 1

            elif ConditionArray[i, 1] == 1:
                StopXX = param[int(ConditionArray[i, 0]), 1]
                NUMERIC_CONDITION = ALARMING
                if i > 1:
                    if ConditionArray[i - 2, 1] == 1:
                        XX_Time = XX_Time + StopXX - StartXX
                    else:
                        StartAlrm = param[int(ConditionArray[i, 0]), 1]
                        XX_Time = 0

                else:
                    StartAlrm = param[int(ConditionArray[i, 0]), 1]
                    XX_Time = 0

        elif _switch_val == NORM:
            if i == ConditionArray.shape[0] - 1:  # End Condition
                StopNorm = param[int(ConditionArray[i, 0]), 1]

            elif ConditionArray[i, 1] == -1:
                StopNorm = param[int(ConditionArray[i, 0]), 1]
                # Leak the bucket
                Alarm_Bucket = Alarm_Bucket - np.dot(StopNorm - StartNorm, LEAKRATE)
                if Alarm_Bucket < 0:
                    Alarm_Bucket = 0

                StartXX = param[int(ConditionArray[i, 0]), 1]
                NUMERIC_CONDITION = XX

            elif ConditionArray[i, 1] == 1:
                StopNorm = param[int(ConditionArray[i, 0]), 1]
                # Leak the bucket
                Alarm_Bucket = Alarm_Bucket - np.dot(StopNorm - StartNorm, LEAKRATE)
                if Alarm_Bucket < 0:
                    Alarm_Bucket = 0

                StartAlrm = param[int(ConditionArray[i, 0]), 1]
                NUMERIC_CONDITION = ALARMING

            XX_Time = 0

        if CHECK_ALARM == 1:
            # Check if Alarm Occurred (account for Delay & Buckets)
            Alarm_Time = StopAlrm - StartAlrm - XX_Time
            # Time in alarm condition
            Alarm_Bucket = Alarm_Bucket + Alarm_Time
            if Alarm_Bucket >= 0:
                NewAlarm = np.array([[StartAlrm, StopAlrm, XX_Time, Alarm_Bucket]])
                try:
                    Alarms = np.vstack((Alarms, NewAlarm))
                except ValueError:
                    Alarms = NewAlarm

        CHECK_ALARM = 0

    return Alarms


def process_aa_delay_fcn(Alarms, delay):
    if Alarms is None:
        num_alarms = 0
        time_over_threshold = 0
    else:
        idx = Alarms[:, 3] >= delay
        num_alarms = int(np.sum(idx))
        time_over_threshold = int(
            np.sum(Alarms[idx, 1] - Alarms[idx, 0] - Alarms[idx, 2])
        )
    return num_alarms, time_over_threshold


def patient_alarms(pgsql_, hid, site, data, cr_data):
    """
    Run Alarms Analysis over Vital Signs
    """

    # Cardiac rate (CR)
    CR_PARAMS = {"HR": {}, "PR": {}}
    with pgsql_.cursor(cursor_factory=DictCursor) as read_cursor:
        read_cursor.execute(
            "SELECT * from aa_site_defaults where param='HR' AND code='Sotera'"
        )
        for row in read_cursor:
            CR_PARAMS["HR"][row["alarm_type"]] = {
                "Sotera": (row["threshold"], row["delay"])
            }

        read_cursor.execute(
            "SELECT * from aa_site_defaults where param='PR' AND code='Sotera'"
        )
        for row in read_cursor:
            CR_PARAMS["PR"][row["alarm_type"]] = {
                "Sotera": (row["threshold"], row["delay"])
            }

        read_cursor.execute(
            "SELECT * from aa_site_defaults where param='HR' AND code='{}'".format(site)
        )
        for row in read_cursor:
            CR_PARAMS["HR"][row["alarm_type"]]["Site"] = (
                row["threshold"],
                row["delay"],
            )

        read_cursor.execute(
            "SELECT * from aa_site_defaults where param='PR' AND code='{}'".format(site)
        )
        for row in read_cursor:
            CR_PARAMS["PR"][row["alarm_type"]]["Site"] = (
                row["threshold"],
                row["delay"],
            )

    delay_range = (LDR["DELAY"]["MIN"], LDR["DELAY"]["MAX"], LDR["DELAY"]["INC"])

    alarms = {"HR": {}, "PR": {}}
    for param in AA_PARAMS.keys():  # PARAMS
        if param in data.keys():
            for kind in AA_PARAMS[param]:  # HIGH/LOW
                limit_range = (
                    LDR[param][kind]["MIN"],
                    LDR[param][kind]["MAX"],
                    LDR[param][kind]["INC"],
                )
                with pgsql_, pgsql_.cursor() as pg_write_cursor:
                    if param in ("HR", "PR"):
                        a = find_alarms(
                            pg_write_cursor,
                            hid,
                            kind,
                            param,
                            data[param],
                            limit_range,
                            delay_range,
                            (
                                CR_PARAMS[param][kind]["Sotera"][0],
                                CR_PARAMS[param][kind]["Site"][0],
                            ),
                        )
                        if param == "HR":
                            a = find_alarms(
                                pg_write_cursor,
                                hid,
                                kind,
                                "HR_A",
                                data["HR_A"],
                                limit_range,
                                delay_range,
                            )
                    else:
                        a = find_alarms(
                            pg_write_cursor,
                            hid,
                            kind,
                            param,
                            data[param],
                            limit_range,
                            delay_range,
                        )
                if param in ("HR", "PR"):
                    alarms[param][kind] = a

    sql = """INSERT
               INTO aa_alarms
                    (hid, param, alarm_type, threshold,
                     delay, delay_hr, alarms, time_over_threshold)
             VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"""

    seen = {}
    for kind in ["HIGH", "LOW"]:
        for s in ["Sotera", "Site"]:

            hr_limit, hr_delay = CR_PARAMS["HR"][kind][s]
            pr_limit, pr_delay = CR_PARAMS["PR"][kind][s]

            if (hr_delay, pr_limit, pr_delay) in seen.keys():
                continue

            seen[(hr_delay, pr_limit, pr_delay)] = True

            hr_alarms = None
            if kind not in alarms["HR"].keys():
                alarms["HR"][kind] = {}

            if pr_limit not in alarms["HR"][kind].keys():
                alarms["HR"][kind][pr_limit] = None

            if alarms["HR"][kind][pr_limit] is not None:
                idx = alarms["HR"][kind][pr_limit][:, 3] > hr_delay
                if np.sum(idx) > 0:
                    hr_alarms = alarms["HR"][kind][pr_limit][idx, :]

            pr_alarms = None

            if kind not in alarms["PR"].keys():
                alarms["PR"][kind] = {}

            if pr_limit not in alarms["PR"][kind].keys():
                alarms["PR"][kind][pr_limit] = None

            if alarms["PR"][kind][pr_limit] is not None:
                idx = alarms["PR"][kind][pr_limit][:, 3] > pr_delay
                if np.sum(idx) > 0:
                    pr_alarms = alarms["PR"][kind][pr_limit][idx, :]

            cr_alarms = find_cardiac_alarms(
                cr_data["times"], hr_alarms, pr_alarms, cr_data["L_PR_XX"]
            )
            if cr_alarms is not None:
                num_alarms = cr_alarms.shape[0]
                time_over_threshold = int(np.sum(cr_alarms[:, 1] - cr_alarms[:, 0]))
                with pgsql_, pgsql_.cursor() as pg_write_cursor:
                    pg_write_cursor.execute(
                        sql,
                        (
                            hid,
                            "CR",
                            kind,
                            hr_limit,
                            pr_delay,
                            hr_delay,
                            num_alarms,
                            time_over_threshold,
                        ),
                    )
    return alarms


def run_itemfreq(param, MINIM, MAXIM):
    Hist = None
    if param.shape[0] > 0 and param.shape[1] > 1:
        tmp = param[param[:, 2] > -1, 2].astype(int)
        if tmp.shape[0] > 0:
            Hist = np.bincount(tmp)
            Hist = np.c_[np.arange(Hist.shape[0]), Hist]
            Hist = Hist[Hist[:, 1] > 0, :]
            Hist = Hist[(Hist[:, 0] >= MINIM) * (Hist[:, 0] <= MAXIM), :]
            if Hist.shape[0] == 0:
                Hist = None
    return Hist


def patient_numeric_histograms(pgsql_, hid, data):

    sql = """INSERT INTO aa_numeric_histograms
             (hid, param, bin, frequency)
             VALUES (%s,%s,%s,%s)"""

    hist_limits = {
        "SPO2": (49.0, 100.0),
        "PR": (0.0, 240.0),
        "HR": (0.0, 240.0),
        "RR": (0.0, 50.0),
        "CNIBP_SYS": (60.0, 240.0),
        "CNIBP_DIA": (40.0, 160.0),
        "CNIBP_MAP": (50.0, 185.0),
        "BP_SYS": (60.0, 240.0),
        "BP_DIA": (40.0, 160.0),
        "BP_MAP": (50.0, 185.0),
    }
    Hist = {}

    for param in hist_limits.keys():
        with pgsql_, pgsql_.cursor() as pg_write_cursor:
            if param in data.keys():
                Hist[param] = run_itemfreq(
                    data[param], hist_limits[param][0], hist_limits[param][1]
                )
            else:
                Hist[param] = None
            if Hist[param] is not None:
                for row in Hist[param]:
                    pg_write_cursor.execute(sql, (hid, param, int(row[0]), int(row[1])))


def patient_time_in_histograms(pgsql_, hid, data, post_combo=False):

    with pgsql_.cursor(cursor_factory=DictCursor) as read_cursor:
        read_cursor.execute("SELECT * FROM aa_site_defaults WHERE code='Sotera'")
        param_seen = {}
        cr_data = {
            "PR_xx_times": None,
            "PR_dropout_times": None,
            "HR_xx_times": None,
            "HR_dropout_times": None,
        }

        if post_combo:
            update_rates = update_rates_combo
        else:
            update_rates = update_rates_pre_combo

        param_times = {}
        for name in update_rates.keys():
            param_times[name] = {
                "active": 0,
                "xx": 0,
                "dropout": 0,
                "on-network": 0,
                "on-network-percentage": -1,
                "display-percentage": -1,
            }
        param_times["BP_MAP"] = {
            "active": 0,
            "xx": 0,
            "dropout": 0,
            "on-network": 0,
            "on-network-percentage": -1,
            "display-percentage": -1,
        }
        param_times["SPO2"] = {
            "active": 0,
            "xx": 0,
            "dropout": 0,
            "on-network": 0,
            "on-network-percentage": -1,
            "display-percentage": -1,
            "xx_sensor_off": 0,
            "xx_sensor_on": 0,
            "display-percentage-sensor-on": -1,
            "sensor-off-percentage": -1,
        }

        for row in read_cursor:
            name = row["param"]
            kind = row["alarm_type"]
            ALARM_LIMIT = abs(row["threshold"])
            if name in data.keys():
                if data[name].shape[0] > 0 and data[name].shape[1] > 1:
                    if name[:2] == "BP":
                        param_times[name] = {}
                        param_times[name]["dropout"] = 0
                        param_times[name]["active"] = data[name].shape[0]
                        param_times[name]["on-network"] = data[name].shape[0]
                        param_times[name]["on-network-percentage"] = -1
                        param_times[name]["xx"] = float(
                            np.sum((data[name][:, 2] < 0) * (data[name][:, 2] > -4))
                        )
                        param_times[name]["display-percentage"] = float(
                            (
                                100.0
                                * (
                                    param_times[name]["on-network"]
                                    - param_times[name]["xx"]
                                )
                            )
                            / param_times[name]["on-network"]
                        )
                    else:
                        if name == "HR":
                            names = ["HR", "HR_A"]
                        else:
                            names = [name]
                        for name in names:
                            if name in data.keys():
                                Alarms = process_aa_limit_fcn(
                                    kind, data[name], ALARM_LIMIT
                                )
                                with pgsql_, pgsql_.cursor() as pg_write_cursor:
                                    if Alarms is not None:
                                        time_in_alarms_histogram(
                                            pg_write_cursor, hid, kind, name, Alarms
                                        )
                                    if name in param_seen.keys():
                                        pass
                                    else:
                                        if name == "SPO2":
                                            SPO2_ = revise_spo2_xx_codes(data)
                                            (
                                                param_times[name],
                                                xx_times,
                                                reset_times,
                                                dropout_times,
                                                off_times,
                                            ) = find_spo2_times(
                                                SPO2_, update_rates[name]
                                            )
                                        else:
                                            (
                                                param_times[name],
                                                xx_times,
                                                reset_times,
                                                dropout_times,
                                            ) = find_parameter_times(
                                                data[name], update_rates[name]
                                            )
                                        if xx_times is not None:
                                            time_in_xx_histogram(
                                                pg_write_cursor, hid, name, xx_times
                                            )
                                        if dropout_times is not None:
                                            time_in_dropout_histogram(
                                                pg_write_cursor,
                                                hid,
                                                name,
                                                dropout_times,
                                            )
                                        param_seen[name] = True
                                        if name == "HR" or name == "PR":
                                            if xx_times is not None:
                                                cr_data[
                                                    "{}_xx_times".format(name)
                                                ] = xx_times.copy()
                                            if dropout_times is not None:
                                                cr_data[
                                                    "{}_dropout_times".format(name)
                                                ] = dropout_times.copy()

    param_times["CR"] = {}
    # simulate Cardiac Rate
    tuple_ = simulate_cr_structs(
        data,
        cr_data["PR_xx_times"],
        cr_data["PR_dropout_times"],
        cr_data["HR_xx_times"],
        cr_data["HR_dropout_times"],
    )

    cr_data["times"] = tuple_[0]
    xx_times = tuple_[1]
    dropout_times = tuple_[2]
    cr_data["L_PR_XX"] = tuple_[3]
    cr_data["L_HR_XX"] = tuple_[4]

    if xx_times is not None:
        param_times["CR"]["xx"] = float(np.sum(xx_times[:, 1] - xx_times[:, 0]))
    else:
        param_times["CR"]["xx"] = 0
    if dropout_times is not None:
        param_times["CR"]["dropout"] = float(
            np.sum(dropout_times[:, 1] - dropout_times[:, 0])
        )
    else:
        param_times["CR"]["dropout"] = 0

    param_times["CR"]["active"] = float(cr_data["times"][-1] - cr_data["times"][0])
    param_times["CR"]["on-network"] = (
        param_times["CR"]["active"] - param_times["CR"]["dropout"]
    )

    if param_times["CR"]["on-network"] > 0:
        param_times["CR"]["display-percentage"] = float(
            (100.0 * (param_times["CR"]["on-network"] - param_times["CR"]["xx"]))
            / param_times["CR"]["on-network"]
        )
    else:
        param_times["CR"]["display-percentage"] = -1

    with pgsql_, pgsql_.cursor() as pg_write_cursor:
        if xx_times is not None:
            time_in_xx_histogram(pg_write_cursor, hid, "CR", xx_times)

        if dropout_times is not None:
            time_in_dropout_histogram(pg_write_cursor, hid, "CR", dropout_times)

        times, session_dropout_times = find_session_times(
            data["TIME_SYNC"], param_times, update_rates["TIME_SYNC"]
        )
        if session_dropout_times is not None:
            time_in_dropout_histogram(
                pg_write_cursor, hid, "ALL", session_dropout_times
            )

    return times, cr_data


def patient_update_session_data(pgsql_, session, times):
    """
    Insert per-session info into session_data table
    """

    sql = """ UPDATE aa_session_data SET
    site = '{site}',
    status = 'processed',
    software_version = '{software_version}',
    duration = {duration},
    time_wt = {time_WT},
    time_cable = {time_Cable},
    time_total = {time_Total},
    time_cnibp = {time_CNIBP},
    time_on_network = {time_On_Network},
    display_percent_spo2 = {display_percent_SPO2:.2f},
    display_percent_spo2_sensor_on = {display_percent_SPO2_sensor_on:.2f},
    sensor_off_percent_spo2 = {sensor_off_percent_SPO2:.2f},
    display_percent_pr = {display_percent_PR:.2f},
    display_percent_hr = {display_percent_HR:.2f},
    display_percent_rr = {display_percent_RR:.2f},
    display_percent_temp = {display_percent_TEMP:.2f},
    display_percent_cr = {display_percent_CR:.2f},
    display_percent_bp = {display_percent_BP:.2f},
    display_percent_cnibp = {display_percent_CNIBP:.2f},
    display_percent_hr_a = {display_percent_HR_A:.2f},
    time_in_xx_spo2 = {time_in_xx_SPO2},
    time_in_xx_spo2_sensor_off = {time_in_xx_SPO2_sensor_off},
    time_in_xx_pr = {time_in_xx_PR},
    time_in_xx_rr = {time_in_xx_RR},
    time_in_xx_hr = {time_in_xx_HR},
    time_in_xx_temp = {time_in_xx_TEMP},
    time_in_xx_cr = {time_in_xx_CR},
    count_xx_bp = {count_xx_BP},
    time_in_xx_cnibp = {time_in_xx_CNIBP},
    time_in_xx_hr_a = {time_in_xx_HR_A},
    time_spo2 = {time_SPO2},
    time_pr = {time_PR},
    time_hr = {time_HR},
    time_rr = {time_RR},
    time_cr = {time_CR},
    count_bp = {count_BP},
    time_cnibp2 = {time_CNIBP2},
    time_temp = {time_TEMP},
    time_hr_a = {time_HR_A}
    WHERE hid = {hid}
    """

    with pgsql_, pgsql_.cursor() as pg_write_cursor:
        pg_write_cursor.execute(
            sql.format(
                site=session["site"],
                software_version=session["sw_version"],
                duration=session["duration"],
                time_WT=int(np.round(times["WT"]["active"])),
                time_Cable=int(np.round(times["CABLE"]["active"])),
                time_Total=int(np.round(times["SESSION"]["active"])),
                time_CNIBP=int(np.round(times["CNIBP_MAP"]["active"])),
                time_On_Network=int(np.round(times["SESSION"]["on-network"])),
                display_percent_SPO2=times["SPO2"]["display-percentage"],
                display_percent_SPO2_sensor_on=times["SPO2"][
                    "display-percentage-sensor-on"
                ],
                sensor_off_percent_SPO2=times["SPO2"]["sensor-off-percentage"],
                display_percent_PR=times["PR"]["display-percentage"],
                display_percent_HR=times["HR"]["display-percentage"],
                display_percent_RR=times["RR"]["display-percentage"],
                display_percent_TEMP=times["TEMP"]["display-percentage"],
                display_percent_CR=times["CR"]["display-percentage"],
                display_percent_BP=times["BP_MAP"]["display-percentage"],
                display_percent_CNIBP=times["CNIBP_MAP"]["display-percentage"],
                display_percent_HR_A=times["HR_A"]["display-percentage"],
                time_in_xx_SPO2=int(np.round(times["SPO2"]["xx"])),
                time_in_xx_SPO2_sensor_off=int(
                    np.round(times["SPO2"]["xx_sensor_off"])
                ),
                time_in_xx_PR=int(np.round(times["PR"]["xx"])),
                time_in_xx_RR=int(np.round(times["RR"]["xx"])),
                time_in_xx_HR=int(np.round(times["HR"]["xx"])),
                time_in_xx_TEMP=int(np.round(times["TEMP"]["xx"])),
                time_in_xx_CR=int(np.round(times["CR"]["xx"])),
                count_xx_BP=int(np.round(times["BP_MAP"]["xx"])),
                time_in_xx_CNIBP=int(np.round(times["CNIBP_MAP"]["xx"])),
                time_in_xx_HR_A=int(np.round(times["HR_A"]["xx"])),
                time_SPO2=int(np.round(times["SPO2"]["active"])),
                time_PR=int(np.round(times["PR"]["active"])),
                time_HR=int(np.round(times["HR"]["active"])),
                time_RR=int(np.round(times["RR"]["active"])),
                time_CR=int(np.round(times["CR"]["active"])),
                count_BP=int(np.round(times["BP_MAP"]["active"])),
                time_CNIBP2=int(np.round(times["CNIBP_MAP"]["active"])),
                time_TEMP=int(np.round(times["TEMP"]["active"])),
                time_HR_A=int(np.round(times["HR_A"]["active"])),
                hid=session["hid"],
            )
        )


def simulate_cr_structs(
    data, pr_xx_times, pr_dropout_times, hr_xx_times, hr_dropout_times
):
    if "HR" in data.keys() and "PR" in data.keys():
        tmin = min(data["PR"][0, 1], data["HR"][0, 1])
        tmax = max(data["PR"][-1, 1], data["HR"][-1, 1])
    elif "PR" in data.keys():
        tmin = data["PR"][0, 1]
        tmax = data["PR"][-1, 1]
    elif "HR" in data.keys():
        tmin = data["HR"][0, 1]
        tmax = data["HR"][-1, 1]
    else:
        tmin = 0
        tmax = 0

    times = np.arange(tmin, tmax + 0.1, 0.1)

    l_pr_xx = np.zeros_like(times, dtype=bool)
    if pr_xx_times is not None:
        for t0, t1, tx in pr_xx_times:
            l_pr_xx[(times >= t0) * (times <= t1)] = True

    l_hr_xx = np.zeros_like(times, dtype=bool)
    if hr_xx_times is not None:
        for t0, t1, tx in hr_xx_times:
            l_hr_xx[(times >= t0) * (times <= t1)] = True

    if pr_xx_times is not None and hr_xx_times is not None:
        l_cr_xx = l_pr_xx * l_hr_xx
    elif pr_xx_times is not None:
        l_cr_xx = l_pr_xx
    elif hr_xx_times is not None:
        l_cr_xx = l_hr_xx
    else:
        l_cr_xx = np.zeros_like(times, dtype=bool)

    if l_cr_xx.shape[0] > 0:
        xx_chg = np.diff(np.r_[False, l_cr_xx] * 1)
        if l_cr_xx[-1]:
            xx_chg[-1] = 0 if xx_chg[-1] == 1 else -1
        cr_xx_times = np.c_[times[xx_chg == 1], times[xx_chg == -1]]
        cr_xx_times = np.c_[cr_xx_times, np.zeros(cr_xx_times.shape[0])]
    else:
        cr_xx_times = None
    l_pr_dropout = np.zeros_like(times, dtype=bool)
    if pr_dropout_times is not None:
        for t0, t1 in pr_dropout_times:
            l_pr_dropout[(times >= t0) * (times <= t1)] = True

    l_hr_dropout = np.zeros_like(times, dtype=bool)
    if hr_dropout_times is not None:
        for t0, t1 in hr_dropout_times:
            l_hr_dropout[(times >= t0) * (times <= t1)] = True

    l_cr_dropout = l_pr_dropout * l_hr_dropout

    dropout_chg = np.diff(np.r_[False, l_cr_dropout] * 1)
    if l_cr_dropout.shape[0] > 0 and l_cr_dropout[-1]:
        dropout_chg[-1] = 0 if dropout_chg[-1] == 1 else -1
    cr_dropout_times = np.c_[times[dropout_chg == 1], times[dropout_chg == -1]]

    return times, cr_xx_times, cr_dropout_times, l_pr_xx, l_hr_xx


def find_cardiac_alarms(times, hr_alarms, pr_alarms, l_pr_xx):

    l_hr_alarms = np.zeros_like(times, dtype=bool)
    if hr_alarms is not None:
        for t0, t1, tx, tb in hr_alarms:
            l_hr_alarms[(times >= t0) * (times <= t1)] = True

    l_pr_alarms = np.zeros_like(times, dtype=bool)
    if pr_alarms is not None:
        for t0, t1, tx, tb in pr_alarms:
            l_pr_alarms[(times >= t0) * (times <= t1)] = True

    l_cr_alarms = (l_pr_xx * l_hr_alarms + l_pr_alarms) * ~(
        l_pr_alarms * l_pr_xx * ~l_hr_alarms
    )

    if np.sum(l_cr_alarms) > 0:
        cr_chg = np.diff(np.r_[False, l_cr_alarms] * 1)
        if l_cr_alarms[-1]:
            cr_chg[-1] = 0 if cr_chg[-1] == 1 else -1
        cr_alarms = np.c_[times[cr_chg == 1], times[cr_chg == -1]]
    else:
        cr_alarms = None

    return cr_alarms
