from sotera.util.time import get_datetime_from_timestamp
from numpy import (
    sum as npsum,
    nonzero,
    unique,
    bincount,
    sort,
    histogram,
    arange,
    bitwise_and,
    diff,
    max as npmax,
    abs as npabs,
)


def nibp_points_only(BP):
    lastt = -1
    dict_ = {}
    for t, sy, di, mp, pr, ec in BP[:, (1, 2, 3, 4, -3, -2)]:
        key = (t, sy, di, mp, pr, ec)
        try:
            dict_[key] += 1
        except KeyError:
            dict_[key] = 1
        if dict_[key] > 1:
            continue
        if lastt < 0:
            deltat = -1
        else:
            deltat = t - lastt
        lastt = t
        yield (t, deltat, sy, di, mp, -1, -1, -1, -1, pr, ec)


def nibp_points_with_cnibp(BP, CNIBP):
    lastt = -1
    dict_ = {}
    for t, sy, di, mp, pr, ec in BP[:, (1, 2, 3, 4, -3, -2)]:
        key = (t, sy, di, mp, pr, ec)
        try:
            dict_[key] += 1
        except KeyError:
            dict_[key] = 1
        if dict_[key] > 1:
            continue
        i = (CNIBP[:, 1] >= t - 300) * (CNIBP[:, 1] < t)
        if sum(i) > 0:
            cnibp_t = CNIBP[i, :][-1, 1]
            cnibp_sys = CNIBP[i, :][-1, 2]
            cnibp_dia = CNIBP[i, :][-1, 3]
            cnibp_map = CNIBP[i, :][-1, 4]
        else:
            cnibp_t = -1
            cnibp_sys = -1
            cnibp_dia = -1
            cnibp_map = -1

        if lastt < 0:
            deltat = -1
        else:
            deltat = t - lastt
        lastt = t
        yield (t, deltat, sy, di, mp, cnibp_t, cnibp_sys, cnibp_dia, cnibp_map, pr, ec)


def cnibp_bland_altman_points(BP, CNIBP):
    for t, sy, di, mp in BP[:, (1, 2, 3, 4)]:
        if mp > 0:
            i = (CNIBP[:, 1] >= t - 300) * (CNIBP[:, 1] < t)
            if npsum(i) > 0:
                tmp = CNIBP[i, :]
                j = tmp[:, 4] > 0
                if npsum(j) > 0:
                    yield (
                        t,  # bp_time
                        sy,  # bp_sys
                        di,  # bp_dia
                        mp,  # bp_map
                        tmp[j, :][-1, 1],  # cnibp_time
                        tmp[j, :][-1, 2],  # cnibp_sys
                        tmp[j, :][-1, 3],  # cnibp_dia
                        tmp[j, :][-1, 4],  # cnibp_map
                        (tmp[j, :][-1, 2] + sy) / 2.0,  # mean_sys
                        (tmp[j, :][-1, 3] + di) / 2.0,  # mean_dia
                        (tmp[j, :][-1, 4] + mp) / 2.0,  # mean_map
                        tmp[j, :][-1, 2] - sy,  # diff_sys
                        tmp[j, :][-1, 3] - di,  # diff_dia
                        tmp[j, :][-1, 4] - mp,  # diff_map
                    )


def device_alarms_histogram(ALARMS):
    for a in nonzero(bincount(ALARMS[:, 2].astype(int)))[0]:
        yield a, unique(ALARMS[ALARMS[:, 2] == a, 5]).shape[0]


def device_alarms_info(ALARMS, tz):
    for sn in sort(unique(ALARMS[:, 5])):
        alarm_type = None
        for row in ALARMS[ALARMS[:, 5] == sn, :]:
            if (row[2], row[4]) != alarm_type:
                alarm_type = (row[2], row[4])
                dt = get_datetime_from_timestamp(row[1], tz)
                yield (
                    row[0],
                    dt,
                    row[2],
                    row[3],
                    row[4],
                    row[5],
                    dt.weekday(),
                )


def spo2_control_histogram(SPO2_CTRL):
    y = bincount(SPO2_CTRL[:, -1].astype(int))
    ii = nonzero(y)[0]
    for i, y in zip(ii, y[ii]):
        yield i, y


def ppg_beat_class_histogram(PPG_BEAT):
    y = bincount(PPG_BEAT[:, -1].astype(int))
    ii = nonzero(y)[0]
    for i, y in zip(ii, y[ii]):
        yield i, y


def ppg_pmi_histogram(PPG_BEAT):
    cnts, bins = histogram(PPG_BEAT[:, 11], [-1] + arange(0, 2010, 10))
    for i, y in zip(bins[:-1], cnts):
        yield i, y


ANGLEV_UPDATE_THRESH = 12  # degrees
ANGLEH_UPDATE_THRESH = 25  # degrees
ELBOW_HT_UPDATE_THRESH = 8  # cm
WRIST_HT_UPDATE_THRESH = 8  # cm
WINDOW = 15 * 60  # seconds


def calculate_calibration_times(
    TIME_SYNC, CNIBP_CAL, SENSOR_CONNECTION, BP, POSTURE_PKT, ARM_HT_PKT
):
    def getpos(position, thresh1, thresh2):
        if position.shape[0] < 1:
            return 0
        nibp_position1 = position[-1, 3]
        nibp_position2 = position[-1, 4]
        diff1 = npabs(position[:, 3] - nibp_position1)
        diff2 = npabs(position[:, 4] - nibp_position2)

        if npmax(diff1) < thresh1:
            last_position1 = 0
        else:
            j = position.shape[0]
            for i in range(j):
                if npabs(nibp_position1 - position[j - i - 1, 3]) >= thresh1:
                    last_position1 = position[j - i - 1, 1]
                    break

        if npmax(diff2) < thresh2:
            last_position2 = 0
        else:
            j = position.shape[0]
            for i in range(j):
                if npabs(nibp_position2 - position[j - i - 1, 4]) >= thresh2:
                    last_position2 = position[j - i - 1, 1]
                    break
        return max(last_position1, last_position2)

    ARM_HT = ARM_HT_PKT[ARM_HT_PKT[:, 3] < 16000, :]
    POSTURE = POSTURE_PKT[POSTURE_PKT[:, 3] < 16000, :]

    # convert BP start inflation to unix time
    CAL = CNIBP_CAL.copy()
    CAL[:, 2] = CAL[:, 1] - (CAL[:, 0] - CAL[:, 2]) / 500.0

    # identify cuff connections
    idx = bitwise_and(SENSOR_CONNECTION[:, 2].astype("uint16"), 2) == 2
    cuff_connect = SENSOR_CONNECTION[idx, :]

    for i, (_, stop_nibp, start_nibp, *_) in enumerate(CAL.tolist()):

        # find all nibp_connect and bp
        lookback = start_nibp - WINDOW

        idx = (BP[:, 1] >= lookback) * (BP[:, 1] < start_nibp)
        last_bp_measure = BP[idx, 1][-1] if sum(idx) else 0

        idx = (cuff_connect[:, 1] >= lookback) * (cuff_connect[:, 1] < start_nibp)
        first_cuff_conn = cuff_connect[idx, 1][0] if sum(idx) else 0

        idx = diff(TIME_SYNC[:, 0]) < 0
        device_swaps = TIME_SYNC[1:, 1][idx]

        idx = (device_swaps > lookback) * (device_swaps < start_nibp)
        last_dev_swap = device_swaps[idx][0] if sum(idx) else 0

        idx = (POSTURE[:, 1] >= lookback) * (POSTURE[:, 1] < start_nibp)
        last_posture = getpos(
            POSTURE[idx, :], ANGLEV_UPDATE_THRESH, ANGLEH_UPDATE_THRESH
        )

        idx = (ARM_HT[:, 1] >= lookback) * (ARM_HT[:, 1] < start_nibp)
        last_arm_height = getpos(
            ARM_HT[idx, :], ELBOW_HT_UPDATE_THRESH, WRIST_HT_UPDATE_THRESH
        )

        start_cnibp = max(
            last_bp_measure,
            first_cuff_conn,
            last_posture,
            last_arm_height,
            last_dev_swap,
        )

        # returns:
        yield (
            i,
            start_nibp,  # start of nibp inflation
            start_nibp - start_cnibp if start_cnibp > 0 else -1,
            stop_nibp - start_nibp,  # nibp inflation time
        )
