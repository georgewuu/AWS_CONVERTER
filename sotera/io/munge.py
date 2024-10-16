import numpy as np
import scipy.interpolate
import bisect

PPG_SAT_LOW = 25000000
PPG_SAT_HIGH = 645000000


def preprocess_ppg(
    data,
    WAVEFORMS=("IR_FILT", "RED_FILT", "AMBIENT", "IR_AC", "IR_DC", "RED_AC", "RED_DC"),
    upsample=False,
):
    """Package PPG waveforms input to offline algorithms"""

    if "PPG" in data.keys():
        PPG = data["PPG"].copy()
    else:
        _data_ = {}
        if upsample or (data["IR_FILT"].shape[0] / data["IR_DC"].shape[0]) > 3.9:
            PPG = np.ones((data["IR_FILT"].shape[0], 9)) * np.nan
            x = data["IR_FILT"][:, 0]
            PPG[:, 0] = x
            f = scipy.interpolate.interp1d(
                data["IR_FILT"][:, 0], data["IR_FILT"][:, 1], bounds_error=False
            )
            PPG[:, 1] = f(x)
            PPG[:, 2] = data["IR_FILT"][:, 2]
            idx = ~np.isnan(PPG[:, 2])
            _data_["IR_FILT"] = data["IR_FILT"]
            _data_["RED_FILT"] = data["RED_FILT"]
            i = 3
            for k in ("AMBIENT", "IR_AC", "IR_DC", "RED_AC", "RED_DC"):
                f = scipy.interpolate.interp1d(
                    data[k][:, 0], data[k][:, 2], bounds_error=False
                )
                PPG[:, i] = f(x)
                _data_[k] = PPG[idx, :][:, (0, 1, i)]
                i += 1
            del PPG
        else:
            for k in WAVEFORMS:
                _data_[k] = data[k]

        s0 = None
        s1 = None
        wfms = []
        for k in WAVEFORMS:
            if k in _data_.keys():
                if s0 is None:
                    s0 = int(_data_[k][0, 0])
                else:
                    s0 = min(s0, int(_data_[k][0, 0]))

                if s1 is None:
                    s1 = int(_data_[k][-1, 0])
                else:
                    s1 = max(s1, int(_data_[k][-1, 0]))

        PPG = np.ones((s1 - s0 + 1, len(WAVEFORMS) + 2)) * np.iinfo(np.int32).min
        PPG[:, 0] = np.arange(s0, s1 + 1)
        c = 2
        for k in WAVEFORMS:
            try:
                # copy data into merged PPG waveform remove duplicate points
                sqn, idx0 = np.unique(_data_[k][:, 0], return_index=True)
                tmp = _data_[k][idx0, :]
                idx = np.in1d(PPG[:, 0], tmp[:, 0])
                PPG[idx, 1] = tmp[:, 1]
                PPG[idx, c] = tmp[:, 2]
                idx = np.isnan(PPG[:, c])
                PPG[idx, c] = PPG_SAT_HIGH
            except KeyError:
                pass
            c += 1

        idx2 = np.isnan(PPG[:, 1])
        PPG[idx2, 1] = np.interp(PPG[idx2, 0], PPG[~idx2, 0], PPG[~idx2, 1])

    gain_col = 4 if "SPO2_CTRL2" in data.keys() else 2
    gain = 4 * np.ones(PPG.shape[0])
    if "SPO2_CTRL" in data.keys():
        idx = data["SPO2_CTRL"][:, 6] == 6
        if np.sum(idx) > 0:
            last_sn = data["SPO2_CTRL"][idx, 0][0]
            last_gain = data["SPO2_CTRL"][idx, gain_col][0]
            for i in range(1, np.sum(idx)):
                sn = data["SPO2_CTRL"][idx, 0][i]
                dx = (PPG[:, 0] > last_sn) * (PPG[:, 0] <= sn)
                gain[dx] = last_gain
                last_sn = sn
                last_gain = data["SPO2_CTRL"][idx, gain_col][i]
            dx = PPG[:, 0] > last_sn
            gain[dx] = last_gain
    PPG = np.c_[PPG, gain]
    return PPG


ECG_MISSING_DATA = 2 ** 23 - 32
ECG_UPPER_RAIL = 2 ** 23 - 32
ECG_LOWER_RAIL = 32 - 2 ** 23


def preprocess_ecg(
    data, leads=("ECG_I", "ECG_II", "ECG_III"), upsample=False, min_cols=5
):
    """Package ECG waveforms input to offline algorithms"""

    metadata = {"dups": {}}
    if data is None:
        metadata["errors"] = "No data"
        return None, metadata

    # make single ecg matrix
    _data_ = {}
    if upsample:
        ECG = np.ones((data["ECG_II"].shape[0], 5)) * np.nan
        x = data["ECG_II"][:, 0]
        ECG[:, 0] = x
        f = scipy.interpolate.interp1d(
            data["ECG_II"][:, 0], data["ECG_II"][:, 1], bounds_error=False
        )
        ECG[:, 1] = f(x)
        f = scipy.interpolate.interp1d(
            data["ECG_I"][:, 0], data["ECG_I"][:, 2], bounds_error=False
        )
        ECG[:, 2] = f(x)
        ECG[:, 3] = data["ECG_II"][:, 2]
        f = scipy.interpolate.interp1d(
            data["ECG_III"][:, 0], data["ECG_III"][:, 2], bounds_error=False
        )
        ECG[:, 4] = f(x)

        idx = ~np.isnan(ECG[:, 2])
        _data_["ECG_I"] = ECG[idx, :][:, (0, 1, 2)]
        _data_["ECG_III"] = ECG[idx, :][:, (0, 1, 4)]
        _data_["ECG_II"] = data["ECG_II"]
    else:
        _data_["ECG_I"] = data["ECG_I"]
        _data_["ECG_II"] = data["ECG_II"]
        _data_["ECG_III"] = data["ECG_III"]

    ECG = None
    num_available_leads = 0
    s0 = None
    s1 = None
    for k in leads:
        if k in _data_.keys():
            num_available_leads += 1
            if s0 is None:
                s0 = int(_data_[k][0, 0])
            else:
                s0 = min(s0, int(_data_[k][0, 0]))

            if s1 is None:
                s1 = int(_data_[k][-1, 0])
            else:
                s1 = max(s1, int(_data_[k][-1, 0]))
    if num_available_leads > 0:
        if s1 - s0 > 5400000:
            metadata["errors"] = "Block too long"
        else:
            ECG = np.ones((s1 - s0 + 1, num_available_leads + 2)) * np.nan
            ECG[:, 0] = np.arange(s0, s1 + 1)
            for i, k in enumerate(leads):
                if k in _data_.keys():
                    # remove duplicate points
                    sqn, idx0 = np.unique(_data_[k][:, 0], return_index=True)
                    metadata["dups"][k] = (_data_[k].shape[0] - idx0.shape[0]) / 500
                    tmp = _data_[k][idx0, :]
                    # copy data into merged ECG waveform
                    idx1 = np.in1d(ECG[:, 0], tmp[:, 0])
                    ECG[idx1, 1] = tmp[:, 1]
                    ECG[idx1, 2 + i] = tmp[:, 2]
            idx = ECG[:, 2:] >= ECG_UPPER_RAIL
            ECG[:, 2:][idx] = ECG_MISSING_DATA
            idx = ECG[:, 2:] <= ECG_LOWER_RAIL
            ECG[:, 2:][idx] = ECG_MISSING_DATA
    else:
        metadata["errors"] = "No ECG found"
    idx2 = np.isnan(ECG[:, 1])
    ECG[idx2, 1] = np.interp(ECG[idx2, 0], ECG[~idx2, 0], ECG[~idx2, 1])

    if min_cols > 0 and ECG.shape[1] < min_cols:
        ECG = np.c_[
            ECG, ECG_MISSING_DATA * np.ones((ECG.shape[0], min_cols - ECG.shape[1]))
        ]

    return ECG


def preprocess_activity(data):
    """Package Accelerometer and posture pack data for input to offline activity algorithm"""

    ut_start = np.max(
        [data["ACC_ARM"][0, 1], data["ACC_ECG"][0, 1], data["ACC_WRT"][0, 1]]
    )
    sn_start = np.max(
        [data["ACC_ARM"][0, 0], data["ACC_ECG"][0, 0], data["ACC_WRT"][0, 0]]
    )
    sn_stop = np.min(
        [data["ACC_ARM"][-1, 0], data["ACC_ECG"][-1, 0], data["ACC_WRT"][-1, 0]]
    )
    sn_array = np.arange(sn_start, sn_stop, 10)
    l_sn = sn_array.shape[0]

    ACC_ARRAY = 32768 * np.ones((l_sn, 15))

    i_acc = 0
    i_pos = 0

    l_pos = data["POSTURE_PKT"].shape[0]

    ACC_ARRAY[:, 0] = sn_array
    ACC_ARRAY[:, 1] = ut_start + 0.002 * (sn_array - sn_start)

    for sn in sn_array:
        i_wrt = bisect.bisect_left(data["ACC_WRT"][:, 0], sn)
        ACC_ARRAY[i_acc, 2:5] = data["ACC_WRT"][i_wrt, 2:5]
        i_arm = bisect.bisect_left(data["ACC_ARM"][:, 0], sn)
        ACC_ARRAY[i_acc, 5:8] = data["ACC_ARM"][i_arm, 2:5]
        i_ecg = bisect.bisect_left(data["ACC_ECG"][:, 0], sn)
        ACC_ARRAY[i_acc, 8:11] = data["ACC_ECG"][i_ecg, 2:5]
        # add posture to the array
        i_posture = bisect.bisect_right(data["POSTURE_PKT"][:, 0], sn)
        if i_posture < l_pos and (data["POSTURE_PKT"][i_posture, 0] - sn) < 10:
            i_pos += 1
            # stuff the packet
            ACC_ARRAY[i_acc, 11:15] = data["POSTURE_PKT"][i_posture, 3:7]
        i_acc += 1
    return ACC_ARRAY


SCG_MISSING_DATA = 2 ** 32 - 1


def preprocess_scg(data):

    min_sn = data["SCG"][:, 0].min()
    max_sn = data["SCG"][:, 0].max()

    SCG = np.ones((int(max_sn - min_sn + 1), data["SCG"].shape[1])) * np.nan
    SCG[:, 0] = np.arange(min_sn, max_sn + 1)
    idx = np.in1d(SCG[:, 0], data["SCG"][:, 0])
    if np.sum(idx) > 0:
        for i in range(1, SCG.shape[1]):
            SCG[idx, i] = data["SCG"][:, i]
            SCG[np.isnan(SCG[:, i]), i] = SCG_MISSING_DATA
    idx = np.isnan(SCG[:, 1])
    if np.sum(idx) > 0:
        SCG[idx, 1] = np.interp(SCG[idx, 0], SCG[~idx, 0], SCG[~idx, 1])

    return SCG

PRES_MISSING_DATA = 2 ** 32 - 1


def preprocess_pres(data):

    min_sn = data["PRES"][:, 0].min()
    max_sn = data["PRES"][:, 0].max()

    PRES = np.ones((int(max_sn - min_sn + 1), data["PRES"].shape[1])) * np.nan
    PRES[:, 0] = np.arange(min_sn, max_sn + 1)
    idx = np.in1d(PRES[:, 0], data["PRES"][:, 0])
    if np.sum(idx) > 0:
        for i in range(1, PRES.shape[1]):
            PRES[idx, i] = data["PRES"][:, i]
            PRES[np.isnan(PRES[:, i]), i] = PRES_MISSING_DATA
    idx = np.isnan(PRES[:, 1])
    if np.sum(idx) > 0:
        PRES[idx, 1] = np.interp(PRES[idx, 0], PRES[~idx, 0], PRES[~idx, 1])

    return PRES
