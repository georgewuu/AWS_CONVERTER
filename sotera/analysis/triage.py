from numpy import empty, where, round as npround, array
from sotera.cluster.control import cluster_decorate
from sotera.io import load_session_data
from sotera.analysis.numerics import (
    nibp_points_with_cnibp,
    nibp_points_only,
    cnibp_bland_altman_points,
    device_alarms_histogram,
    device_alarms_info,
    spo2_control_histogram,
    ppg_beat_class_histogram,
    ppg_pmi_histogram,
    calculate_calibration_times,
)
from sotera.analysis.alarms import (
    patient_time_in_histograms,
    patient_alarms,
    patient_numeric_histograms,
    patient_update_session_data,
)


def has_all(data, arrs):
    arrs_set = set(arrs)
    return arrs_set.intersection(set(data.keys())) == arrs_set


def mine_nibp(pgsql_, data):
    sql = """INSERT INTO aa_nibp_values
             (hid, unixtime, time_from_last, sys, dia, map,
              cnibp_unixtime, cnibp_sys, cnibp_dia, cnibp_map, pr, error_code)
              VALUES ({},{},{},{},{},{},{},{},{},{},{},{})"""
    if "BP" in data.keys() and data["BP"].shape[0] > 0:
        with pgsql_, pgsql_.cursor() as cursor:
            if "CNIBP" in data.keys() and data["CNIBP"].shape[0] > 0:
                for p in nibp_points_with_cnibp(data["BP"], data["CNIBP"]):
                    cursor.execute(sql.format(data["__info__"].hid, *p))
            else:
                for p in nibp_points_only(data["BP"]):
                    cursor.execute(sql.format(data["__info__"].hid, *p))


def mine_cnibp_bland_altman(pgsql_, data):
    if "BP" in data.keys() and "CNIBP" in data.keys():
        with pgsql_, pgsql_.cursor() as cursor:
            for p in cnibp_bland_altman_points(data["BP"], data["CNIBP"]):
                cursor.execute(
                    """INSERT
                         INTO analytics.aa_cnibp_bland_altman
                              (hid, bp_unixtime, bp_sys, bp_dia, bp_map,
                               cnibp_unixtime, cnibp_sys, cnibp_dia,
                               cnibp_map, mean_sys, mean_dia, mean_map,
                               diff_sys, diff_dia, diff_map)
                       VALUES ({},{},{},{},{},{},{},{},{},{},{},{},{},{},{})
                    """.format(
                        data["__info__"].hid, *p
                    )
                )


def mine_cnibp_calibration_times(pgsql_, data):
    arrs = (
        "TIME_SYNC",
        "CNIBP_CAL_PKT",
        "SENSOR_CONNECTION",
        "BP",
        "POSTURE_PKT",
        "ARM_HT_PKT",
    )
    if has_all(data, arrs):
        with pgsql_, pgsql_.cursor() as cursor:
            for p in calculate_calibration_times(
                data["TIME_SYNC"],
                data["CNIBP_CAL_PKT"],
                data["SENSOR_CONNECTION"],
                data["BP"],
                data["POSTURE_PKT"],
                data["ARM_HT_PKT"],
            ):
                cursor.execute(
                    """INSERT
                         INTO analytics.cnibp_calibrations
                              (hid, cal_num, cal_start, pie_fill_time,
                               nibp_inflation_time)
                       VALUES ({},{},{},{},{})
                    """.format(
                        data["__info__"].hid, *p
                    )
                )


def mine_device_alarms_packets(pgsql_, data):
    if "ALARMS" in data.keys():
        with pgsql_, pgsql_.cursor() as cursor:
            for p in device_alarms_histogram(data["ALARMS"]):
                cursor.execute(
                    """INSERT
                         INTO ih_device_alarms_histograms
                              (hid, bin, frequency)
                       VALUES ({},{},{})
                    """.format(
                        data["__info__"].hid, *p
                    )
                )

            for p in device_alarms_info(data["ALARMS"], data["__info__"].tz):
                cursor.execute(
                    """INSERT
                          INTO analytics.device_alarm_info
                               (hid, seqnum_sent, utc_timestamp,
                                alarm, severity, state, seqnum_start,
                                day_of_week)
                        VALUES ({},{},'{}',{},{},{},{},{})
                    """.format(
                        data["__info__"].hid, *p
                    )
                )


def mine_spo2_control_histogram(pgsql_, data):
    if "SPO2_CTRL" in data.keys():
        with pgsql_, pgsql_.cursor() as cursor:
            for p in spo2_control_histogram(data["SPO2_CTRL"]):
                cursor.execute(
                    """INSERT
                         INTO ih_spo2_ctrl_histograms
                              (hid, bin, frequency)
                       VALUES ({},{},{})
                    """.format(
                        data["__info__"].hid, *p
                    )
                )


def mine_ppg_beat_class_histogram(pgsql_, data):
    if "PPG_BEAT_PKT" in data.keys():
        with pgsql_, pgsql_.cursor() as cursor:
            for p in ppg_beat_class_histogram(data["PPG_BEAT_PKT"]):
                cursor.execute(
                    """INSERT
                         INTO ih_ppg_beat_class_histograms
                              (hid, bin, frequency)
                       VALUES ({},{},{})
                    """.format(
                        data["__info__"].hid, *p
                    )
                )


def mine_ppg_pmi_histogram(pgsql_, data):
    if "HR_SCI" in data.keys() and "PPG_BEAT_PKT" in data.keys():
        with pgsql_, pgsql_.cursor() as cursor:
            for p in ppg_pmi_histogram(data["PPG_BEAT_PKT"]):
                cursor.execute(
                    """INSERT
                         INTO ih_ppg_pmi_histograms
                              (hid, bin, frequency)
                       VALUES ({},{},{})
                    """.format(
                        data["__info__"].hid, *p
                    )
                )


def process_hr_a_data(data):
    if "HR" in data.keys():
        if "PR" not in data.keys():
            aHR = data["HR"]
        else:
            PR = data["PR"]
            HR = data["HR"]
            aHR = empty((len(HR), 3))
            pdx_track = 0
            # Step Through the HRs
            for idx in range(len(HR)):
                ti = HR[idx][1]  # Time of HR numeric
                if ti < PR[0][1]:  # If HR starts before PR append HRs to aHR
                    aHR[idx] = HR[idx]
                else:
                    pdx = None
                    # Find corresponding PR index pdx (if any)
                    if ti >= PR[pdx_track, 1]:
                        # Check if current PR or next PR correspond..
                        if (
                            PR[pdx_track, 1] == ti
                        ):  # If PR at index pdx_track is equal to ti
                            pdx = pdx_track
                        else:
                            if pdx_track < len(PR) - 2:
                                if (
                                    PR[pdx_track + 1, 1] == ti
                                ):  # If PR at index pdx_track+1 is equal to ti
                                    pdx = pdx_track + 1
                                elif (
                                    PR[pdx_track, 1] < ti < PR[pdx_track + 1, 1]
                                ):  # If HR beat in question is betwen
                                    # pdx_track and pdx_track+1
                                    pdx = pdx_track

                    if (
                        not pdx
                    ):  # If the previous PR and the one after do not match up...
                        # (Missing Data/PR or HR removed/etc)
                        if ti in PR[:, 1]:  # If exact HR time is in PR
                            pdxs = [i for i, x in enumerate(PR[:, 1]) if x == ti]
                            if len(pdxs) == 1:  # Only One Exact Match
                                pdx = pdxs[0]
                            else:
                                print(idx)
                                print(pdxs)
                                # error = Time_Error
                        else:  # Look between ti-3 and ti to find PR
                            s_ti = ti - 3
                            ixn = (PR[:, 1] > s_ti) * (PR[:, 1] < ti)
                            pdxs = where(ixn)[0]
                            if pdxs.size > 0:
                                if (
                                    len(pdxs) == 1
                                ):  # Only One PR value in 3 second window
                                    pdx = pdxs[0]
                                else:
                                    pdx = pdxs[
                                        len(pdxs) - 1
                                    ]  # Take value closest to HR time

                    if pdx:  # Corresponding PR found
                        pdx_track = pdx
                        # print(pdx)
                        if PR[pdx][2] == -1 or PR[pdx][2] == -2:
                            aHR[idx] = HR[idx]
                        else:
                            aHR[idx] = [HR[idx][0], HR[idx][1], -3]

                    else:  # No Corresponding PR
                        aHR[idx] = HR[idx]

        data["HR_A"] = aHR

    return data


@cluster_decorate()
def post_session_triage(pgsql_, aid, jobid, job):

    hid = job["hid"]
    data = load_session_data(hid, pgsql_=pgsql_)
    data = process_hr_a_data(data)

    # dedup alarms
    alarms = array(list(set(tuple(row) for row in data['ALARMS'])))
    if data['ALARMS'].shape[0] != alarms.shape[0]:
        data['ALARMS'] = alarms

    # purge any info about this hid that might exist in tables
    # this makes it ok to rerun sessions
    with pgsql_, pgsql_.cursor() as cursor:
        cursor.execute(
            f"""DELETE FROM analytics.aa_alarms WHERE hid = {hid};
                DELETE FROM analytics.aa_cnibp_bland_altman WHERE hid = {hid};
                DELETE FROM analytics.cnibp_calibrations WHERE hid = {hid};
                DELETE FROM analytics.aa_nibp_values WHERE hid = {hid};
                DELETE FROM analytics.aa_numeric_histograms WHERE hid = {hid};
                DELETE FROM analytics.aa_time_in_alarm_histograms WHERE hid = {hid};
                DELETE FROM analytics.device_alarm_info WHERE hid = {hid};
                DELETE FROM analytics.aa_time_in_xx_histograms WHERE hid = {hid};
                DELETE FROM analytics.aa_time_in_dropout_histograms WHERE hid = {hid};
                DELETE FROM analytics.ih_device_alarms_histograms WHERE hid = {hid};
                DELETE FROM analytics.ih_spo2_ctrl_histograms WHERE hid = {hid};
                DELETE FROM analytics.ih_ppg_beat_class_histograms WHERE hid= {hid};
                DELETE FROM analytics.ih_ppg_pmi_histograms WHERE hid = {hid};"""
        )

    # add missing numerics arrays variables to data dictionary
    for k in ["BP", "CNIBP"]:
        if k in data.keys():
            data["%s_SYS" % k] = data[k][:, (0, 1, 2)]
            data["%s_DIA" % k] = data[k][:, (0, 1, 3)]
            data["%s_MAP" % k] = data[k][:, (0, 1, 4)]

    post_combo = "HR_SCI" in data.keys()

    # alarms analysis
    times, cr_data = patient_time_in_histograms(pgsql_, hid, data, post_combo)
    patient_alarms(pgsql_, hid, data["__info__"].site, data, cr_data)
    patient_numeric_histograms(pgsql_, hid, data)

    session = {
        "hid": hid,
        "site": data["__info__"].site,
        "sw_version": "0.0.0.0",
        "duration": int(npround(times["WT"]["active"])),  # fixme ?
    }
    patient_update_session_data(pgsql_, session, times)

    # other data triage
    mine_device_alarms_packets(pgsql_, data)
    mine_cnibp_bland_altman(pgsql_, data)
    mine_cnibp_calibration_times(pgsql_, data)
    mine_nibp(pgsql_, data)
    mine_spo2_control_histogram(pgsql_, data)
    mine_ppg_beat_class_histogram(pgsql_, data)
    mine_ppg_pmi_histogram(pgsql_, data)

    return "done"
