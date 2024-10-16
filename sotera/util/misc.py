import numpy as np
from intervaltree import Interval, IntervalTree


def get_vfib_intervals(HR_RHYTHM, use_rhythm_sn=False, winlen=12288):
    idx = (
        (np.bitwise_and(HR_RHYTHM[:, 6].astype(int), 2) > 0)
        * (HR_RHYTHM[:, 6] > 0)
        * (HR_RHYTHM[:, 6] < 10)
    )
    if use_rhythm_sn:
        itree = IntervalTree(
            [
                Interval(x[0], x[1] + 500, data=("VFIB",))
                for x in HR_RHYTHM[idx, :][:, (7, 0)]
            ]
        )
    else:
        itree = IntervalTree(
            [
                Interval(max(x - winlen, 0), x + 500, data=("VFIB",))
                for x in HR_RHYTHM[idx, :][:, 0]
            ]
        )
    itree.merge_overlaps(lambda x, y: ("VFIB",))
    return itree


def get_afib_intervals(HR_RHYTHM, use_rhythm_sn=False, winlen=98000):
    idx = HR_RHYTHM[:, 6].astype(int) == 1
    if use_rhythm_sn:
        itree = IntervalTree(
            [
                Interval(x[0], x[1] + 500, data=("AFIB",))
                for x in HR_RHYTHM[idx, :][:, (7, 0)]
            ]
        )
    else:
        itree = IntervalTree(
            [
                Interval(max(x - winlen, 0), x + 500, data=("AFIB",))
                for x in HR_RHYTHM[idx, :][:, 0]
            ]
        )
    itree.merge_overlaps(lambda x, y: ("AFIB",))
    return itree


def get_asys_intervals(HR_RHYTHM, use_rhythm_sn=False, winlen=1000):
    idx = (
        (np.bitwise_and(HR_RHYTHM[:, 6].astype(int), 4) > 0)
        * (HR_RHYTHM[:, 6] > 0)
        * (HR_RHYTHM[:, 6] < 10)
    )
    itree = IntervalTree(
        [Interval(x - winlen, x, data=("ASYS",)) for x in HR_RHYTHM[idx, 0]]
    )
    itree.merge_overlaps(lambda x, y: ("ASYS",))
    return itree


def deviceid_to_serialno(deviceid):
    serialno = (
        "????20"
        + str(((deviceid >> 18) & 0x3F) + 13).zfill(2)
        + str((deviceid >> 14) & 0xF).zfill(2)
        + str(deviceid & 0x3FFF).zfill(5)
    )
    return serialno


def serialno_to_deviceid(s):

    deviceid = -1

    if len(s) == 15:
        s = s[6:]

    if len(s) == 9:
        try:
            year = int(s[0:2])
            month = int(s[2:4])
            serial = int(s[4:])
            if year < 13:
                # 'Year 20%d not supported;
                # device IDs can only be generated from 2013 on.' % year
                pass
            elif serial > 16383:
                # 'Serial number overflow'
                pass
            else:
                deviceid = int(
                    (serial & 0x3FFF) | (month & 0xF) << 14 | ((year - 13) & 0x3F) << 18
                )
        except ValueError:
            #  print 'Invalid serial number'
            pass

    return deviceid
