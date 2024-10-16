import warnings
import platform

is_not_jython = platform.system() != "Java"

OTHER_KEYS = (
    "HR",
    "BP_PKT",
    "BP",
    "RR",
    "SPO2",
    "TEMP",
    "ACC_ECG",
    "ACC_ARM",
    "ACC_WRT",
    "PR",
    "SPO2_CTRL",
    "IP",
    "PAT_PKT",
    "CNIBP_CAL_PKT",
    "CNIBP",
    "ARM_HT_PKT",
    "POSTURE_PKT",
    "PPG_BEAT_PKT",
    "PPG_DEBUG_PKT",
    "ACC_ECG_500",
    "SCG",
    "CNIBP",
    "ALARMS",
    "ALARM_STATUS",
    "ALARM_LIMITS",
    "BATTERY_STATUS",
    "SENSOR_CONNECTION",
    "HR_SCI",
    "HR_RHYTHM",
    "RHYTHM_AFIB",
    "RHYTHM_VFIB",
    "HR_BEAT",
    "NIBP_PEAK",
    "PRES",
)

INTONLY = (
    "HR",
    "BP",
    "RR",
    "SPO2",
    "TEMP",
    "ACC_ECG",
    "ACC_ARM",
    "ACC_WRT",
    "PR",
    "IP",
    "CNIBP",
    "ACC_ECG_500",
    "SCG",
    "PRES" "ALARMS",
    "BATTERY_STATUS",
    "SENSOR_CONNECTION",
)

ECG_KEYS = ("ECG_I", "ECG_II", "ECG_III", "ECG_V", "ECG_AVL", "ECG_AVR", "ECG_AVF")
PPG_KEYS = ("IR_FILT", "RED_FILT", "AMBIENT", "IR_AC", "IR_DC", "RED_AC", "RED_DC")

WAVEFORMS = (
    "ECG_I",
    "ECG_II",
    "ECG_III",
    "ECG_V",
    "ECG_AVL",
    "ECG_AVR",
    "ECG_AVF",
    "IR_FILT",
    "RED_FILT",
    "AMBIENT",
    "IR_AC",
    "IR_DC",
    "RED_AC",
    "RED_DC",
    "ACC_ECG",
    "ACC_ARM",
    "ACC_WRT",
    "IP",
    "ACC_ECG_500",
    "SCG",
    "PRES",
    "ECG",
    "PPG",
)

NUMERICS = ("HR", "PR", "RR", "SPO2", "BP", "CNIBP", "ALARMS")


def make_key(hid, blockno, fn, tier=None):
    if tier:
        return "{}/{}/{:04d}/{}".format(tier, hid, blockno, fn)
    else:
        return "{}/{:04d}/{}".format(hid, blockno, fn)


def arrays_to_get(requested_arrays, all_arrays):
    if type(requested_arrays) is str:
        requested_arrays = (requested_arrays,)
    requested_arrays = set(requested_arrays) | set(["TIME_SYNC"])
    return set(all_arrays).intersection(requested_arrays)


def find_tier(fn):
    arr = fn.split(".")[0]
    if arr in WAVEFORMS:
        return "tier1"
    else:
        return "tier2"


def array_file(arr):
    return "{}.npy.xz".format(arr)


def array_key(hid, blockno, arr):
    fn = array_file(arr)
    return make_key(hid, blockno, fn, find_tier(fn))


if is_not_jython:
    from . import local
try:
    from . import munge
except:
    warnings.warn("munge submodule not available")
try:
    from . import cloud
    from .cloud import download_session_data, load_session_data
except ImportError:
    warnings.warn("Will not be able to load data from cloud")
try:
    from . import annotation
except:
    warnings.warn("Will not be able to load annotations from cloud")
