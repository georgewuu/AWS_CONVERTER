import numpy as np

sn25    = np.arange(0,250,10)
zr25    = np.zeros_like(sn25)
sn125   = np.arange(125)
zr125   = np.zeros_like(sn125)
sn75    = np.arange(75)
zr75    = np.zeros_like(sn75)

#MOTION AND NO DISPLAY VALUES
NO_DISP_VAL = -1
NO_DISP_VISI = 16383
MOTION_VAL = -2
MOTION_VISI = 16382
VARIANCE_VAL = -3
VARIANCE_VISI = 16381
PLUSPLUS_VAL = -4
PLUSPLUS_VISI = 16380
MINUSMINUS_VAL = -5
MINUSMINUS_VISI = 16379
CUFF_INFLATION_VAL = -6
CUFF_INFLATION_VISI = 16378
WALKING_VAL = -7
WALKING_VISI = 16377
PPG_PERSISTENT_VAL = -8
PPG_PERSISTENT_VISI = 16376
PPG_TRANSIENT_VAL = -9
PPG_TRANSIENT_VISI = 16375
PAHC_RESET_VAL = -10
PAHC_RESET_VISI = 16374

def replace_constants(array, columns):
    tmp = array[:,columns]
    tmp[ tmp == NO_DISP_VISI ] = NO_DISP_VAL
    tmp[ tmp == MOTION_VISI ] = MOTION_VAL
    tmp[ tmp == VARIANCE_VISI ] = VARIANCE_VAL
    tmp[ tmp == PLUSPLUS_VISI ] = PLUSPLUS_VAL
    tmp[ tmp == MINUSMINUS_VISI ] = MINUSMINUS_VAL
    tmp[ tmp == CUFF_INFLATION_VISI ] = CUFF_INFLATION_VAL
    tmp[ tmp == WALKING_VISI ] = WALKING_VAL    
    tmp[ tmp == PPG_PERSISTENT_VISI ] = PPG_PERSISTENT_VAL
    tmp[ tmp == PPG_TRANSIENT_VISI ] = PPG_TRANSIENT_VAL
    tmp[ tmp == PAHC_RESET_VISI ] = PAHC_RESET_VAL
    array[:,columns] = tmp
    return array

# DEFAULT EMPTY COLUMN VALUE
EMPTY_VAL = -7e8;

#Constant Multipliers
k_ppg_dc = 3300./65535. #millivolts
k_ppg_filt = (1./9977.)*(3300./65535.) #millivolts
k_ecg = (1./6)*(4800./(2**24)) #millivolts
k_ip = (1./4095)*(1./2)*(16)*(2400./(2**24)) #millivolts
k_ambient = (3300./4095.) #millivolt


posture_codes = {
    -2: "",
    -1: "",
    0:  "UNK",   # Uknown
    1:  "U90",   # Upright 90 deg
    2:  "U45",   # Upright 45 deg
    3:  "LSP",   # Lying Supine
    4:  "LPR",   # Lying Prone
    5:  "LRS",   # Lying on Right Side
    6:  "LLS",   # Lying on Left Site
    7:  "WLK",   # Walking
    8:  "RCLR",  ## Reclined  Right side
    9:  "RCLL",  ## Reclined Left Side 
    10: "FALL",  # Fall
    11: "S-U90", # User selection: 
    12: "S-U45",
    13: "S-LSP",
    14: "S-LPR",
    15: "S-LRS",
    16: "S-LLS",

}

ltaa_codes = {
    0: "",
    1: "Afib",
    2: "Vfib",
    3: "Afib+VFib",
    4: "Asys",
    5: "Asys+Afib",
    6: "Asys+Vfib"
}

alarm_codes = {
    0: 'LT-LOW-HR',
    3: 'VTACH-PULSELESS',
    4: 'VTACH-W-PULSE',
    5: 'APNEA-W-RESP',
    6: 'CUFF-BATTERY-TEMP',
    7: 'MONITOR-BATTERY-TEMP',
    8: 'UNUSUAL-MOTION',
    9: 'CUFF-NO-PULSE',
    11: 'PR-HIGH',
    12: 'PR-LOW',
    13: 'HR-HIGH',
    14: 'HR-LOW',
    15: 'BP-SYS-HIGH',
    16: 'BP-SYS-LOW',
    17: 'BP-DIA-HIGH',
    18: 'BP-DIA-LOW',
    19: 'BP-MAP-HIGH',
    20: 'BP-MAP-LOW',
    21: 'RESP-HIGH',
    22: 'RESP-LOW',
    23: 'SPO2-HIGH',
    24: 'SPO2-LOW',
    25: 'SKIN-TEMP-HIGH',
    26: 'SKIN-TEMP-LOW',
    27: 'ECG-ONE-OFF',
    28: 'ECG-MANY-OFF',
    29: 'CHEST-DISCONNECTED',
    30: 'CHEST-FAILURE',
    31: 'CHEST-MULTIPLE',
    32: 'THUMB-NO-PULSE',
    33: 'THUMB-OFF',
    34: 'THUMB-DISCONNECTED',
    35: 'THUMB-PARTIAL-FAILURE',
    37: 'CUFF-BATTERY-LOW',
    38: 'CUFF-BATTERY-EMPTY',
    40: 'CUFF-NO-PULSE-ALERT',
    41: 'CUFF-LEAK',
    43: 'CUFF-NOT-CALIBRATED',
    44: 'CUFF-UNOBTAINABLE',
    45: 'CUFF-FAILURE',
    46: 'CUFF-DISCONNECTED',
    47: 'CUFF-BIOMEDCAL-EXPIRED',
    48: 'CUFF-PRESSURE',
    49: 'CUFF-MULTIPLE',
    50: 'CUFF-OCCLUDED',
    51: 'MONITOR-BATTERY-LOW',
    52: 'MONITOR-BATTERY-CRIT-LOW',
    53: 'MONITOR-BATTERY-TOO-LOW',
    54: 'INVALID-PLUG',
    55: 'AUDIO-FAILURE',
    56: 'MONITOR-FAILURE',
    58: 'SENSORS-ALL-DISCONNECTED',
    59: 'MOTION-HR',
    60: 'MOTION-PR',
    61: 'MOTION-BP',
    62: 'MOTION-RESP',
    63: 'MOTION-SPO2',
    64: 'PATIENTID-CONFIRM',
    65: 'PATIENTID-REJECTED',
    66: 'ELECTRIC-SHOCK',
    67: 'PATIENT-WALKED',
    68: 'PATIENT-CALLING',
    69: 'PATIENT-TAMPERING',
    71: 'MONITORING-IN-PROGRESS',
    72: 'CONNECT-TO-PATIENT',
    73: 'PDS-DISK-FULL',
    74: 'PDS-DISK-CRIT-FULL',
    75: 'PDS-TEMP',
    76: 'PDS-BATTERY',
    77: 'PDS-MEMORY',
    78: 'NETWORK-LOST',
    79: 'CHEST-SOFTWARE',
    80: 'CUFF-SOFTWARE',
    81: 'TEMPERATURE-FAULT',
    82: 'ACCEL-WRIST',
    83: 'ACCEL-CHEST',
    84: 'ACCEL-UPPER-ARM',
    87: 'LT-CRITICAL-PR',
    88: 'CNIBP-CAL-NEEDED',
    89: 'MOTION-CNIBP',
    90: 'CNIBP-CAL-FAILED',
    93: 'AFIB-CVR',
    94: 'AFIB-RVR',
    95: 'VFIB',
    96: 'ASYSTOLE',
    116: 'BAT-PAK-FAULT',
    117: 'DEMO-MODE',
    118: 'FALL',
    119: 'UNDESIRED-POSTURE',
    120: 'IMMOBILITY',
    121: 'BAT-PAK-LOW',
    122: 'BAT-PAK-CRITICAL',
    123: 'CNIBP-CAL-24HRS',
    124: 'CNIBP-CAL-30PCMAP',
    125: 'CNIBP-CAL-STALE',
    126: 'CG-OVERHEATED',
    127: 'CG-BATTERY-LOW',
    128: 'CG-BATTERY-CRIT-LOW',
    129: 'CG-DISCONNECTED',
    130: 'CG-FAILURE',
    131: 'CG-REPOSITION',
    132: 'CG-CHANGE-DISPOSABLE',
    133: 'CG-NO-DISPOSABLE',
    134: 'THUMB-REPLACE',
    135: 'WALKING',
    136: 'SPO2-STALE'
}
