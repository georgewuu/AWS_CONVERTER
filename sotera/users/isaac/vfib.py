import numpy as np
import scipy.signal as signal

k_ecg = 4.76837158203125e-05 # ViSi scaling
ECG_MAX = 10.0/k_ecg # 10.0 mV
ECG_MIN = 0 #0.150/k_ecg #  0.150 mV

def update_count2(ecg, Fs=125):    
    # bandpass filter the waveform
    N  = 1    # Filter order
    Wn = np.array([2.0*13.0/Fs,  2.0*16.5/Fs]) # -3dB cutoff frequencies
    B, A = signal.iirfilter(N, Wn, btype="bandpass", ftype="butter")
    ecg_filt = signal.lfilter(B, A, ecg)   
    
    # mean and max are calculated for non-overlapping windows of 256 samples (~1.024 seconds) in the buffer
    l_ecg = ecg_filt.shape[0]
    l_win = l_ecg/8 # ~1.024 seconds
    win_bounds = np.arange(0,l_ecg+1,l_win)
    num_bounds = win_bounds.shape[0]
    count2 = 0
    for i in range(num_bounds-1):
        y = np.abs(ecg_filt[win_bounds[i]:win_bounds[i+1]-1])
        mean_y = np.mean(y)
        SamplesAboveThresh = y[y > mean_y]
        count2 = count2 + SamplesAboveThresh.shape[0]
        SamplesAboveThresh = 0
        
    return count2


def update_tcsc(ecg, Fs=125):    
    # update the threshold crossing sample count
    # note: the paper recommends that the base calculation be performed on
    #       3 second over-lapping windows updated every 1 second
    
    V0 = 0.2 # threshold
    
    Ls = int(3*Fs)
    Le = ecg.shape[0]
    Lu = 1*Fs
    
    NumIts = int(np.round((Le-Ls)/Lu) + 1)
    
    # remove the mean
    ecg = ecg - np.mean(ecg)
    
    # normalize the waveform using the max value
    max_ecg = np.max(ecg)
    ecg = (1/max_ecg)*ecg
    
    # pre-allocate
    N = np.zeros((NumIts,1))
      
    for i in range(Ls-1):
        for j in range(NumIts):
            idx = j*Lu+i
            if np.abs(ecg[idx]) > V0:
                N[j] = N[j] + 1
    
    N = (100.0/Ls)*N
    tcsc = np.mean(N)
    
    return tcsc

def update_vfleak(ecg):
    l_ecg = ecg.shape[0]
    diff_ecg = np.diff(ecg, n=1, axis=0)
    diff_ecg = np.abs(diff_ecg)
    
    # mean period
    T = 2.0*3.14159265*np.sum(np.abs(ecg[1:l_ecg-1]))/np.sum(diff_ecg)
    
    Num = 0
    Den = 0
    j = int(np.round(T/2))
    
    for i in range(j,l_ecg):
        Num = Num + np.abs(ecg[i] + ecg[i-j])
        Den = Den + np.abs(ecg[i]) + np.abs(ecg[i-j])
    
    vfleak = Num/Den
    
    return vfleak

def update_sampen(data):
    dim = 2 # dimension
    r = 0.2*np.std(data)
    
    N = data.shape[0]
    correl = np.zeros((2,1))
    dataMat = np.zeros((dim+1,N-dim))
   
    for i in range(dim+1):
        dataMat[i,:] = data[i:N-dim+i]
    
    # run both dimensions simultaneously
    m = dim + 1
    count2 = np.zeros((N-m,1))
    count3 = np.zeros((N-m,1))
    tempMat = dataMat[0:m,:] 
        
    for i in range(N-m):
        
        # calculate Chebyshev distance, excluding self-matching case
        x1 = tempMat[:,i+1:N-dim]
        x2 = np.ones((3,N-m-i))
        y  = tempMat[:,i:i+1]
        x2[0,:] = y[0]
        x2[1,:] = y[1]
        x2[2,:] = y[2]
        dim2 = np.abs(x1[0:2,:]-x2[0:2,:])
        dim3 = np.abs(x1-x2)
        dist2 = np.max(dim2, axis=0)
        dist3 = np.max(dim3, axis=0)
        D2 = dist2 < r
        D3 = dist3 < r
        count2[i] = np.sum(D2)*(1.0/(N-dim))
        count3[i] = np.sum(D3)*(1.0/(N-dim))
        
    correl[0] = np.sum(count2)*(1.0/(N-dim))
    correl[1] = np.sum(count3)*(1.0/(N-dim))
    
    sampen = -1
    if correl[1] != 0:
        sampen_array = np.log(correl[0]/correl[1])
        sampen = sampen_array[0]
    
    return sampen

def vfib_features(EcgWin1, EcgWin2, Fs):
    EcgMax1  = -1
    count2_1 = -1
    tcsc_1   = -1
    vfleak_1 = -1
    sampen_1 = -1
    if EcgWin1.shape[0] > 0:    
        EcgMax1 = np.max(np.abs(EcgWin1))
        if (EcgMax1 < ECG_MAX and EcgMax1 > ECG_MIN):
            count2_1 = update_count2(EcgWin1,Fs)
            tcsc_1   = update_tcsc(EcgWin1,Fs)
            vfleak_1 = update_vfleak(EcgWin1)
            sampen_1 = update_sampen(EcgWin1)
            
    EcgMax2  = -1
    count2_2 = -1
    tcsc_2   = -1
    vfleak_2 = -1
    sampen_2 = -1
    if EcgWin2 is not None and EcgWin2.shape[0] > 0:        
        EcgMax2 = np.max(np.abs(EcgWin2))
        if (EcgMax2 < ECG_MAX and EcgMax2 > ECG_MIN):
            count2_2 = update_count2(EcgWin2,Fs)
            tcsc_2   = update_tcsc(EcgWin2,Fs)
            vfleak_2 = update_vfleak(EcgWin2)
            sampen_2 = update_sampen(EcgWin2)
        
    return [EcgMax1,count2_1,tcsc_1,vfleak_1,sampen_1,EcgMax2,count2_2,tcsc_2,vfleak_2,sampen_2]


def vfib_driver(ECG, atree, sn_mod=4, win_size=1024, WINDOW_TICKS=4096, Fs = 125., twolead=True):

    Visi_Fs = 500.0 # hz    
    WINDOW_TIME = float(WINDOW_TICKS)/Visi_Fs # seconds
    UPDATE_TICKS = WINDOW_TICKS

    # downsample
    temp = np.mod(ECG[:,0],sn_mod)
    ECG = ECG[temp == 0,:]

    features = []
    nwins = int(np.floor( (ECG[-1,0] - ECG[0,0])/UPDATE_TICKS ))
    for i in range(nwins):
        sn0 = ECG[0,0] + UPDATE_TICKS*i
        sn1 = sn0 + WINDOW_TICKS
        idx = (sn0 <= ECG[:,0]) * (sn1 > ECG[:,0])        
        f = vfib_features(ECG[idx,2], ECG[idx,3] if twolead else None, Fs)
        p = 0.
        is_vfib = 0
        for iv in atree.search(sn0, sn1):
            if 'VFIB' in iv.data:
                if sn0 < iv.begin:
                    p = (min(sn1,iv.end) - iv.begin)/float(WINDOW_TICKS)
                elif sn1 > iv.end:
                    p = ( iv.end - sn0 ) / float(WINDOW_TICKS)
                else:
                    p = 1.
                is_vfib = int(p >= 0.5)
                break        
        features.append( [sn0, sn1] + f + [p, is_vfib])
    features = np.array( features )

    return features
