import scipy.signal as signal
import numpy as np

def update_kurtosis(ecg):
    l_ecg = ecg.shape[0]
    x = (ecg - np.mean(ecg))
    num = x*x*x*x
    den = np.var(ecg)*np.var(ecg)
    kurtss = np.sum(num/den)/l_ecg
    
    return kurtss


def update_count2(ecg):
    # assumes sample rate = 250Hz
    Fs = 250
    
    # bandpass filter the waveform
    N  = 1    # Filter order
    Wn = np.array([2.0*13.0/Fs,  2.0*16.5/Fs]) # -3dB cutoff frequencies
    B, A = signal.iirfilter(N, Wn, btype="bandpass", ftype="butter")
    ecg_filt = signal.lfilter(B, A, ecg)   
    
    # mean and max are calculated for non-overlapping windows of 256 samples (~1.024 seconds) in the buffer
    l_ecg = ecg_filt.shape[0]
    l_win = 256 # ~1.024 seconds
    win_bounds = np.arange(0,l_ecg+1,l_win)
    num_bounds = win_bounds.shape[0]
    count2 = 0
    for i in range(0,num_bounds-1):
        y = np.abs(ecg_filt[win_bounds[i]:win_bounds[i+1]-1])
        mean_y = np.mean(y)
        SamplesAboveThresh = y[y > mean_y]
        count2 = count2 + SamplesAboveThresh.shape[0]
        SamplesAboveThresh = 0
        
    return count2


def update_tcsc(ecg):
    #assumes sample rate  = 250Hz
    Fs = 250
    
    # update the threshold crossing sample count
    # note: the paper recommends that the base calculation be performed on
    #       3 second over-lapping windows updated every 1 second
    
    V0 = 0.2 # threshold
    
    Ls = 3*Fs
    Le = ecg.shape[0]
    Lu = 1*Fs
    
    NumIts = np.round((Le-Ls)/Lu) + 1
    
    # remove the mean
    ecg = ecg - np.mean(ecg)
    
    # normalize the waveform using the max value
    max_ecg = np.max(ecg)
    ecg = (1/max_ecg)*ecg
    
    # pre-allocate
    N = np.zeros((NumIts,1))
      
    for i in range(0, Ls-1):
        for j in range(0, NumIts):
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
   
    for i in range(0,dim+1):
        dataMat[i,:] = data[i:N-dim+i]
    
    # run both dimensions simultaneously
    m = dim + 1
    count2 = np.zeros((N-m,1))
    count3 = np.zeros((N-m,1))
    tempMat = dataMat[0:m,:] 
        
    for i in range(0,N-m):
        
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


def VfibFeatures(EcgWin):
    # vfib_vectors = [kurtosis, count2, tcsc, vfleak, sampen]
    
    # only calcualte features for windows without missing data
    # and if the ECG leads are "on patient"

    k_ecg = 4.76837158203125e-05 # ViSi scaling
    ECG_LIMIT = 10.0/k_ecg # 10 mV
    vfib_vector = -1*np.ones((1,5))
    
    
    if (EcgWin.shape[0] == 2048):
        EcgMax = np.max(np.abs(EcgWin))
        if (EcgMax < ECG_LIMIT):
            vfib_vector[0,0] = update_kurtosis(EcgWin)
            vfib_vector[0,1] = update_count2(EcgWin)
            vfib_vector[0,2] = update_tcsc(EcgWin)
            vfib_vector[0,3] = update_vfleak(EcgWin)
            vfib_vector[0,4] = update_sampen(EcgWin)
            
    return vfib_vector