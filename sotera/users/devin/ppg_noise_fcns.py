import numpy as np
import matplotlib.pyplot as plt
import sotera

def find_radio_on(IR):
    x = np.mod(IR[:,2],16)
    sn_radio_on = IR[x == 13,0]
    return sn_radio_on 


def find_radio_on_idx(ppg, sn_radio_on):
    idx_match = np.in1d(ppg[:,0], sn_radio_on)
    l_ppg = ppg.shape[0]
    idx_sqn = np.arange(l_ppg)
    radio_on_idx = idx_sqn[idx_match]
    return radio_on_idx


def find_peak_stop(ppg, peakIdx, peakSlope, peakType):
    
    MIN_NOISE_SLOPE = 29        
    HALF_PEAK_MIN = 11
    HALF_PEAK_MAX = 24
    
    if peakSlope >= MIN_NOISE_SLOPE:
        peakStopIdx = peakIdx + HALF_PEAK_MAX
    else:
        peakStopIdx = peakIdx
    
    startIdx = peakIdx + HALF_PEAK_MIN
    stopIdx  = peakIdx + HALF_PEAK_MAX
    if peakType == 1: # positive peak
        for j in range(startIdx,stopIdx):
            if (ppg[j,2] < ppg[j-1,2] and ppg[j,2] <= ppg[j+1,2] and ppg[j,2] <= ppg[j+3,2]):
                peakStopIdx = j
                break
    else: # negative peak
        for j in range(startIdx,stopIdx):
            if (ppg[j,2] > ppg[j-1,2] and ppg[j,2] >= ppg[j+1,2] and ppg[j,2] >= ppg[j+3,2]):
                peakStopIdx = j
                break
                
    return peakStopIdx 


def quantify_noise_spike(ppg, radio_idx):
    
    # initialize output array
    RadioNoiseData = np.zeros((1,6));
    
    # peak constants
    PEAK_WIN = 40; # sequence numbers
    PEAK_RADIUS = 6;
    
    # initialize max and min values
    ppg_radio = ppg[radio_idx,2];
    ppg_max = ppg_radio;
    ppg_min = ppg_radio;
    max_slope = 0;
    min_slope = 0;
    
    # find peak/valley and max/min slope
    for idx in range(radio_idx+1,radio_idx+PEAK_WIN+1):
        
        # find max/min 2nd derivative
        ppg_slope = (ppg[idx+1,2]-ppg[idx,2]) - (ppg[idx-1,2]-ppg[idx-2,2]);
        if(ppg_slope > max_slope):
            max_slope = ppg_slope
        elif(ppg_slope < min_slope):
            min_slope = ppg_slope
            
        # find peak/valley
        if (ppg[idx,2] >= ppg[idx-1,2] and ppg[idx,2] >= ppg[idx-PEAK_RADIUS,2] and
            ppg[idx,2] > ppg[idx+1,2] and ppg[idx,2] > ppg[idx+PEAK_RADIUS,2] and
            ppg[idx,2] > ppg_max):
            ppg_max = ppg[idx,2]
            max_idx = idx
        elif(ppg[idx,2] <= ppg[idx-1,2] and ppg[idx,2] <= ppg[idx-PEAK_RADIUS,2] and
             ppg[idx,2] < ppg[idx+1,2] and ppg[idx,2] < ppg[idx+PEAK_RADIUS,2] and
             ppg[idx,2] < ppg_min):
            ppg_min = ppg[idx,2]
            min_idx = idx
    
    peakPosHt = ppg_max-ppg_radio;
    peakNegHt = ppg_radio-ppg_min;
    peakSlope = np.max([max_slope,-1*min_slope])
    
    MIN_NOISE_SPIKE = 49 # 2.5mV*(65535.0/3300.0)
    
    if (peakPosHt >= peakNegHt) and (peakPosHt > MIN_NOISE_SPIKE):
        peakStopIdx = find_peak_stop(ppg, max_idx, peakSlope, 1)
        peakHalfHt = ppg_max - ppg[peakStopIdx,2]
        if (peakHalfHt >= 0.5*peakPosHt):       
            RadioNoiseData[0,0] =  ppg[radio_idx,0]  # start_sn
            RadioNoiseData[0,1] =  ppg[max_idx,0]    # peak_sn
            RadioNoiseData[0,2] =  ppg[peakStopIdx,0]# stop_sn
            RadioNoiseData[0,3] =  ppg[radio_idx,2]  # start_ppg
            RadioNoiseData[0,4] =  ppg[max_idx,2]    # peak_ppg
            RadioNoiseData[0,5] =  ppg[peakStopIdx,2]# stop_ppg
        
    elif (peakNegHt > MIN_NOISE_SPIKE):
        peakStopIdx = find_peak_stop(ppg, min_idx, peakSlope, -1)
        peakHalfHt = ppg[peakStopIdx,2] - ppg_min 
        if (peakHalfHt >= 0.5*peakNegHt):
            RadioNoiseData[0,0] =  ppg[radio_idx,0]  # start_sn
            RadioNoiseData[0,1] =  ppg[min_idx,0]    # peak_sn
            RadioNoiseData[0,2] =  ppg[peakStopIdx,0]# stop_sn
            RadioNoiseData[0,3] =  ppg[radio_idx,2]  # start_ppg
            RadioNoiseData[0,4] =  ppg[min_idx,2]    # peak_ppg
            RadioNoiseData[0,5] =  ppg[peakStopIdx,2]# stop_ppg
        

    return RadioNoiseData


def calculate_noise_density(NoiseData, winSize, snBlockStart, snBlockStop):
    
    Fs = 500 # Hz
    snWin = winSize*500
       
    SnUpdate = np.arange(snBlockStart,snBlockStop,snWin)
    numIts = len(SnUpdate)
    
    NoiseDensity = np.zeros((numIts-1,2))
    
    l_nd = NoiseData.shape[0]
    NoiseDataDensity = np.zeros((l_nd,1))
    
    for j in range(1,numIts):
        start_sn = SnUpdate[j-1]
        stop_sn  = SnUpdate[j]
        WinNoise = NoiseData[(NoiseData[:,0] > start_sn)*(NoiseData[:,0] <= stop_sn),:]
        NoiseDensity[j-1,0] = stop_sn
        NoiseDensity[j-1,1] = float(WinNoise.shape[0])/winSize
               
    return NoiseDensity


def add_noise_density(NoiseDensity, x):
    
    # add a noise density value for each value in x
    l_x = x.shape[0]
    y = np.zeros((l_x,1))

    numIts = NoiseDensity.shape[0]
    for j in range(1,numIts):
        start_sn = NoiseDensity[j-1,0]
        stop_sn  = NoiseDensity[j,0]
                
        # label all of the spikes with the windowed density
        y[(x[:,0] > start_sn)*(x[:,0] <= stop_sn)] = NoiseDensity[j,1]
        
    z = np.hstack((x,y))
    
    return z


def analyze_radio_noise(NoiseData, NoiseDensity, snBlockStart, snBlockStop):
    
    # Function to calculate wi-fi noise statistsics
    # *************************************************************
    #
    # Noise amplitude stats based on the Tukey Box and Whisker plot
    #
    # *************************************************************
    
    MIN_DENSITY = 1 # spike/sec
    AMP_THRESH = 6 # mV
    FALSE_POSITIVE_RATE = 3 # 3 spikes/minute (empirically determined)
    K_PPG = (3300.0/65535.0) # mV
    
    # screen data from analysis when the density is less then a specified threshold
    NoiseData = NoiseData[NoiseData[:,6] > MIN_DENSITY]
    
    # initialize output
    NoiseStats = np.zeros((1,8))
    
    # eliminate files in which the spike count is very low
    # and the file likely only contains misclassified artifact
    NumSpikes = NoiseData.shape[0]
    FileTimeMin = (snBlockStop-snBlockStart)/(500.0*60) # in minutes
    NumFalsePosSpikes = FileTimeMin*FALSE_POSITIVE_RATE
    
    if NumFalsePosSpikes >= NumSpikes:
        return NoiseStats
    else:
        # median noise amplitude
        NoiseAmp = K_PPG*(np.abs(NoiseData[:,4] - NoiseData[:,3]))
        NoiseStats[0,0] = np.median(NoiseAmp)
    
        # upper quartile noise amplitude
        x1 = NoiseAmp[NoiseAmp > NoiseStats[0,0]]
        NoiseStats[0,1] = (np.median(x1))
    
        # lower quartile noise amplitude
        x2 = NoiseAmp[NoiseAmp < NoiseStats[0,0]]
        NoiseStats[0,2] = (np.median(x2))
    
        # upper whisker noise amplitude
        IQR = NoiseStats[0,1]-NoiseStats[0,2]
        upperbound = 1.5*IQR + NoiseStats[0,1]
        x3 = NoiseAmp[NoiseAmp <= upperbound]
        NoiseStats[0,3] = np.max(x3)
    
        # quantity of significant noise spikes   
        x3 = NoiseAmp[NoiseAmp >= AMP_THRESH];
        NoiseStats[0,4] = x3.shape[0]
    
        # noise density
        x4 = NoiseDensity[NoiseDensity[:,1] >= 1,1]
        x5 = NoiseDensity[NoiseDensity[:,1] >= 2,1]
        x6 = NoiseDensity[NoiseDensity[:,1] >= 3,1]
        den = float(NoiseDensity.shape[0])
        NoiseStats[0,5] = 100.0*x4.shape[0]/den # Pct time > 1 spike/sec
        NoiseStats[0,6] = 100.0*x5.shape[0]/den # Pct time > 2 spikes/sec
        NoiseStats[0,7] = 100.0*x6.shape[0]/den # Pct time > 3 spikes/sec
    
    return NoiseStats


def analyze_vitals_with_high_noise_density(x):
    
    MIN_DENSITY = 1 # noise spikes/sec
    x = x[x[:,3] >= MIN_DENSITY,:]
    NoiseStats = -1*np.ones((1,3))
    
    if x.shape[0] > 0:
        y = x[x[:,2] > 0,:]
        NoiseStats[0,0] = 100.0*float(y.shape[0])/x.shape[0]
        if y.shape[0] > 0:
            NoiseStats[0,1] = np.median(y[:,2])
            NoiseStats[0,2] = np.max(y[:,2])

    return NoiseStats       


def print_noise_stats(NoiseStats, SignalString):
    print('\r')
    print(SignalString + ' -------------------------------')
    print('Amplitude (mV)')
    tempStr = str(NoiseStats[0,2])
    print('Lower Quartile = ' + tempStr[0:4])
    tempStr = str(NoiseStats[0,0])
    print('Median         = ' + tempStr[0:4])
    tempStr = str(NoiseStats[0,1])
    print('Upper Quartile = ' + tempStr[0:4])
    tempStr = str(NoiseStats[0,3])
    print('Upper Limit    = ' + tempStr[0:4])
    print('\r')
    print('Quantity (#)')
    print('Number of Spikes >= 6 mV = ' + str(NoiseStats[0,4]))
    tempStr = str(NoiseStats[0,5])
    print('\r')
    print('Density (%)')
    print('Monitoring time > 1 spike/sec = ' + tempStr[0:4])
    tempStr = str(NoiseStats[0,6])
    print('Monitoring time > 2 spike/sec = ' + tempStr[0:4])
    tempStr = str(NoiseStats[0,7])
    print('Monitoring time > 3 spike/sec = ' + tempStr[0:4])



def ppg_noise_analysis_by_hid_and_block_number(hid, block_number, print_data = False, plot_data = False):

    # returns a 1 by 27 array of noise stats for the IR and Red DC PPG signals for each block
    # requires pwd software version # 2.3.57 or later 
    
    # call Isaac's api function to load ppg waveform data
    data = sotera.io.load_session_data(hid, block_number)
    
    # Analysis requires the DC waveforms
    if 'IR_DC' in data:
        ir_filt = data['IR_FILT']
        ir_dc = data['IR_DC']
        red_dc = data['RED_DC']
        spo2 = data['SPO2']
        pr = data['PR']
    else:
        print('Cannot perform noise analysis for HID: ' + str(hid) + ' Block Number: ' + str(block_number))
        return

    # Find the sequence numbers corresponding to the start of the wi-fi radio transmission
    sn_radio_on = find_radio_on(ir_filt)
    # Analysis requires the Radio On bit in the filtered waveform
    if sn_radio_on.shape[0] < 1:
        print('Cannot perform noise analysis for HID: ' + str(hid) + ' Block Number: ' + str(block_number))
        return
    # Find the array indices for red and ir dc corresponding to the start of wi-fi radio transmisson    
    ir_radio_idx = find_radio_on_idx(ir_dc, sn_radio_on)
    red_radio_idx = find_radio_on_idx(red_dc, sn_radio_on)
    num_red = len(red_radio_idx)
    num_ir  = len(ir_radio_idx)
    num_radio = np.max([num_red, num_ir]) - 2
    if num_radio < 240:
        return


    # Identify all of the wi-fi noise spikes in the ppg data
    IrNoiseData  = np.zeros((num_ir,6))
    RedNoiseData = np.zeros((num_red,6)) 
    for i in range(1,num_radio):
        if (i < num_ir):
            idx = ir_radio_idx[i]
            radio_idx = 16
            start_idx = idx-radio_idx+1
            stop_idx  = idx + 128
            ir  = ir_dc[start_idx:stop_idx,:]
            IrNoiseData[i,:] = quantify_noise_spike(ir, radio_idx)
        if (i < num_red):
            idx = red_radio_idx[i]
            radio_idx = 16
            start_idx = idx-radio_idx+1
            stop_idx  = idx + 128
            red  = red_dc[start_idx:stop_idx,:]
            RedNoiseData[i,:] = quantify_noise_spike(red, radio_idx)    
    # Remove rows without any noise
    IrNoiseData = IrNoiseData[IrNoiseData[:,0] > 0,:]
    RedNoiseData = RedNoiseData[RedNoiseData[:,0] > 0,:]

    # Estimate file start/stop
    snBlockStart = ir_dc[0,0]
    snBlockStop  = ir_dc[-1,0]

    # Calculate noise density
    WIN_SIZE = 30 # seconds
    IrNoiseDensity  = calculate_noise_density(IrNoiseData, WIN_SIZE, snBlockStart, snBlockStop)
    RedNoiseDensity = calculate_noise_density(RedNoiseData, WIN_SIZE, snBlockStart, snBlockStop)
    
    # Add a noise density estimate to every parameter value
    IrNoiseData = add_noise_density(IrNoiseDensity, IrNoiseData)
    RedNoiseData = add_noise_density(RedNoiseDensity, RedNoiseData)
    pr = add_noise_density(IrNoiseDensity, pr)
    spo2 = add_noise_density(IrNoiseDensity, spo2)
    
    # Calculate noise stats
    IrNoiseStats  = analyze_radio_noise(IrNoiseData, IrNoiseDensity, snBlockStart, snBlockStop)
    RedNoiseStats = analyze_radio_noise(RedNoiseData, RedNoiseDensity, snBlockStart, snBlockStop)
    
    # Calculate vital sign stats in regions with high noise density
    PrNoiseStats   = analyze_vitals_with_high_noise_density(pr)
    SpO2NoiseStats = analyze_vitals_with_high_noise_density(spo2)
    if 'HR' in data:
        hr = data['HR']
        hr = add_noise_density(IrNoiseDensity, hr)
        HrNoiseStats = analyze_vitals_with_high_noise_density(hr)
    else:
        HrNoiseStats = -1*np.ones((1,3))
    
    if print_data:
        print_noise_stats(IrNoiseStats, 'IR PPG')
        print_noise_stats(RedNoiseStats, 'RED PPG')
    
    if plot_data:
        k_ppg = 3300.0/65535.0
        plt.figure()
        ax1 = plt.subplot(3,1,1)
        plt.plot(ir_dc[:,0],k_ppg*ir_dc[:,2],'k')
        plt.plot(IrNoiseData[:,0], k_ppg*IrNoiseData[:,3],'go')
        plt.plot(IrNoiseData[:,1], k_ppg*IrNoiseData[:,4],'bo')
        plt.plot(IrNoiseData[:,2], k_ppg*IrNoiseData[:,5],'ro')
        plt.plot(red_dc[:,0],k_ppg*red_dc[:,2],'r')
        plt.plot(RedNoiseData[:,0], k_ppg*RedNoiseData[:,3],'go')
        plt.plot(RedNoiseData[:,1], k_ppg*RedNoiseData[:,4],'bo')
        plt.plot(RedNoiseData[:,2], k_ppg*RedNoiseData[:,5],'ro')
        plt.xlabel('Sequence Number')
        plt.ylabel('PPG DC (mV)')
        plt.title(hid)

        ax2 = plt.subplot(3,1,2, sharex=ax1)
        plt.plot(IrNoiseDensity[:,0], IrNoiseDensity[:,1],'k')
        plt.plot(RedNoiseDensity[:,0], RedNoiseDensity[:,1],'r')
        plt.xlabel('Sequence Number')
        plt.ylabel('Noise Density (spikes/sec)')
        plt.ylim([0,4])
        
        ax3 = plt.subplot(3,1,3, sharex=ax1)
        RED_AMP  = np.abs(k_ppg*(RedNoiseData[:,4]-RedNoiseData[:,3]))
        IR_AMP = np.abs(k_ppg*(IrNoiseData[:,4]-IrNoiseData[:,3]))
        plt.plot(IrNoiseData[:,0],IR_AMP, 'k.')
        plt.plot(RedNoiseData[:,1], RED_AMP,'r.')
        plt.xlabel('Sequence Number')
        plt.ylabel('Noise Amplitude (mV)')
        plt.ylim([0,50])
    
    SnBlock = np.ones((1,2))
    SnBlock[0,0] = snBlockStart
    SnBlock[0,1] = snBlockStop
    
    PpgNoiseStats = np.hstack((IrNoiseStats,RedNoiseStats))
    VitalSignNoiseStats = np.hstack((SpO2NoiseStats,PrNoiseStats,HrNoiseStats))
    WiFiNoiseStats = np.hstack((SnBlock,PpgNoiseStats,VitalSignNoiseStats))

    rdict = { 'hid':hid, 'block_number':block_number }
    elements = ('sn_start','sn_stop','ir_med','ir_uqa','ir_lqa','ir_limit','ir_quant','ir_nd1','ir_nd2','ir_nd3','red_med',
               'red_uqa','red_lqa','red_limit','red_quant','red_nd1','red_nd2','red_nd3','spo2_disp',
               'spo2_med','spo2_max','pr_disp','pr_med','pr_max','hr_disp','hr_med','hr_max')
    for i,key in enumerate(elements):
        rdict[key] = WiFiNoiseStats[0,i]

    return rdict
