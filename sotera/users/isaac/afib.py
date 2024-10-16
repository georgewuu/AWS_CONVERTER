import numpy as np
from sklearn.ensemble import AdaBoostClassifier
import sotera.io

def update_sampen(data, dim=2, k_std=0.2):

    r = k_std*np.std(data)
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
        x2 = np.ones((m,N-m-i))
        y  = tempMat[:,i:i+1]
        for k in range(0,m):
            x2[k,:] = y[k]
        dim2 = np.abs(x1[0:dim,:]-x2[0:dim,:])
        dim3 = np.abs(x1-x2)
        dist2 = np.max(dim2, axis=0)
        dist3 = np.max(dim3, axis=0)
        D2 = dist2 < r
        D3 = dist3 < r
        count2[i] = np.sum(D2)*(1.0/(N-dim))
        count3[i] = np.sum(D3)*(1.0/(N-dim))
        
    correl[0] = np.sum(count2)*(1.0/(N-dim))
    correl[1] = np.sum(count3)*(1.0/(N-dim))
    
    sampen = -10
    if correl[1] != 0:
        sampen_array = np.log(correl[0]/correl[1])
        sampen = sampen_array[0]
    
    return sampen, r

def update_rmssd(dRR_Win, RR_Win):
    
    # root mean square of successive differences
    l = len(dRR_Win)
    mu = np.mean(RR_Win)
    
    x = np.power(dRR_Win,2)
    y = np.sum(x)
    z = float(y)/float(l-1)
    
    rmssd = np.sqrt(z)/mu 

    return rmssd

def update_tpr(RR_Win):
    
    # turning point ratio of three successive RR intervals
    l = len(RR_Win)
    
    tpr_count = 0
    
    for i in range(1,l-1):
        # determine if RR[i] is a turning point
        a1 = RR_Win[i-1]
        a2 = RR_Win[i]
        a3 = RR_Win[i+1]
        if (a2 > a1 and a2 > a3):
            tpr_count += 1
        elif (a2 < a1 and a2 < a3):
            tpr_count += 1
    
    # based on paper
    tpr_expected_mean  = (2.0*(l-2)-4.0)/3.0
    tpr_expected_std = np.sqrt((16.0*(l-2)-29.0)/90.0)
    
    tpr_delta = tpr_count-tpr_expected_mean
    tpr = tpr_count #np.power(tpr_delta,2)
    
    return tpr
     
def cosine_similarity(ecgA, ecgB):
    
    if ecgA.shape[0] == ecgB.shape[0]:
        Anorm = np.linalg.norm(ecgA)
        Bnorm = np.linalg.norm(ecgB)
        ecgA = ecgA.transpose()
        Z = np.dot(ecgA,ecgB)
        cosim = Z[0][0]/(Anorm*Bnorm)
    else:
        cosim = -1
        
    return cosim


def classify_afib_by_hid_and_block_number(data, ModelName, AFIB_UPDATE_RATE = 30, COSIM_THRESHOLD = 0.6, COSIM_WIN = 20, pgsql_ = None):
    # calculate Afib features for a defined number of beats
    # RHYTHM_PKT = [ut update, classification, ut start]
    
    clf, params  = sotera.io.cloud.load_sklearn_model(ModelName, pgsql_)
    
    # data in parameters packet
    num_beats = params['num_beats']
    sampen_dim = params['sampen_dim']
    sampen_scale = params['sampen_scale']
    features = params['features']
    mu = [features['sampen']['mean'],features['rmmsd']['mean']] #,features['tpr']['mean']]
    sigma = [features['sampen']['stDev'],features['rmmsd']['stDev']] #,features['tpr']['stDev']]
    
    # RHYTHM_PKT = [ut rhythm_code rhythm_start ut_start ut_stop rmssd sampen tpr]
    AFib_code = 1 #2008 #(vfib/vtach)
    XX_code   = -1 #10001 # (XX - unable to make a classification)
    SQI_code = -2 # data screened by cosine similarity
    No_Rhythm_code = 0 #10002 #(Unmatched class - successful classification but did not classify as afib)
    RHYTHM_PKT = -1*np.ones((1,8))
    
    # set maximum look back
    MIN_HR = 40.0 # beats/min
    MAX_WINDOW_TIME = np.round(num_beats*(60.0/MIN_HR)) + 1 # in seconds
    
    # api function to load data
    # **************************************************************************************************
    #data = sotera.io.load_session_data(hid, block_number, variable_names=['HR_BEAT', 'HR', 'ECG', 'ECG_I', 'ECG_II'], pgsql_ = pgsql_)
    
    if 'HR_BEAT' in data:
        HR_BEAT = data['HR_BEAT']
        HR = data['HR']
        ECG_II = data['ECG_II']
    elif 'HR_BEATS' in data:
        HR_BEAT = data['HR_BEATS']
    else:
        #print('hid: ' + str(hid) + ' does not contain HR_BEAT')
        return RHYTHM_PKT    
    
    if HR_BEAT.shape[0] < 500:
        #print('hid: ' + str(hid) + ' contains < 500 beats')
        return RHYTHM_PKT 
    else:    
        #RR intervals
        RR = HR_BEAT
        RR = RR[RR[:,2] > 0,:]
        RR = RR[RR[:,2] < 1000,:]
        #dRR intervals
        NumRows = RR.shape[0]
        dRR = np.zeros((NumRows,3))
        dRR[:,0] = RR[:,0]
        dRR[:,1] = RR[:,1]
        dRR[0,2] = 0
        dRR[1:(NumRows+1),2] = np.diff(RR[:,2])
    
    # loop through the RR data and add a screening code into column 3 based on Cosine Similarity of the ECG waveform
    l_beats = RR.shape[0]
    l_ecg = ECG_II.shape[0]

    COSIM_PKT = []
    for i in range(0,l_beats):
        idxII = np.searchsorted(ECG_II[:,0], RR[i,0])
        RR[i,4] = idxII
        if(idxII < l_ecg and RR[i,0] == ECG_II[idxII,0] and i > 0 and idxII > COSIM_WIN and RR[i-1,4] > COSIM_WIN):
            
            idx0 = int(RR[i,4]-COSIM_WIN)
            idx1 = int(RR[i,4]+COSIM_WIN)
            ecgA = ECG_II[idx0:idx1,2:3]
            
            idx0 = int(RR[i-1,4]-COSIM_WIN)
            idx1 = int(RR[i-1,4]+COSIM_WIN)
            ecgB = ECG_II[idx0:idx1,2:3]
            
            CoSim = cosine_similarity(ecgA, ecgB)

            COSIM_PKT.append( [ RR[i,0], CoSim] )
            if CoSim >= COSIM_THRESHOLD:
                RR[i,3] = 1
            else:
                RR[i,3] = 0
        else:
            RR[i,3] = 0
            #print("No Match for beat sn in ECG II")
    
    COSIM_PKT = np.array(COSIM_PKT)
    
    ut_update = RR[num_beats,1] + AFIB_UPDATE_RATE
    num_updates = int(np.fix((RR[-1,1] - ut_update)/AFIB_UPDATE_RATE))
    
    RHYTHM_PKT = -10*np.ones((num_updates,9))
    #x = -1*np.ones((1,3))
    x = -1*np.ones((1,2))
    
    # initalize the first rhythm packet
    RHYTHM_PKT[0,0] = ut_update
    RHYTHM_PKT[0,1] = No_Rhythm_code
    RHYTHM_PKT[0,2] = ut_update

    for i in range(1,num_updates):
        
        ut_update = ut_update +  AFIB_UPDATE_RATE
        RHYTHM_PKT[i,0] = ut_update
        RHYTHM_PKT[i,1] = No_Rhythm_code
        
        stop_idx = np.searchsorted(RR[:,1],ut_update)
        start_idx = np.searchsorted(RR[:,1],(ut_update-MAX_WINDOW_TIME))
        RHYTHM_PKT[i,7] = RR[start_idx:stop_idx,:].shape[0]
        
        ut_start = RR[start_idx,1]
        ut_stop  = RR[stop_idx,1]

        # No data in window: rhythm = "XX"
        if (stop_idx == start_idx):
            RHYTHM_PKT[i,1] = XX_code
            RHYTHM_PKT[i,2] = ut_update
            if (RHYTHM_PKT[i,1] == RHYTHM_PKT[i-1,1]):
                RHYTHM_PKT[i,2] = RHYTHM_PKT[i-1,2]
            print("no data in window")
            continue
            
        # Leads-off in window: rhythm = "XX"    
        if (0 in RR[start_idx:stop_idx,5]):
            RHYTHM_PKT[i,1] = XX_code
            RHYTHM_PKT[i,2] = ut_update
            if (RHYTHM_PKT[i,1] == RHYTHM_PKT[i-1,1]):
                RHYTHM_PKT[i,2] = RHYTHM_PKT[i-1,2]
            print("leads-off")
            continue
            
        # Missing data in window: rhythm = "XX"
        HrWin = HR[(HR[:,1]>=ut_start)*(HR[:,1]<=ut_stop),1]
        WinDiff = (ut_stop-ut_start)-HrWin.shape[0]
        if(WinDiff > 5):
            RHYTHM_PKT[i,1] = XX_code
            RHYTHM_PKT[i,2] = ut_update
            if (RHYTHM_PKT[i,1] == RHYTHM_PKT[i-1,1]):
                RHYTHM_PKT[i,2] = RHYTHM_PKT[i-1,2]
            print ("Missing data in window")
            continue
        
         # No new data in window: rhythm = "XX"
        if(ut_update-ut_stop) > AFIB_UPDATE_RATE:
            RHYTHM_PKT[i,1] = XX_code
            RHYTHM_PKT[i,2] = ut_update
            if (RHYTHM_PKT[i,1] == RHYTHM_PKT[i-1,1]):
                RHYTHM_PKT[i,2] = RHYTHM_PKT[i-1,2]
            print ("No new data in window")
            continue
            
        # Update Classification if Min HR condition is met
        if(stop_idx - start_idx) >= num_beats:    
            
            RR_Win  = RR[start_idx:stop_idx,:]
            dRR_Win = dRR[start_idx:stop_idx,:]
            
            # use cosine similarity screening
            dRR_Win = dRR_Win[RR_Win[:,3] > 0,:]
            RR_Win  = RR_Win[RR_Win[:,3] > 0,:]
            
            
            win_beats = RR_Win.shape[0]

            if win_beats >= num_beats:
            
                idx1 = int(win_beats)
                idx0 = int(win_beats-num_beats)
                
                RR_input  = RR_Win[idx0:idx1,2]
                dRR_input = dRR_Win[idx0:idx1,2]
                #print(RR_input.shape)
                
                x[0,0], r = update_sampen(dRR_input, sampen_dim, sampen_scale)
                x[0,1] = update_rmssd(dRR_input, RR_input)
                #x[0,2] = update_tpr(RR_input)
                RHYTHM_PKT[i,8] = r

                if(x[0,1] > 0):
                    for k in range(0,len(mu)):
                        x[0,k] = (1.0/float(sigma[k]))*(float(x[0,k]-mu[k]))
                    RHYTHM_PKT[i,3] = ut_start
                    RHYTHM_PKT[i,4] = ut_stop
                    RHYTHM_PKT[i,5] = x[0,0]
                    RHYTHM_PKT[i,6] = x[0,1]

                    y = clf.predict(x)
                    if (y == 1):
                        RHYTHM_PKT[i,1] = AFib_code
                # Hold previous Rhythm for sampen = -1
                else:
                    RHYTHM_PKT[i,1] = XX_code
            else:
                RHYTHM_PKT[i,1] = SQI_code
                
            RHYTHM_PKT[i,2] = ut_update            
            if (RHYTHM_PKT[i,1] == RHYTHM_PKT[i-1,1]):
                RHYTHM_PKT[i,2] = RHYTHM_PKT[i-1,2]
    
    return RHYTHM_PKT, COSIM_PKT
