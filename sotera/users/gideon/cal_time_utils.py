import numpy as np

def CalculateCalTimesFcn(start_times_stamps,CNIBP_CAL_PKT, SENSOR_CONNECTION, BP, POSTURE_PKT, ARM_HT_PKT):
    ANGLEV_UPDATE_THRESH = 12  #  degrees
    ANGLEH_UPDATE_THRESH = 25  #  degrees
    ELBOW_HT_UPDATE_THRESH = 8 #  cm
    WRIST_HT_UPDATE_THRESH = 8 #  cm
    
    # intialize CalTime
    CalTime = []
    
    l_cnibp = CNIBP_CAL_PKT.shape[0]
    
    # convert BP start inflation to unix time
    CNIBP_CAL_PKT[:,2] = CNIBP_CAL_PKT[:,1] - (1/500.0)*(CNIBP_CAL_PKT[:,0]-CNIBP_CAL_PKT[:,2]);
    
    # identify cuff connections
    SENSOR_CONNECTION_conn_bit = SENSOR_CONNECTION[:,2].astype('uint16')
    cuff_bit = np.bitwise_and(SENSOR_CONNECTION_conn_bit,2)
    cuff_connect = SENSOR_CONNECTION[cuff_bit == 2,:]
    
    WINDOW = 15*60 # seconds
    m = 0
    
    for i,cnibp in enumerate(CNIBP_CAL_PKT):
    
        t_start_nibp = cnibp[2]
        t_stop = cnibp[1]
        
        # find all nibp_connect and bp
        lookback = t_start_nibp - WINDOW;
        t_bp = BP[np.logical_and(BP[:,1] >= lookback, BP[:,1] < t_start_nibp),1]
        t_connect = cuff_connect[np.logical_and(cuff_connect[:,1] >= lookback, cuff_connect[:,1] < t_start_nibp),1]
                  
        posture = POSTURE_PKT[np.logical_and(POSTURE_PKT[:,1] >= lookback, POSTURE_PKT[:,1] < t_start_nibp),:]
        arm_height = ARM_HT_PKT[np.logical_and(ARM_HT_PKT[:,1] >= lookback, ARM_HT_PKT[:,1] < t_start_nibp),:]
        
        device_swap=start_times_stamps[(start_times_stamps.timestamps>lookback)&(start_times_stamps.timestamps<t_start_nibp)]
        
        if device_swap.empty:
            device_swap_start_time=0
        else:
            device_swap_start_time=device_swap.timestamps.iloc[0]
            
        
        if not posture.shape[0]:
           last_posture = 0
        else:
           last_posture = check_position(posture,ANGLEV_UPDATE_THRESH,ANGLEH_UPDATE_THRESH)
                          
        if not arm_height.shape[0]:
            last_arm_height = 0
        else:
            last_arm_height = check_position(arm_height,ELBOW_HT_UPDATE_THRESH,WRIST_HT_UPDATE_THRESH)
                  
        if not t_bp.shape[0]:
            last_bp = 0
        else:
            last_bp = t_bp[-1]
                  
        if not t_connect.shape[0]:
            first_connect = 0
        else:
            first_connect = t_connect[0]
    
        t_start = np.max([device_swap_start_time,first_connect,last_bp,last_posture,last_arm_height])
                  
        if t_start > 0:
#             CalTime.append([t_start,t_stop,t_stop - t_start,CNIBP_CAL_PKT[i,3],first_connect,
#                             last_posture,last_arm_height,last_bp])
            CalTime.append({'nibp_inflation_start':t_start_nibp,
                            'pie_filling_time':t_start_nibp-t_start, 
                            'nibp_inflation_time':t_stop - t_start_nibp, 
                            })
        else:
            CalTime.append({'nibp_inflation_start':t_start_nibp,
                            'pie_filling_time':-1, 
                            'nibp_inflation_time':t_stop - t_start_nibp, 
                            })
                  
    return CalTime
                  
def check_position(position, thresh1, thresh2):

    nibp_position1 = position[-1,3]
    nibp_position2 = position[-1,4]

    diff1 = np.abs(position[:,3] - nibp_position1)
    diff2 = np.abs(position[:,4] - nibp_position2)

    if np.max(diff1) < thresh1:
        last_position1 = 0
    else:
        l = position.shape[0]
        for i in range(l):
            if np.abs(nibp_position1-position[l-i-1,3]) >= thresh1:
               last_position1 = position[l-i-1,1]
               break

    if np.max(diff2) < thresh2:
        last_position2 = 0
    else:
        l = position.shape[0]
        for i in range(l):
            if np.abs(nibp_position2-position[l-i-1,4]) >= thresh2:
               last_position2 = position[l-i-1,1]
               break

    last_position = np.max([last_position1,last_position2])
                  
    return last_position

def get_cnibp_map_change_alerts(data,delay,percent_threshold=.3,missing_threshold=60):
    """ calculates number of alerts and time in alert for 'MAP change' alert """
    # extract unix time and MAP from packets
    try:
        CNIBP = data['CNIBP'][:,(1,4)]                   
        CNIBP_CAL_PKT = data['CNIBP_CAL_PKT'][:,(1,6)]
    except:
        return 0,0
    
    num_alerts_over_delay = 0 
    time_in_alert_over_delay = 0.0
    
    for i,row in enumerate(CNIBP_CAL_PKT):
        if i < CNIBP_CAL_PKT.shape[0]-1:
            cnibpChunk = CNIBP[np.logical_and(CNIBP[:,0]>=CNIBP_CAL_PKT[i,0],CNIBP[:,0]<CNIBP_CAL_PKT[i+1,0])]
        else:
            cnibpChunk = CNIBP[CNIBP[:,0]>=CNIBP_CAL_PKT[i,0]]

        #delay = 100    # (seconds)
        #percent_threshold = .3 #percentage threshold MAP can drift and still be ok
        #missing_threshold = 60 # (seconds) number of seconds that we assume missing data equals last seen value

        upper_rail = row[1]*(1.0+percent_threshold)
        lower_rail = row[1]*(1.0-percent_threshold)

        # map all true XX states to MAP
        cnibpChunk[cnibpChunk[:,1]==-1,1] = row[1]    
        cnibpChunk[cnibpChunk[:,1]==-2,1] = row[1]
        cnibpChunk[cnibpChunk[:,1]==-6,1] = row[1]
        cnibpChunk[cnibpChunk[:,1]==-7,1] = row[1]
        cnibpChunk[cnibpChunk[:,1]==-8,1] = row[1]
        cnibpChunk[cnibpChunk[:,1]==-9,1] = row[1]
        cnibpChunk[cnibpChunk[:,1]==-10,1] = row[1]

        cnibpChunk[cnibpChunk[:,1]==-4,1] = 240   #map ++ state to upper sys limit
        cnibpChunk[cnibpChunk[:,1]==-5,1] = 40    #map -- state to lower dia limit

        # add in data if missing data lasts more than missing_threshold seconds
        times = np.diff(cnibpChunk[:,0])
        for i,time in enumerate(times):
            if time > missing_threshold:
                cnibpChunk = np.insert(cnibpChunk,i+1,[cnibpChunk[i,0]+missing_threshold,row[1]],0)

        alert_idxs = np.logical_or(cnibpChunk[:,1]>=upper_rail,cnibpChunk[:,1]<=lower_rail)
        num_alert_idxs = np.sum(alert_idxs)
        if num_alert_idxs > 0:
            #normal_idxs = ~alert_idxs #np.logical_and(cnibpChunk[:,1]>lower_rail,cnibpChunk[:,1]<upper_rail)
            first_alert_idx = np.nonzero(alert_idxs)[0][0]
            transition_idxs = np.nonzero(np.diff(alert_idxs))[0]+1

            if transition_idxs.shape[0] > 0:
                if first_alert_idx==transition_idxs[0]:  # normal case
                    alert_on_times = cnibpChunk[transition_idxs[::2],0]
                    alert_off_times = cnibpChunk[transition_idxs[1::2],0]
                else:                             # if file started off in alarm state
                    alert_on_times = np.concatenate([np.array([cnibpChunk[0,0]]),cnibpChunk[transition_idxs[1::2],0]])
                    alert_off_times = cnibpChunk[transition_idxs[::2],0]
                    
                if alert_on_times.shape != alert_off_times.shape:  #if alert goes into EOF
                    #print alert_off_times.shape,type(alert_on_times),type(cnibpChunk[-1,0])
                    alert_off_times = np.concatenate([alert_off_times,np.array([cnibpChunk[-1,0]])])
                alert_durations = alert_off_times - alert_on_times
                alert_over_delay_durations = alert_durations[alert_durations>=delay]
                num_alerts_over_delay += alert_over_delay_durations.shape[0]
                time_in_alert_over_delay += np.sum(alert_over_delay_durations-delay)
            
    return num_alerts_over_delay,time_in_alert_over_delay

def get_cnibp_lost_alerts(data,delay,missing_threshold=60):
    """ calculates number of alerts and time in alert for CNIBP 'MAP LOST' alert """
    # extract unix time and MAP from packets
    try:
        CNIBP = data['CNIBP'][:,(1,4)]                   
    except:
        return 0,0

    CNIBP[CNIBP[:,1]==-4,1] = 240   #map ++ state to upper sys limit
    CNIBP[CNIBP[:,1]==-5,1] = 40    #map -- state to lower dia limit

    # add in data if missing data lasts more than missing_threshold seconds
    times = np.diff(CNIBP[:,0])
    for i,time in enumerate(times):
        if time > missing_threshold:
            CNIBP = np.insert(CNIBP,i+1,[CNIBP[i,0]+missing_threshold,100],0)

    xx_idxs = CNIBP[:,1]<0
    num_xx_idxs = np.sum(xx_idxs)

    num_xx_over_delay = 0
    time_in_xx_over_delay = 0
    
    if num_xx_idxs > 0:
        first_xx_idx = np.nonzero(xx_idxs)[0][0]
        transition_idxs = np.nonzero(np.diff(xx_idxs))[0]+1

        if transition_idxs.shape[0] > 0:
            if first_xx_idx==transition_idxs[0]:  # normal case
                xx_on_times = CNIBP[transition_idxs[::2],0]
                xx_off_times = CNIBP[transition_idxs[1::2],0]
            else:                             # if file started off in alarm state
                xx_on_times = np.concatenate([np.array([CNIBP[0,0]]),CNIBP[transition_idxs[1::2],0]])
                xx_off_times = CNIBP[transition_idxs[::2],0]

            if xx_on_times.shape != xx_off_times.shape:  #if XX goes into EOF
                #print alert_off_times.shape,type(alert_on_times),type(cnibpChunk[-1,0])
                xx_off_times = np.concatenate([xx_off_times,np.array([CNIBP[-1,0]])])
            xx_durations = xx_off_times - xx_on_times
            xx_over_delay_durations = xx_durations[xx_durations>=delay]
            num_xx_over_delay = xx_over_delay_durations.shape[0]
            time_in_xx_over_delay = np.sum(xx_over_delay_durations-delay)
            
    return num_xx_over_delay,time_in_xx_over_delay

