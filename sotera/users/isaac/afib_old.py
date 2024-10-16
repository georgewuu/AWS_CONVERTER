
import numpy as np
from . import stats

def afib_features(RR, dRR, sampen_dim, sampen_scale):

        
    if dRR.shape[0] > 3:
        sampen = stats.update_sampen(dRR, sampen_dim, sampen_scale)
        rmssd =  stats.update_rmssd(dRR, RR)
        tpr = stats.update_tpr(RR)
    else:
        sampen = -1
        rmssd = -1
        tpr
        
    return [sampen,rmssd,tpr,RR.shape[0]]

def afib_driver(beats, atree, num_beats, sampen_dim, sampen_scale, DELTA = 500 * 15):
    RR_wins = []
    dRR_wins = []
    features = []

    MAX_WINDOW_TIME = 500 * num_beats * (60/30)
    
    RR = beats[:,(0,2)].tolist()
    if beats.shape[0] > 10:
        min_ = beats[:,0].min()
        max_ = beats[:,0].max()
        NWINS = int(np.ceil((max_ - min_)/ DELTA))
        for i in (range(NWINS)):
            sn1 = min_ + DELTA*i
            sn0 = sn1 - MAX_WINDOW_TIME
            if sn0 < 0:
                sn0 = 0
                
            i = beats[:,0] <= sn1

            if np.sum(i) > num_beats:
                tmp = beats[i,:][-num_beats:]
                i = tmp[:,0] >= sn1 - MAX_WINDOW_TIME
                if np.sum(i) > 0:
                    tmp = tmp[i,:]
                    tmp = tmp[tmp[:,2] > 0,:]
                    sn0 = tmp[0,0]
                    RR_wins.append(tmp[:,2])
                    dRR_wins.append(np.diff(RR_wins[-1]))
                    f = afib_features(RR_wins[-1], dRR_wins[-1],sampen_dim, sampen_scale)
                else:
                    f = 4 * [ -1 ]
                    
            else:
                f = 4 * [ -1 ]
                
            p = 0.
            is_afib = 0
            for iv in atree.search( sn0, sn1 ):
                if 'AFIB' in iv.data:
                    if sn0 < iv.begin:
                        p = (min(sn1,iv.end) - iv.begin)/float(sn1-sn0)
                    elif sn1 > iv.end:
                        p = ( iv.end - sn0 ) / float(sn1-sn0)
                    else:
                        p = 1.
                    is_afib = int(p >= 0.5)
                    break
            features.append( [sn0, sn1] + f + [p, is_afib] )
            
    features = np.array( features )
    RR_wins = np.array(RR_wins)
    dRR_wins = np.array(dRR_wins)
    RR = np.array(RR)
    
    return RR, RR_wins, dRR_wins, features

