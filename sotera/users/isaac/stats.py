import numpy as np
import math

def mad(x):
    return np.median(np.abs(x - np.median(x)))

def normalize_data(data):
    """
    Normalize such that the mean of the input is 0 and the sample variance is 1

    :param data: The data set, expressed as a flat list of floats.
    :type data: list

    :return: The normalized data set, as a flat list of floats.
    :rtype: list
    """

    mean = np.mean(data)
    var = 0

    for _ in data:
        data[data.index(_)] = _ - mean

    for _ in data:
        var += math.pow(_, 2)

    var = math.sqrt(var / float(len(data)))

    for _ in data:
        data[data.index(_)] = _ / var

    return data

def sampen2(data, mm=2, r=0.2, normalize=False):
    """
    Calculates an estimate of sample entropy and the variance of the estimate.

    :param data: The data set (time series) as a list of floats.
    :type data: list

    :param mm: Maximum length of epoch (subseries).
    :type mm: int

    :param r: Tolerance. Typically 0.1 or 0.2.
    :type r: float

    :param normalize: Normalize such that the mean of the input is 0 and
    the sample, variance is 1.
    :type normalize: bool

    :return: List[(Int, Float/None, Float/None)...]

    Where the first (Int) value is the Epoch length.
    The second (Float or None) value is the SampEn.
    The third (Float or None) value is the Standard Deviation.

    The outputs are the sample entropies of the input, for all epoch lengths of
    0 to a specified maximum length, m.

    If there are no matches (the data set is unique) the sample entropy and
    standard deviation will return None.

    :rtype: list
    """

    n = len(data)

    if n == 0:
        raise ValueError("Parameter `data` contains an empty list")

    if mm > n / 2:
        raise ValueError(
            "Maximum epoch length of %d too large for time series of length "
            "%d (mm > n / 2)" % (
                mm,
                n,
            )
        )

    mm += 1

    mm_dbld = 2 * mm

    if mm_dbld > n:
        raise ValueError(
            "Maximum epoch length of %d too large for time series of length "
            "%d ((mm + 1) * 2 > n)" % (
                mm,
                n,
            )
        )

    if normalize is True:
        data = normalize_data(data)

    # initialize the lists
    run = [0] * n
    run1 = run[:]

    r1 = [0] * (n * mm_dbld)
    r2 = r1[:]
    f = r1[:]

    f1 = [0] * (n * mm)
    f2 = f1[:]

    k = [0] * ((mm + 1) * mm)

    a = [0] * mm
    b = a[:]
    p = a[:]
    v1 = a[:]
    v2 = a[:]
    s1 = a[:]
    n1 = a[:]
    n2 = a[:]

    for i in range(n - 1):
        nj = n - i - 1
        y1 = data[i]

        for jj in range(nj):
            j = jj + i + 1

            if data[j] - y1 < r and y1 - data[j] < r:
                run[jj] = run1[jj] + 1
                m1 = mm if mm < run[jj] else run[jj]

                for m in range(m1):
                    a[m] += 1
                    if j < n - 1:
                        b[m] += 1
                    f1[i + m * n] += 1
                    f[i + n * m] += 1
                    f[j + n * m] += 1

            else:
                run[jj] = 0

        for j in range(mm_dbld):
            run1[j] = run[j]
            r1[i + n * j] = run[j]

        if nj > mm_dbld - 1:
            for j in range(mm_dbld, nj):
                run1[j] = run[j]

    for i in range(1, mm_dbld):
        for j in range(i - 1):
            r2[i + n * j] = r1[i - j - 1 + n * j]
    for i in range(mm_dbld, n):
        for j in range(mm_dbld):
            r2[i + n * j] = r1[i - j - 1 + n * j]
    for i in range(n):
        for m in range(mm):
            ff = f[i + n * m]
            f2[i + n * m] = ff - f1[i + n * m]
            k[(mm + 1) * m] += ff * (ff - 1)
    m = mm - 1
    while m > 0:
        b[m] = b[m - 1]
        m -= 1
    b[0] = float(n) * (n - 1.0) / 2.0
    for m in range(mm):
        p[m] = float(a[m]) / float(b[m])
        v2[m] = p[m] * (1.0 - p[m]) / b[m]
    for m in range(mm):
        d2 = m + 1 if m + 1 < mm - 1 else mm - 1
        for d in range(d2):
            for i1 in range(d + 1, n):
                i2 = i1 - d - 1
                nm1 = f1[i1 + n * m]
                nm3 = f1[i2 + n * m]
                nm2 = f2[i1 + n * m]
                nm4 = f2[i2 + n * m]
                # if r1[i1 + n * j] >= m + 1:
                #    nm1 -= 1
                # if r2[i1 + n * j] >= m + 1:
                #    nm4 -= 1
                for j in range(2 * (d + 1)):
                    if r2[i1 + n * j] >= m + 1:
                        nm2 -= 1
                for j in range(2 * d + 1):
                    if r1[i2 + n * j] >= m + 1:
                        nm3 -= 1
                k[d + 1 + (mm + 1) * m] += float(2 * (nm1 + nm2) * (nm3 + nm4))

    n1[0] = float(n * (n - 1) * (n - 2))
    for m in range(mm - 1):
        for j in range(m + 2):
            n1[m + 1] += k[j + (mm + 1) * m]
    for m in range(mm):
        for j in range(m + 1):
            n2[m] += k[j + (mm + 1) * m]

    # calculate standard deviation for the set
    for m in range(mm):
        v1[m] = v2[m]
        dv = (n2[m] - n1[m] * p[m] * p[m]) / (b[m] * b[m])
        if dv > 0:
            v1[m] += dv
        s1[m] = math.sqrt(v1[m])

    # assemble and return the response
    response = []
    for m in range(mm):
        if p[m] == 0:
            # Infimum, the data set is unique, there were no matches.
            response.append((m, None, None))
        else:
            response.append((m, -math.log(p[m]), s1[m]))
    return response

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
    
    sampen = -1
    if correl[1] != 0:
        sampen_array = np.log(correl[0]/correl[1])
        sampen = sampen_array[0]
    
    return sampen

def update_rmssd(dRR_Win, RR_Win):

    # root mean square of successive differences
    l = len(dRR_Win)
    mu = np.mean(RR_Win)

    x = np.power(dRR_Win,2)
    y = np.sum(x)
    z = float(y)/(l-1)

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
