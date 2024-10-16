from matplotlib.ticker import FuncFormatter, MaxNLocator
from matplotlib.pylab import (
    figure,
    subplot,
    plot,
    ylabel,
    ylim,
    xlim,
    grid,
    legend,
    title,
    subplots_adjust,
    setp,
)
from numpy import max as np_max, min as np_min
from .time import get_string_from_timestamp


class TimeFormatter:
    def __init__(self, zone, fmt="%m-%d %H:%M"):
        self.zone = zone
        self.fmt = fmt

    def timestr(self, ts, pos):
        return get_string_from_timestamp(ts, self.zone, self.fmt)

    def formatter(self):
        return FuncFormatter(self.timestr)

    def apply(self, axes):
        axes.xaxis.set_major_formatter(self.formatter())


def display_range(values, min_range=None, max_val=None):
    tmp = values[values > -1]
    maxv = np_max(tmp)
    minv = np_min(tmp)
    rangev = maxv - minv
    midpoint = minv + rangev / 2
    rangev *= 1.5
    if min_range is not None:
        rangev = max(min_range, rangev)
    ul = midpoint + rangev / 2
    ll = midpoint - rangev / 2
    if max_val is not None:
        if ul > max_val:
            ll = ll - (ul - max_val)
            ul = max_val
    return ll, ul


def time_iter(tstart, tmax, period):
    ts0 = tstart
    ts1 = ts0 + period
    while ts1 <= tmax:
        yield ts0, ts1
        ts0 += period
        ts1 += period
    yield ts0, ts1


def numerics_plot(data, start_ts, titlestr="", NHOURS=8, zone="US/Pacific"):
    ms = 3
    times = list(time_iter(start_ts, data["HR"][-1, 1], NHOURS * 60 * 60))
    N = len(times)
    for n, (ts0, ts1) in enumerate(times):
        fig = figure(figsize=(8.5, 11))
        ax0 = subplot(411)
        idx = (data["HR"][:, 1] >= ts0) * (data["HR"][:, 1] <= ts1)
        plot(data["HR"][idx, 1], data["HR"][idx, 2], "g.", markersize=ms)
        hrlim = display_range(data["HR"][idx, 2])

        idx = (data["PR"][:, 1] >= ts0) * (data["PR"][:, 1] <= ts1)
        plot(data["PR"][idx, 1], data["PR"][idx, 2], "b.", markersize=ms)
        prlim = display_range(data["PR"][idx, 2])
        ylabel("HR/PR [bpm]")
        ylim(min(hrlim[0], prlim[0]), max(hrlim[1], prlim[1]))
        grid(axis="x")
        legend(["HR", "PR"], markerscale=5, loc=1)
        if len(titlestr) > 0:
            tmp = (titlestr, f"page {n+1} of {N}")
        else:
            tmp = (f"page {n+1} of {N}",)
        title(" ".join(tmp))

        subplot(412, sharex=ax0)
        idx = (data["SPO2"][:, 1] >= ts0) * (data["SPO2"][:, 1] <= ts1)
        plot(data["SPO2"][idx, 1], data["SPO2"][idx, 2], "b.", markersize=ms)
        ylabel("SPO2 [%]")
        ylim(*display_range(data["SPO2"][idx, 2], 20, 100.5))
        grid(axis="x")

        subplot(413, sharex=ax0)
        idx = (data["RR"][:, 1] >= ts0) * (data["RR"][:, 1] <= ts1)
        plot(data["RR"][idx, 1], data["RR"][idx, 2], "r.", markersize=ms)
        ylabel("Resp [bpm]")
        ylim(*display_range(data["RR"][idx, 2]))
        grid(axis="x")

        ax = subplot(414, sharex=ax0)
        idx = (data["CNIBP"][:, 1] >= ts0) * (data["CNIBP"][:, 1] <= ts1)
        plot(data["CNIBP"][idx, 1], data["CNIBP"][idx, 2], "b.", markersize=ms)
        plot(data["CNIBP"][idx, 1], data["CNIBP"][idx, 3], "r.", markersize=ms)
        plot(data["CNIBP"][idx, 1], data["CNIBP"][idx, 4], "g.", markersize=ms)
        ylabel("CNIBP [mmHg]")

        syslim = display_range(data["CNIBP"][idx, 2])
        dialim = display_range(data["CNIBP"][idx, 3])
        maplim = display_range(data["CNIBP"][idx, 4])
        ylim(min(syslim[0], dialim[0], maplim[0]), max(syslim[1], dialim[1], maplim[1]))

        xlim(ts0 - 150, ts1 + 150)
        ax.xaxis.set_major_locator(MaxNLocator(5))
        TimeFormatter(zone).apply(ax)
        grid(axis="x")

        subplots_adjust(hspace=0)
        setp([a.get_xticklabels() for a in fig.axes[:-1]], visible=False)
        yield fig
