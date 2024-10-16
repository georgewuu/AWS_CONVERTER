from __future__ import print_function
import os
import numpy as np
from tempfile import NamedTemporaryFile
import subprocess
from intervaltree import Interval, IntervalTree
import sotera.io
import sotera.util.misc
import sotera.algorithms
import sotera.cluster.control

DATABASE_URIS = (
    "/tmp",
    ".",
    "http://wfdb.s3-website-us-west-1.amazonaws.com/MITDB",
    "http://wfdb.s3-website-us-west-1.amazonaws.com/AHA",
    "http://wfdb.s3-website-us-west-1.amazonaws.com/CUDB",
    "http://wfdb.s3-website-us-west-1.amazonaws.com/AFDB",
    "http://wfdb.s3-website-us-west-1.amazonaws.com/NSTDB",
)

WFDB_ENV_DEFAULT = ":".join(DATABASE_URIS)


def get_wfdb_records(pgsql_):
    s = "SELECT database, hid, record FROM standard_ecgdb_testing.records"
    records = {}
    with pgsql_, pgsql_.cursor() as cursor:
        cursor.execute(s)
        for row in cursor:
            try:
                records[row[0]].append((row[1], row[2]))
            except KeyError:
                records[row[0]] = [(row[1], row[2])]
    return records


def _mktime(sn):
    tm = 2 * sn
    ms = int(tm % 1000)

    if tm > ms:
        tm = (tm - ms) / 1000
        sec = int(tm % 60)
    else:
        sec = 0

    if tm > sec:
        tm = (tm - sec) / 60
        min_ = int(tm % 60)
    else:
        min_ = 0

    if tm > min_:
        tm = (tm - min_) / 60
        hour = int(tm % 60)
    else:
        hour = 0

    if hour > 0:
        return "{:2d}:{:02d}:{:02d}.{:03d}".format(hour, min_, sec, ms)
    else:
        return "   {:2d}:{:02d}.{:03d}".format(min_, sec, ms)


wfdb2tags = {
    ("+", "(N"): ["NSR"],
    ("+", "(AFIB"): ["AFIB"],
    ("+", "(AFL"): ["AFL"],
    ("+", "(VFL"): ["VFIB"],
    ("+", "(VT"): ["VTACH"],
    ("[",): ["VFIB"],
    ("]",): ["NSR"],
}

tags2wfdb = {"AFIB": "(AFIB", "VFIB": "(VT", "NSR": "(N"}


def load_and_munge_record(hid, db, pgsql_, filter_ecg=True, single_lead=False):
    data = sotera.io.load_session_data(hid, 0, pgsql_=pgsql_)
    raw = sotera.io.munge.preprocess_ecg(data, min_cols=6)
    fecg = sotera.algorithms.ecg_filter(raw) if filter_ecg else raw

    if db in ("mitdb", "aha", "nstdb"):
        fecg[:, 0] = raw[:, 0]
        fecg[:, 1] = fecg[:, 0] / 500.0
        fecg[:, 5] = fecg[:, 2 if single_lead else 3]
        fecg[:, 3] = fecg[:, 2]  # lead II <- first signal
        fecg[:, 4] = fecg[:, 2]  # lead III <- second signal
        fecg[:, 2] = 0

    elif db in ("cudb",):
        fecg[:, 0] = raw[:, 0]
        fecg[:, 1] = fecg[:, 0] / 500.0
        fecg[:, 5] = 0
        fecg[:, 3] = fecg[:, 2]  # lead II <- first signal
        fecg[:, 4] = 0
        fecg[:, 2] = 0

    return fecg


def load_wfdb_record(pgsql_, record):
    with pgsql_, pgsql_.cursor() as cursor:
        cursor.execute(
            """ SELECT hid, database
                    FROM standard_ecgdb_testing.records
                    WHERE record = '{}' """.format(
                record
            )
        )
        return load_and_munge_record(*cursor.fetchone()) if cursor.rowcount else None


def wfdb_annots_to_sotera_tree(wfdb_annots, begin, end):
    itree = IntervalTree()
    iv = [None, None, None]
    for wa in wfdb_annots:
        if wa[0] > end:
            continue
        if wa[1] == "+":  # Rhythm change
            try:
                tags = wfdb2tags[(wa[1], wa[-1])]
            except KeyError:
                tags = [
                    "OTHER",
                ]
            if iv[0] is None or wa[0] < begin:
                iv[0] = wa[0] if wa[0] > begin else begin
                iv[2] = tags
            elif iv[1] is None:
                iv[1] = wa[0]
                itree.add(Interval(iv[0], iv[1], iv[2]))
                iv = [wa[0], None, tags]
        elif wa[1] == "[":  # start vf
            try:
                tags = wfdb2tags[(wa[1],)]
            except KeyError:
                tags = [
                    "OTHER",
                ]
            if iv[0] is None or wa[0] < begin:
                iv[0] = wa[0] if wa[0] > begin else begin
                iv[2] = tags
            elif iv[1] is None:
                iv[1] = wa[0]
                itree.add(Interval(iv[0], iv[1], iv[2]))
                iv = [wa[0], None, tags]
        elif wa[1] == "]":  # stop vf
            try:
                tags = wfdb2tags[(wa[1],)]
            except KeyError:
                tags = [
                    "OTHER",
                ]
            if iv[0] is None or wa[0] < begin:
                iv[0] = wa[0] if wa[0] > begin else begin
                iv[2] = tags
            elif iv[1] is None:
                iv[1] = wa[0]
                itree.add(Interval(iv[0], iv[1], iv[2]))
                iv = [wa[0], None, tags]

    if iv[1] is None and iv[0] is not None and iv[0] < end:
        itree.add(Interval(iv[0], end, iv[2]))

    return itree


def sotera_tree_to_wfdb_annots(
    record, annotator, atree, cwd="/tmp", WFDB_ENV=WFDB_ENV_DEFAULT
):
    cmd = ["/opt/wfdb/bin/wrann", "-r", record, "-a", annotator]
    p = subprocess.Popen(cmd, stdin=subprocess.PIPE, cwd=cwd, env={"WFDB": WFDB_ENV})
    buf = ""
    if len(atree) > 0:
        for av in sorted(atree):
            sn = int(av.begin)
            tm = _mktime(sn)
            code = "+"
            for tag in av.data:
                buf += "{} {:8d}  {}  0    0    0\t{}\n".format(
                    tm, sn, code, tags2wfdb[tag]
                )
                if tag == "VFIB":
                    buf += "{} {:8d}  {}  0    0    0\t{}\n".format(
                        tm, sn, "[", tags2wfdb[tag]
                    )

            sn = int(av.end)
            tm = _mktime(sn)
            code = "+"
            buf += "{} {:8d}  {}  0    0    0\t{}\n".format(tm, sn, code, "(N")
            if "VFIB" in av.data:
                buf += "{} {:8d}  {}  0    0    0\t{}\n".format(
                    tm, sn, "]", tags2wfdb["VFIB"]
                )

    else:
        sn = 0
        tm = _mktime(sn)
        code = "+"
        buf += "{} {:8d}  {}  0    0    0\t{}\n".format(tm, sn, code, "(N")

    try:
        p.communicate(input=bytes(buf, "utf_8"))
    except:  # noqa E722
        p.communicate(input=buf)

    p.stdin.close()
    p.terminate()
    return os.path.join(cwd, "{}.{}".format(record, annotator))


def beats_to_ann(record, annotator, beats, cwd="/tmp", WFDB_ENV=WFDB_ENV_DEFAULT):
    cmd = ["/opt/wfdb/bin/wrann", "-r", record, "-a", annotator]
    p = subprocess.Popen(cmd, stdin=subprocess.PIPE, cwd=cwd, env={"WFDB": WFDB_ENV})
    buf = ""
    for beat in beats:
        sn = int(beat[0])
        tm = _mktime(sn)
        code = "Q"
        buf += "{} {:8d}  {}  0    0    0\n".format(tm, sn, code)
    try:
        p.communicate(input=bytes(buf, "utf_8"))
    except:  # noqa E722
        p.communicate(input=buf)
    p.stdin.close()
    p.terminate()
    return os.path.join(cwd, "{}.{}".format(record, annotator))


def hr_to_ann(record, annotator, hr, cwd="/tmp", WFDB_ENV=WFDB_ENV_DEFAULT):
    cmd = ["/opt/wfdb/bin/wrann", "-r", record, "-a", annotator]
    p = subprocess.Popen(cmd, stdin=subprocess.PIPE, cwd=cwd, env={"WFDB": WFDB_ENV})
    buf = ""
    for m in hr:
        # sn = int(m[0])
        # tm = _mktime(sn)
        # code = "="
        pass
    try:
        p.communicate(input=bytes(buf, "utf_8"))
    except:  # noqa E722
        p.communicate(input=buf)
    p.stdin.close()
    p.terminate()
    return os.path.join(cwd, "{}.{}".format(record, annotator))


BEAT_CODES = (
    "N",
    "L",
    "R",
    "A",
    "J",
    "S",
    "V",
    "r",
    "F",
    "Q",
    "a",
    "e",
    "j",
    "n",
    "E",
    "/",
    "f",
)


def make_refhr(
    record, ref="atr", start=0, stop=-1, cwd="/tmp", WFDB_ENV=WFDB_ENV_DEFAULT
):
    annots = _rdann(record, ref, cwd=cwd, WFDB_ENV=WFDB_ENV)
    beats = np.array([a[0] for a in annots if a[1] in BEAT_CODES])
    beats = np.c_[beats, beats / 500.0].astype(int)
    offline = sotera.algorithms.heartrate_numeric(beats, start_sn=start, stop_sn=stop)
    return offline["HR_RHYTHM"]


def _mxm(record, ref, test, cwd="/tmp", WFDB_ENV=WFDB_ENV_DEFAULT):
    cmd = ["/opt/wfdb/bin/mxm", "-r", record, "-a", ref, test, "-l", "-"]
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, cwd=cwd, env={"WFDB": WFDB_ENV})
    line = p.stdout.readline().decode("utf_8").strip()
    p.terminate()
    return line


def mxm(record, HR_REF, HR, startsn=-1, cwd="/tmp", WFDB_ENV=WFDB_ENV_DEFAULT):

    with NamedTemporaryFile(prefix=record + ".", dir=cwd, delete=False) as tmpfile1:
        ref_ann = tmpfile1.name.split(".")[1]
        fn1 = hr_to_ann(record, ref_ann, HR_REF, cwd=cwd, WFDB_ENV=WFDB_ENV)

    with NamedTemporaryFile(prefix=record + ".", dir=cwd, delete=False) as tmpfile2:
        test_ann = tmpfile2.name.split(".")[1]
        fn2 = hr_to_ann(record, test_ann, HR, cwd=cwd, WFDB_ENV=WFDB_ENV)
    line_ = _mxm(record, ref_ann, test_ann, cwd=cwd, WFDB_ENV=WFDB_ENV)
    os.remove(fn1)
    os.remove(fn2)
    return line_


def _bxb(record, ref, test, cwd="/tmp", WFDB_ENV=WFDB_ENV_DEFAULT):
    with NamedTemporaryFile() as fp:
        cmd = ["/opt/wfdb/bin/bxb", "-r", record, "-a", ref, test, "-l", "-", fp.name]
        p = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, cwd=cwd, env={"WFDB": WFDB_ENV}
        )
        line_ = p.stdout.readline().decode("utf_8").strip()
        p.terminate()
    return line_


def bxb(record, HR_BEAT, ref="atr", cwd="/tmp", WFDB_ENV=WFDB_ENV_DEFAULT):
    idx = HR_BEAT[:, 7].astype(int) > 100
    beats = HR_BEAT[idx, :]
    with NamedTemporaryFile(prefix=record + ".", dir=cwd, delete=False) as tmpfile:
        ann = tmpfile.name.split(".")[1]
    fn = sotera.analysis.wfdb.beats_to_ann(record, ann, beats)
    line_ = _bxb(record, ref, ann, cwd=cwd, WFDB_ENV=WFDB_ENV)
    os.remove(fn)
    return line_


def _epicmp_afib(record, ref, test, cwd="/tmp", WFDB_ENV=WFDB_ENV_DEFAULT):
    cmd = ["/opt/wfdb/bin/epicmp", "-r", record, "-a", ref, test, "-l", "-A", "-"]
    p = subprocess.Popen(cmd, cwd=cwd, stdout=subprocess.PIPE, env={"WFDB": WFDB_ENV})
    line_ = p.stdout.readline().decode("utf_8").strip()
    p.terminate()
    return line_


def epicmp_afib(record, HR, ref="atr", cwd="/tmp", WFDB_ENV=WFDB_ENV_DEFAULT):
    itree = sotera.util.misc.get_afib_intervals(HR)
    with NamedTemporaryFile(prefix=record + ".", dir=cwd, delete=False) as tmpfile:
        ann = tmpfile.name.split(".")[1]
    fn = sotera_tree_to_wfdb_annots(record, ann, itree)
    line_ = _epicmp_afib(record, ref, ann, cwd=cwd, WFDB_ENV=WFDB_ENV)
    os.remove(fn)
    return line_


def _epicmp_vfib(record, ref, test, cwd="/tmp", WFDB_ENV=WFDB_ENV_DEFAULT):
    cmd = ["/opt/wfdb/bin/epicmp", "-r", record, "-a", ref, test, "-l", "-V", "-"]
    p = subprocess.Popen(cmd, cwd=cwd, stdout=subprocess.PIPE, env={"WFDB": WFDB_ENV})
    line_ = p.stdout.readline().decode("utf_8").strip()
    p.terminate()
    return line_


def epicmp_vfib(record, HR, ref="atr", cwd="/tmp", WFDB_ENV=WFDB_ENV_DEFAULT):
    itree = sotera.util.misc.get_vfib_intervals(HR)
    with NamedTemporaryFile(prefix=record + ".", dir=cwd, delete=False) as tmpfile:
        ann = tmpfile.name.split(".")[1]
    fn = sotera_tree_to_wfdb_annots(record, ann, itree)
    line_ = _epicmp_vfib(record, ref, ann, cwd=cwd, WFDB_ENV=WFDB_ENV)
    os.remove(fn)
    return line_


def compare_all(record, db, offline, ref="atr", cwd="/tmp", WFDB_ENV=WFDB_ENV_DEFAULT):
    bxb_ = None
    mxm_ = None
    vfib_ = None
    afib_ = None
    HR_REF = None
    if db in ("mitdb", "nstdb"):
        afib_ = sotera.analysis.wfdb.epicmp_afib(
            record, offline["HR_RHYTHM"], ref=ref, cwd=cwd, WFDB_ENV=WFDB_ENV
        )
    if db in ("mitdb", "aha", "cudb"):
        vfib_ = sotera.analysis.wfdb.epicmp_vfib(
            record, offline["HR_RHYTHM"], ref=ref, cwd=cwd, WFDB_ENV=WFDB_ENV
        )
    if db in ("mitdb", "aha", "nstdb"):
        bxb_ = sotera.analysis.wfdb.bxb(
            record, offline["HR_BEAT"], ref=ref, cwd=cwd, WFDB_ENV=WFDB_ENV
        )
        HR_REF = sotera.analysis.wfdb.make_refhr(
            record,
            stop=int(offline["HR_RHYTHM"][-1, 0] + 250),
            ref=ref,
            cwd=cwd,
            WFDB_ENV=WFDB_ENV,
        )
        mxm_ = sotera.analysis.wfdb.mxm(
            record, offline["HR_RHYTHM"], HR_REF, cwd=cwd, WFDB_ENV=WFDB_ENV
        )
    return bxb_, mxm_, vfib_, afib_, HR_REF


def exclude_line(l):
    return l[:3] in ("102", "104", "107", "217") or l[:4] in ("2202", "8205")


def sumstats_bxb(lines, default_excludes=True, cwd="/tmp", WFDB_ENV=WFDB_ENV_DEFAULT):
    with NamedTemporaryFile(delete=False, mode="w") as stats:
        print(
            "Record Nn' Vn' Fn' On'  Nv   Vv  Fv' Ov' No'"
            " Vo' Fo'  Q Se   Q +P   V Se   V +P  V FPR",
            file=stats,
        )
        for l in lines:
            if not (default_excludes and exclude_line(l)):
                print(l, file=stats)
        stats.close()
        cmd = ["/opt/wfdb/bin/sumstats", stats.name]
        p = subprocess.Popen(
            cmd, cwd=cwd, stdout=subprocess.PIPE, env={"WFDB": WFDB_ENV}
        )
        result = [l.decode("utf_8").strip() for l in p.stdout.readlines()]
        os.remove(stats.name)
        p.terminate()
    return result


def sumstats_mxm(lines, default_excludes=True, cwd="/tmp", WFDB_ENV=WFDB_ENV_DEFAULT):
    with NamedTemporaryFile(delete=False, mode="w") as stats:
        print("(Measurement errors)", file=stats)
        print("Record\tRMS error (%%)\tMean reference measurement", file=stats)
        for l in lines:
            if not (default_excludes and exclude_line(l)):
                print(l, file=stats)
        stats.close()
        cmd = ["/opt/wfdb/bin/sumstats", stats.name]
        p = subprocess.Popen(
            cmd, cwd=cwd, stdout=subprocess.PIPE, env={"WFDB": WFDB_ENV}
        )
        result = [l.decode("utf_8").strip() for l in p.stdout.readlines()]
        os.remove(stats.name)
        p.terminate()
    return result


def sumstats_afib(lines, default_excludes=True, cwd="/tmp", WFDB_ENV=WFDB_ENV_DEFAULT):
    with NamedTemporaryFile(delete=False, mode="w") as stats:
        print("(AF detection)", file=stats)
        print(
            "Record  TPs   FN  TPp   FP  ESe E+P DSe D+P  Ref duration  Test duration",
            file=stats,
        )
        for l in lines:
            if not (default_excludes and exclude_line(l)):
                print(l, file=stats)
        stats.close()
        cmd = ["/opt/wfdb/bin/sumstats", stats.name]
        p = subprocess.Popen(
            cmd, cwd=cwd, stdout=subprocess.PIPE, env={"WFDB": WFDB_ENV}
        )
        result = [l.decode("utf_8").strip() for l in p.stdout.readlines()]
        os.remove(stats.name)
        p.terminate()
    return result


def sumstats_vfib(lines, default_excludes=True, cwd="/tmp", WFDB_ENV=WFDB_ENV_DEFAULT):
    with NamedTemporaryFile(delete=False, mode="w") as stats:
        print("(VF detection)", file=stats)
        print(
            "Record  TPs   FN  TPp   FP  ESe E+P DSe D+P  Ref duration  Test duration",
            file=stats,
        )
        for l in lines:
            if not (default_excludes and exclude_line(l)):
                print(l, file=stats)
        stats.close()
        cmd = ["/opt/wfdb/bin/sumstats", stats.name]
        p = subprocess.Popen(
            cmd, cwd=cwd, stdout=subprocess.PIPE, env={"WFDB": WFDB_ENV}
        )
        result = [l.decode("utf_8").strip() for l in p.stdout.readlines()]
        os.remove(stats.name)
        p.terminate()
    return result


def _rdann(record, ann, cwd="/tmp", WFDB_ENV=WFDB_ENV_DEFAULT):
    cmd = ["/opt/wfdb/bin/rdann", "-e", "-r", record, "-a", ann]
    p = subprocess.Popen(cmd, cwd=cwd, stdout=subprocess.PIPE, env={"WFDB": WFDB_ENV})
    annots = []
    for line in p.stdout.readlines():
        fields = line.decode("utf_8").strip().split()
        sn = int(fields[1])
        code = fields[2]
        aux = fields[-1]
        annots.append([sn, code, aux])
    p.terminate()
    return annots


def rdann(record, ann, begin, end, cwd="/tmp", WFDB_ENV=WFDB_ENV_DEFAULT):
    return wfdb_annots_to_sotera_tree(
        _rdann(record, ann, cwd=cwd, WFDB_ENV=WFDB_ENV), begin, end
    )


# cluster based analysis functions
def make_database_analysis_job(
    pgsql_, name, databases=("aha", "mitdb", "cudb", "nstdb"), settings={}
):
    values = []
    aid = sotera.cluster.control.add_analysis(pgsql_, name)
    RECORDS = sotera.analysis.wfdb.get_wfdb_records(pgsql_)
    list_ = []
    if "filter_ecg" in settings.keys():
        list_.append(f"'filter_ecg',{settings['filter_ecg']}")
    if "single_lead" in settings.keys():
        list_.append(f"'single_lead',{settings['single_lead']}")
    if len(list_) > 0:
        str_ = f" 'settings',jsonb_build_object({','.join(list_)})"
    else:
        str_ = ""
    for db in databases:
        recs = RECORDS[db]
        for hid, record in recs:
            values.append(
                f"""({aid}, jsonb_build_object('hid',{hid},'db','{db}',
                     'record','{record}',{str_}))"""
            )
    sql = f""" INSERT
                 INTO analysis_jobs (aid, args)
               VALUES { ','.join(values)} """
    with pgsql_, pgsql_.cursor() as cursor:
        cursor.execute(sql)
    return aid


@sotera.cluster.control.cluster_decorate()
def cluster_database_runs(pgsql_, aid, jobid, args):
    import sotera
    import sotera.algorithms

    hid = args["hid"]
    db = args["db"]
    record = args["record"]
    settings = args.get("settings", {})
    randomize = settings.get("randomize", False)
    filter_ecg = settings.get("filter_ecg", False)
    single_lead = settings.get("single_lead", False)

    returns = {"bxb": None, "mxm": None, "vfib": None, "afib": None}

    # randomize the starting point of the file
    fecg = load_and_munge_record(
        hid, db, pgsql_, filter_ecg=filter_ecg, single_lead=single_lead
    )

    if randomize:
        offset = int(np.floor(8 * 500 * np.random.rand()))
        offline = sotera.algorithms.heartrate_beat_detector(fecg[offset:, :])
    else:
        offline = sotera.algorithms.heartrate_beat_detector(fecg)

    (
        returns["bxb"],
        returns["mxm"],
        returns["vfib"],
        returns["afib"],
        hr_ref,
    ) = sotera.analysis.wfdb.compare_all(record, db, offline)

    return returns


def aggregate_analysis_results(
    pgsql_, aid, databases=("aha", "mitdb", "cudb", "nstdb")
):
    results = {}
    for db in databases:
        results[db] = {"vfib": [], "mxm": [], "bxb": [], "afib": []}
    with pgsql_, pgsql_.cursor() as cursor:
        cursor.execute(
            f"""
        SELECT args, returns
            FROM analysis.analysis_jobs
            WHERE aid = {aid}
        """
        )
        for args, returns in cursor:
            if args["db"] in databases:
                for test in ("bxb", "mxm", "afib", "vfib"):
                    if returns[test] is not None:
                        results[args["db"]][test].append(returns[test])
    return results
