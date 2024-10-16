import psycopg2
from sotera.db import get_care_units, get_pds_id
from intervaltree import IntervalTree, Interval
from datetime import datetime, timedelta, time


# Site Health functions for data analysis and insertion into database


def populate_lir_table(conn, site, dateStart, dateStop):
    # authorize??
    params = [
        ("SPO2", "LOW"),
        ("CR", "HIGH"),
        ("CR", "LOW"),
        ("RR", "HIGH"),
        ("RR", "LOW"),
        ("PR", "HIGH"),
        ("PR", "LOW"),
        ("HR_A", "HIGH"),
        ("HR_A", "LOW"),
        ("HR", "HIGH"),
        ("HR", "LOW"),
    ]
    params_CNIBP = [
        ("CNIBP_SYS", "LOW"),
        ("CNIBP_SYS", "HIGH"),
        ("CNIBP_DIA", "LOW"),
        ("CNIBP_DIA", "HIGH"),
        ("CNIBP_MAP", "LOW"),
        ("CNIBP_MAP", "HIGH"),
    ]

    with conn as pgsql_:

        cuList = get_care_units(pgsql_, site, "raw_care_unit")
        cuList.append(None)
        # populate sessions and hours
        for cu in cuList:
            hours = compose_lir_hours(
                conn=pgsql_,
                fromStopDate=dateStart,
                toStopDate=dateStop,
                site=site,
                careUnit=cu,
            )
            for row in hours:
                populate_lir_hours(
                    conn=pgsql_,
                    site=site,
                    date=row["date"],
                    sessions=row["sessions"],
                    hours=row["hours"],
                    careUnit=cu,
                )
            # Now get APDs
            # get first 3 params
            aggApd = {}
            hoursDict = {}
            defaults = get_site_defaults(
                pgsql_, site
            )  # get thresholds & delays for site
            for row in hours:
                hoursDict[row["date"]] = {
                    "sessions": row["sessions"],
                    "hours": row["hours"],
                }

            for pair in params:
                param = pair[0]
                if param == "HR_A":
                    aparam = "HR"
                else:
                    aparam = param

                alarmType = pair[1]

                lirApd = compose_lir_apd(
                    conn=pgsql_,
                    fromStopDate=dateStart,
                    toStopDate=dateStop,
                    site=site,
                    param=param,
                    alarmType=alarmType,
                    threshold=defaults[aparam][alarmType]["threshold"],
                    delay=defaults[aparam][alarmType]["delay"],
                    careUnit=cu,
                )

                for row in lirApd:
                    if row["date"] not in aggApd.keys():
                        aggApd[row["date"]] = {}
                    if param not in aggApd[row["date"]].keys():
                        aggApd[row["date"]][param] = 0.0
                    aggApd[row["date"]][param] += float(row["APD"])

            # get CNIBP
            for pair in params_CNIBP:
                param = pair[0]
                alarmType = pair[1]
                CNIBP_lirData = compose_lir_apd(
                    conn=pgsql_,
                    fromStopDate=dateStart,
                    toStopDate=dateStop,
                    site=site,
                    param=param,
                    alarmType=alarmType,
                    threshold=defaults[param][alarmType]["threshold"],
                    delay=defaults[param][alarmType]["delay"],
                    careUnit=cu,
                )

                for row in CNIBP_lirData:
                    if row["date"] not in aggApd.keys():
                        aggApd[row["date"]] = {}
                    aggApd[row["date"]]["CNIBP"] = float(row["APD"])

            # get totals
            for date in aggApd.keys():
                aggApd[date]["Total"] = 0.0
                for param in aggApd[date].keys():
                    if param not in ["Total", "CR", "HR"]:
                        aggApd[date]["Total"] += aggApd[date][param]

            # insert values into LIR table
            for date in sorted(aggApd.keys()):
                # print 'Starting date: {}'.format(date)
                for param in aggApd[date].keys():
                    populate_lir_apd(
                        conn=pgsql_,
                        site=site,
                        date=date,
                        param=param,
                        apd=aggApd[date][param],
                        sessions=hoursDict[date]["sessions"],
                        careUnit=cu,
                    )

    return True


def compose_lir_hours(
    conn, fromStopDate, toStopDate, site, careUnit=None, duration_min=3600
):
    """ get LIR data """
    if careUnit is None:
        cu_str = "is null"
    else:
        cu_str = "= '{0}'".format(careUnit)

    sql = """
    SELECT
        date_stop as date,
        count(*) as sessions,
        sum(duration)/3600.0 as hours
    from session_management.aa_session_data
    where
        site = '{0}'
        AND date_stop between '{1}' and '{2}'
        AND duration >= {3}
        AND care_unit {4}
    group by date_stop
    """.format(
        site, fromStopDate, toStopDate, duration_min, cu_str
    )

    # print sql
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(sql)
        data = cur.fetchall()

    return data


def compose_lir_apd(
    conn,
    fromStopDate,
    toStopDate,
    site,
    param,
    alarmType,
    threshold,
    delay,
    careUnit=None,
    duration_min=3600,
):
    """ get LIR data """
    time_postfix = determine_time_postfix_for_param(param)

    if careUnit is None:
        cu_str = "is null"
    else:
        cu_str = "= '{0}'".format(careUnit)

    sql = """
    SELECT
        sd.date_stop as date,
        SUM(a.alarms/(sd.time_{10}/86400.0)) as "APD"  --this is actually alarms/day
    FROM analytics.aa_alarms a
    JOIN session_management.aa_session_data sd
        ON a.hid = sd.hid
    WHERE
        a.param {0} '{1}'
        AND a.alarm_type = '{2}'
        AND a.threshold = {3}
        AND a.delay = {4}
        AND sd.site = '{5}'
        AND sd.date_stop BETWEEN '{6}' AND '{7}'
        AND sd.duration >= {8}
        AND sd.care_unit {9}
    group by
        sd.date_stop
    """.format(
        "=" if param != "CNIBP" else "like",
        param if param != "CNIBP" else param + "%",
        alarmType,
        threshold,
        delay,
        site,
        fromStopDate,
        toStopDate,
        duration_min,
        cu_str,
        time_postfix,
    )

    # print sql
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(sql)
        data = cur.fetchall()

    return data


def populate_lir_hours(conn, site, date, sessions, hours, careUnit=None):
    if careUnit is None:
        cu_eq = "is"
        cu_str = "null"
    else:
        cu_eq = "="
        cu_str = "'{0}'".format(careUnit)

    sql = """   DO
                $do$
                BEGIN
                    if ( not exists(
                          select *
                            from analytics.leading_indicator_info
                           where site_id='{0}' and date='{1}' and care_unit {5} {4}))
                        then
                            INSERT INTO analytics.leading_indicator_info
                                       (site_id, care_unit, date, sessions,
                                        hours, apd_total, apd_spo2, apd_cr,
                                        apd_rr, apd_cnibp, apd_pr, apd_hr, apd_hr_a)
                            values ('{0}',{4},'{1}',{2},{3},0,0,0,0,0,0,0,0) ;
                    end if;

                    update analytics.leading_indicator_info
                    set
                        sessions={2},
                        hours={3}
                    where
                        site_id = '{0}'
                        and date = '{1}'
                        and care_unit {5} {4};
                END
                $do$
                """.format(
        site, date, sessions, hours, cu_str, cu_eq
    )
    # print sql
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(sql)
        conn.commit()


def populate_lir_apd(conn, site, date, param, apd, sessions, careUnit=None):
    if careUnit is None:
        cu_eq = "is"
        cu_str = "null"
    else:
        cu_eq = "="
        cu_str = "'{0}'".format(careUnit)

    sql = """   DO
                $do$
                begin
                    if(not exists(
                        select *
                          from analytics.leading_indicator_info
                         where site_id='{0}' and date='{1}' and care_unit {5} {4}
                        ))
                        then
                            insert into analytics.leading_indicator_info
                             (site_id, date, apd_{2}, care_unit)
                            values ('{0}','{1}',{3},{4}) ;
                    end if;

                    update analytics.leading_indicator_info
                    set
                        apd_{2}={3}
                    where
                        site_id = '{0}'
                        and date = '{1}'
                        and care_unit {5} {4};
                end
                $do$
                """.format(
        site, date, param, apd / sessions, cu_str, cu_eq
    )
    # print sql
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(sql)
        conn.commit()


def determine_time_postfix_for_param(param):
    # get right time
    postfix = ""
    if param in ["PR", "SPO2", "CR"]:  # XX
        postfix = "WT"
    elif param in ["HR", "RR", "TEMP", "HR_A"]:
        postfix = "Cable"
    elif param in ["BP_SYS", "BP_DIA", "BP_MAP", "CARDIAC"]:
        postfix = "Total"
    elif param in ["CNIBP_SYS", "CNIBP_DIA", "CNIBP_MAP"]:
        postfix = "CNIBP"
    return postfix


def get_site_defaults(conn, site):
    """ get threshold and delay defaults for given site """
    if site == "%":
        site = "Sotera"

    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(
            """ SELECT
                            param,
                            alarm_type,
                            threshold,
                            delay
                        FROM site_management.aa_site_defaults
                        where code = '{}'
                        ORDER BY
                            param """.format(
                site
            )
        )

        data = cur.fetchall()

    defaults_dict = {}
    for row in data:
        if row["param"] not in defaults_dict.keys():
            defaults_dict[row["param"]] = {}
        if row["alarm_type"] not in defaults_dict[row["param"]].keys():
            defaults_dict[row["param"]][row["alarm_type"]] = {}
        defaults_dict[row["param"]][row["alarm_type"]]["threshold"] = row["threshold"]
        defaults_dict[row["param"]][row["alarm_type"]]["delay"] = row["delay"]

    return defaults_dict


def compose_device_session_interval_count(
    conn, pds_id, dateStart, dateStop, cuName, minLength=30
):
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        sql = """
        SELECT  device_id,
                start_time::timestamp,
                stop_time::timestamp
        FROM apollo_download.device_sessions
        WHERE
            start_time::timestamp::date >= '{0}'
            AND stop_time::timestamp::date <= '{1}'
            AND pds_id = {2}
            AND extract(
                EPOCH FROM (stop_time::timestamp - start_time::timestamp)
            ) > 60 * {3}
            """.format(
            dateStart, dateStop, pds_id, minLength
        )
        if cuName != "null":
            sql += "AND care_unit = '{0}'".format(cuName)
        else:
            sql += "AND care_unit is null"

        sql += """
        ORDER BY start_time;
        """
        cur.execute(sql)
        sessions = cur.fetchall()

    deviceSessionIntervals = IntervalTree(
        Interval(s["start_time"], s["stop_time"], s["device_id"]) for s in sessions
    )

    return deviceSessionIntervals


def populate_device_session_interval_count_table(conn, site, dateStart, dateStop):
    pds_id = get_pds_id(conn, site)
    careUnits = get_care_units(conn, site, cuClass="raw_care_unit")
    careUnits.append("null")
    for cuName in careUnits:
        sessionIntervals = compose_device_session_interval_count(
            conn, pds_id, dateStart, dateStop, cuName
        )
        drange = (dateStop - dateStart).days
        for date_inc in range(0, drange):
            date = dateStart + timedelta(date_inc)
            for hr in range(0, 24):
                dt1 = datetime.combine(date, time(hr))
                dt2 = dt1 + timedelta(hours=1)
                num = len(
                    sessionIntervals[dt1:dt2]
                )  # Number of devices connected over the 1 hour timespan
                sql = """
                INSERT
                  INTO analytics.device_session_interval_count
                       (pds_id,date,hour,num_devices,care_unit)
                VALUES ({0},'{1}',{2},{3},'{4}')""".format(
                    pds_id, date, hr, num, cuName
                )
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                    cur.execute(sql)

    conn.commit()
    return True
