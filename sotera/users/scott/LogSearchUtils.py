
import psycopg2
import psycopg2.extras
import os
import sys
#import numpy as np
import pytz
from datetime import datetime, date, timedelta
#import boto
#import requests
from time import time
if sys.version_info > (3,0):    
    timestamp = datetime.timestamp
else:
    from backports.datetime_timestamp import timestamp
#from fuzzywuzzy import fuzz 
import csv
import re
import json
from sotera.io.cloud import load_session_data
import calendar
from sotera.aws import get_pgsql_connection
import sotera.util
from threading import Thread
from Queue import Queue
import select

def get_string_from_timestamp(unixtime, zone, fmt = '%Y-%m-%d %H:%M:%S %Z'):
    if not unixtime:
        return 'NA'
    tz = pytz.timezone(zone)
    dt = datetime.fromtimestamp(unixtime, tz=tz)
    return dt.strftime(fmt)


def searchFullLogs(conn,message,elim_msg,hid,d1,d2,t1,t2,site,deviceID,msg_type,elim_type,log_lvl,dev_class,lid,sw_version,order_by,ciph):
    cipher_table = getCipherTable(conn)
    if message != "":
        ciphers, contents = fit_message_to_cipher(message,cipher_table)
        print(ciphers)
        print(contents)
    else: 
        ciphers = []
        contents = []

    if elim_msg != "":
        elim_ciphers, elim_contents = fit_message_to_cipher(elim_msg,cipher_table)
        print(elim_ciphers)
        print(elim_contents)
    else:
        elim_ciphers = []
        elim_contents = []

    device_types = getLogDeviceTypes(conn)
    log_levels = getLoggingLevels(conn)

    if len(ciphers) == 0 and message != "":
        print('No Cipher Match Found! Please modify your search message!')
        LogSearchResults = None
        Header = None
    elif len(ciphers)>0 or hid != "" or d2 == "":
        if hid != "":
            print("HID run..")
            SQL_TABLE, site = getLogInfoFromHID(conn,hid)
            LogSearchResults = []
            Header = []
            if SQL_TABLE is not None:
                showstr,wherestr,orderbystr = developSQLquery(order_by,site,deviceID,lid,log_lvl,dev_class,d1,t1,d2,t2,ciphers,contents,elim_ciphers,elim_contents,device_types,log_levels)
                getLogInfoFromTable(conn,LogSearchResults,Header,SQL_TABLE,showstr,wherestr,orderbystr, device_types, log_levels, cipher_table)
        else:
            sites = []
            if site == "All":
                sites = getLogSites(conn)
            else:
                sites.append(site)

            Header,LogSearchResults = getLogSearchResults(conn,order_by,sites,deviceID,lid,log_lvl,dev_class,sw_version,d1,t1,d2,t2,ciphers,contents,elim_ciphers,elim_contents,device_types,log_levels,cipher_table)
    else:
        LogSearchResults = {}
        Header = []

    return Header,LogSearchResults


def getLogSearchResults(conn,order_by,sites,deviceID,lid,log_lvl,dev_class,sw_version,d1,t1,d2,t2,ciphers,contents,elim_ciphers,elim_contents,device_types,log_levels,cipher_table):
    LogSearchResults = []
    Header = []
    variables = []
    start_year,start_month,end_year,end_month = get_month_year(d1,t1,d2,t2)
    #Go thru each schema/site in sites
    print("Starting Parallel Search...")
    threads = []
    q = Queue()
    num_worker_threads = min(30,len(sites))
    for ea_site in sites:
        q.put(ea_site)

    for i in range(num_worker_threads):
        thread = Thread(target=getLogSearchBySite,args=(q,LogSearchResults,Header,order_by,deviceID,lid,log_lvl,dev_class,d1,t1,d2,t2,ciphers,contents,elim_ciphers,elim_contents,start_year,start_month,end_year,end_month,device_types,log_levels,cipher_table))
        thread.start()
        threads.append(thread)

    q.join()
    for t in threads:
        if t.is_alive():
            print('Thread Alive! Contact Administrator.')
    
    print("Search Complete.")

    return Header, LogSearchResults


def getLogSearchBySite(q,LogSearchResults,Header,order_by,deviceID,lid,log_lvl,dev_class,d1,t1,d2,t2,ciphers,contents,elim_ciphers,elim_contents,start_year,start_month,end_year,end_month,device_types,log_levels,cipher_table):
    pgsql_ = get_pgsql_connection('logdb',True)
    wait(pgsql_)
    while not q.empty():
        ea_site = q.get()
        print('Searching {0}...'.format(ea_site))
        try:
            showstr,wherestr,orderbystr = developSQLquery(order_by,ea_site,deviceID,lid,log_lvl,dev_class,d1,t1,d2,t2,ciphers,contents,elim_ciphers,elim_contents,device_types,log_levels)
            schema = "log_tables_" + ea_site.lower()
            tables = getSchemaTables(pgsql_,schema)
            full_table_name_array = getSchemaTableNames(ea_site,start_year,start_month,end_year,end_month,tables)
            #print("Searching {0}...".format(ea_site) 
            for full_table_name in full_table_name_array:
                getLogInfoFromTable(pgsql_,LogSearchResults,Header,full_table_name,showstr,wherestr,orderbystr, device_types, log_levels, cipher_table)

            print('{0} Search Complete.'.format(ea_site))

        except:
                print("Unexpected error:",sql)
        q.task_done()

    pgsql_.close()
    return True


def getSchemaTableNames(ea_site,start_year,start_month,end_year,end_month,tables):
    full_table_name_array = []
    schema = "log_tables_" + ea_site.lower()
    if start_year == end_year:
        for ea_month in range(start_month,end_month+1):
            table_str = ea_site.lower() + "_" + str(start_year) + "_" + str(calendar.month_name[ea_month].lower())
            full_table_name = schema + "." + table_str
            if table_str in tables:
                full_table_name_array.append(full_table_name)

    else: ## Cases when searching over Dec,Jan (12,1) gap
        for ea_year in range(start_year,end_year+1):
            if ea_year == start_year:
                for ea_month in range(start_month,13):
                    table_str = ea_site.lower() + "_" + str(ea_year) + "_" + str(calendar.month_name[ea_month].lower())
                    full_table_name = schema + "." + table_str
                    if table_str in tables:
                        #print(table_str)
                        full_table_name_array.append(full_table_name)
                        
            elif ea_year > start_year and ea_year < end_year:
                for ea_month in range(1,13):
                    table_str = ea_site.lower() + "_" + str(ea_year) + "_" + str(calendar.month_name[ea_month].lower())
                    full_table_name = schema + "." + table_str
                    if table_str in tables:
                        #print(table_str)
                        full_table_name_array.append(full_table_name)
                        
            elif ea_year == end_year:
                for ea_month in range(1,end_month+1):
                    table_str = ea_site.lower() + "_" + str(ea_year) + "_" + str(calendar.month_name[ea_month].lower())
                    full_table_name = schema + "." + table_str
                    if table_str in tables:
                        #print(table_str)
                        full_table_name_array.append(full_table_name)
                        
    return full_table_name_array


def fit_message_to_cipher(message,cipher_table):
    ciphers = []
    contents = []

    messages = message.split(',')
    for ea_message in messages:
        #print(ea_message+"!")
        for ea_cipher in cipher_table:
            ea_cipher[1] = "^"+ea_cipher[1]
            if (re.search(ea_cipher[1],ea_message)) or (re.search(ea_message,ea_cipher[1])):
                ciphers.append(ea_cipher[0])
                contents = get_content(contents,ea_cipher,ea_message)
            elif (re.search(ea_cipher[1],ea_message+" ")):
                ea_message += " "
                ciphers.append(ea_cipher[0])
                contents = get_content(contents,ea_cipher,ea_message)
                    
    return ciphers, contents


def get_content(contents,a_cipher,a_message):
    m = re.search(a_cipher[1],a_message)
    #print(m, a_cipher[1],a_message)
    if m:
        content = [content for content in m.groups()]
        #print(content)
        if len(content) > 0:
            contents.append(content)
        else:
            contents.append(None)
    else:
        contents.append(None)

    return contents


def get_month_year(d1,t1,d2,t2):
    timefmt = "%Y-%m-%d"
    if t2 != "":
        t1+=":00"
        t2+=":00"
        datetimestr2 = d2+" "+t2
        timefmt = "%Y-%m-%d %H:%M:%S"
    elif d2 != "":
        datetimestr2 = d2
    if t1 != "":
        datetimestr1 = d1+" "+t1
        if len(datetimestr1) <18:
            timefmt = "%Y-%m-%d %H:%M"
        else:
            timefmt = "%Y-%m-%d %H:%M:%S"
    else:
        datetimestr1 = d1

    datetimestr_a = datetime.strptime(datetimestr1,timefmt)
    start_year = datetimestr_a.year
    start_month = datetimestr_a.month
    if d2 != "":
        datetimestr_b = datetime.strptime(datetimestr2,timefmt)
        end_month = datetimestr_b.month
        end_year = datetimestr_b.year
    else:
        end_month = start_month
        end_year = start_year

    return start_year,start_month,end_year,end_month


def developSQLquery(order_by,site,deviceID,lid,log_lvl,dev_class,d1,t1,d2,t2,ciphers,contents,elim_ciphers,elim_contents,device_types,log_levels):
    timefmt = "%Y-%m-%d"

    if t2 != "":
        datetimestr2 = d2+" "+t2
    elif d2 != "":
        datetimestr2 = d2
    if t1 != "":
        datetimestr1 = d1+" "+t1
        timefmt = "%Y-%m-%d %H:%M:%S"
    else:
        datetimestr1 = d1

    showstr = ""
    wherestr = ""
    if order_by == "datetime_sent":
        orderbystr = order_by
    else:
        orderbystr = order_by+",datetime_sent"

    if lid == "":
        showstr += "lid,"
        wherestr += "lid IN (SELECT lid FROM pwd_logs WHERE site = '{0}' AND first_ts >= '{1}') AND ".format(site,d1)
    else:
        wherestr += "lid={0} AND ".format(lid)
    showstr+= "'{}' as site,".format(site)
    if deviceID == "":
        showstr += "device_id,"
    else:
        wherestr += "device_id={0} AND ".format(deviceID)
    
    if log_lvl == "":
        showstr += "log_level,"
    else:
        print(log_levels)
        print(device_types)

        log_idx = log_levels.index(log_lvl)
        log_code = log_levels[log_idx]
        wherestr += "log_level='{0}' AND ".format(log_code)
    if dev_class == "":
        showstr += "device_type,"
    else:
        dev_idx = device_types.index(dev_class)
        dev_code = device_types[dev_idx]
        wherestr += "device_type='{0}' AND ".format(dev_code)

    try:
        wherestr += "datetime_sent >= '{0}' AND datetime_sent <= '{1}' ".format(datetimestr1,datetimestr2)
    except:
        timediff = 10 #mins
        timefmt = "%Y-%m-%d %H:%M:%S"
        if len(datetimestr1) < 18:
            #print(datetimestr1)
            datetimestr1 = datetimestr1+":00"
        datetimestr_a = datetime.strptime(datetimestr1,timefmt) - timedelta(minutes=timediff)
        datetimestr_b = datetime.strptime(datetimestr1,timefmt) + timedelta(minutes=timediff)
        wherestr += "datetime_sent > '{0}' AND datetime_sent < '{1}' ".format(datetimestr_a,datetimestr_b)
    
    wherestr = add_cipher_wherestr(wherestr, ciphers, contents, "=")
    wherestr = add_cipher_wherestr(wherestr, elim_ciphers, elim_contents, "!=")
    showstr += "to_char(datetime_sent,'YYYY-MM-DD HH24:MI:SS') as datetime_sent,"
    showstr += "sq_num,"
    showstr += "cipher_id,message_content"

    return showstr,wherestr,orderbystr


def add_cipher_wherestr(wherestr, ciphers, contents, symbol):
    if len(ciphers)>0:
        wherestr += """
            AND ("""
        #print(ciphers)
        for i in range(len(ciphers)):
            ea_cipher = ciphers[i]
            wherestr += "(cipher_id "+str(symbol)+" "+str(ea_cipher)+" "
            ea_content = contents[i]
            if ea_content is not None:
                if len(ea_content) > 1:
                    wherestr += "AND "
                    for j in range(len(ea_content)):
                        if ea_content[j] is not None and ea_content[j] != "":
                            wherestr += "message_content[{0}] ".format(j+1)+str(symbol)+" '{0}' AND ".format(ea_content[j])
                    wherestr = wherestr[:-5]
                else:
                    wherestr += "AND '{0}' "+str(symbol)+" ANY (message_content)".format(ea_content[0])
            if symbol == "=":
                wherestr += """) 
                    OR """
            elif symbol == "!=":
                wherestr += """)
                    AND """

        wherestr = wherestr[:-4] + ")"

    #print(wherestr)
    return wherestr


def getLogInfoFromTable(conn,LogSearchResults,Header,full_table_name,showstr,wherestr,orderbystr,device_types,log_levels,cipher_table):
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        #print(full_table_name)
        sql_get_msg = """
        SELECT {0}
        FROM {1}
        WHERE {2}
        ORDER BY {3};
        """.format(showstr,full_table_name,wherestr,orderbystr)
        #print(sql_get_msg)
        #print("Querying Log Database...")
        cur.execute(sql_get_msg)
        wait(cur.connection)
        field_names = [i[0] for i in cur.description]
        if Header == []:
            Header.extend(field_names[:])
        SearchResults = cur.fetchall()
        #print(SearchResults)
    if len(SearchResults) > 0:
        #print("Reconstructing Log Messages...")
        SearchResults = reconstruct_log_messages(SearchResults, field_names, device_types, log_levels, cipher_table)
        LogSearchResults += SearchResults
        #print(LogSearchResults)


def reconstruct_log_messages(SearchResults, field_names, device_types, log_levels, cipher_table):
    for line in SearchResults:
        cipher_id = line[-2]
        msg_content = line[-1]
        for ea_cipher in cipher_table:
            if cipher_id == ea_cipher[0]:
                cipher = ea_cipher[1]
                break

        if "log_level" in field_names:
            idx = field_names.index("log_level")
            for lvl in log_levels:
                if lvl[1] == line[idx]:
                    line[idx] = lvl[0]

        if "device_type" in field_names:
            idx = field_names.index("device_type")
            for dtype in device_types:
                if dtype[1] == line[idx]:
                    line[idx] = dtype[0]
        
        #print(msg_content)           
        msg = cipher[:]
        for ea_content in msg_content:
            msg = re.sub(r'\(\[([-AZFazf09.s_\\]*)\]\*\)',ea_content,msg,count=1)
            
        msg = re.sub(r'\$','',msg)
        msg = re.sub(r'\\','',msg)
        msg = re.sub(r'\^','',msg)

        actual_message = msg
        line[-1] = actual_message
        #line[-2] = cipher

    return SearchResults


def getLogSites(conn):
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        sql = "SELECT schema_name FROM information_schema.schemata WHERE schema_name ~ 'log_tables_site';"
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(sql)
        wait(cur.connection)
        schemas = cur.fetchall()

    sites = []
    for ea_schema in schemas:
        name_str = ea_schema[0].split('_')
        site_name = name_str[2]
        site_name = "Site"+site_name[4:].upper()
        sites.append(site_name)

    return sites


def getSchemaTables(conn,schema):
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        sql = """
        SELECT relname
        FROM pg_class, pg_namespace
        WHERE relnamespace = pg_namespace.oid
            AND nspname = '{}'
            AND relkind = 'r';""".format(schema)

        cur.execute(sql)
        wait(cur.connection)
        tables = cur.fetchall()

    #print("Got Schema Tables.")
    return [table[0] for table in tables]


def getLogMessageTypes(conn):
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        sql = """
                SELECT

                """
        #print(sql)
        cur.execute(sql)
        data = cur.fetchall()

    return data


def getLoggingLevels(conn):
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        sql = "SELECT cipher, code FROM log_level_cipher ORDER BY code"
        #print(sql)
        cur.execute(sql)
        data = cur.fetchall()

    return data


def getLogDeviceTypes(conn):
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        sql = "SELECT cipher, code FROM device_type_cipher ORDER BY code"
        #print(sql)
        cur.execute(sql)
        data = cur.fetchall()

    return data


def getLogSoftwareVersions(conn):
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:

        sql = """
                WITH recursive t(n) AS (
                SELECT MIN(device_type) FROM log_messages
                UNION ALL
                SELECT (SELECT log_messages.device_type FROM log_messages WHERE log_messages.device_type > n
                ORDER BY log_messages.device_type LIMIT 1)
                FROM t WHERE n is not null
                )
                SELECT n from t
                """
        #print(sql)
        cur.execute(sql)
        data = cur.fetchall()

    return data[:,-1]


def getCipherTable(conn):
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        sql_ciphers = """
        SELECT id, cipher
        FROM message_ciphers
        ORDER BY cipher
        """
        cur.execute(sql_ciphers)
        cipher_table = cur.fetchall()

    return cipher_table


def getLogInfoFromHID(conn,HID):
    scidb_ = get_pgsql_connection()
    scidb_cur = scidb_.cursor(cursor_factory=psycopg2.extras.DictCursor)

    SQL_Hid_Ea_Device = []
    sql_determine_site = """
    SELECT site
    FROM aa_session_data
    WHERE hid = {}""".format(HID)
    scidb_cur.execute(sql_determine_site)
    site = scidb_cur.fetchone()

    if site != None:
        site = site[0]

        sql_get_time_zone = """
        SELECT time_zone
        FROM aa_site_data
        WHERE name = '{}'""".format(site)
        scidb_cur.execute(sql_get_time_zone)
        time_zone = scidb_cur.fetchone()
        time_zone = time_zone[0]

        sql_get_hid_info = """
        SELECT hid, block_number, device_id, unix_start, unix_stop
        FROM aa_blocks
        WHERE hid = {} AND device_id is not null
        ORDER BY block_number""".format(HID)
        scidb_cur.execute(sql_get_hid_info)
        hid_info = scidb_cur.fetchall()
        scidb_.close()

        search_criteria = []
        device_id = 0
        i = -1
        for block in hid_info:
            if block[2] == device_id:
                search_criteria[i][2] = get_string_from_timestamp(block[4], time_zone, fmt= '%Y-%m-%d %H:%M:%S')
            else:
                i += 1
                device_id = block[2]
                start_str = get_string_from_timestamp(block[3], time_zone, fmt= '%Y-%m-%d %H:%M:%S')
                end_str = get_string_from_timestamp(block[4], time_zone, fmt= '%Y-%m-%d %H:%M:%S')
                search_criteria.append([device_id,start_str,end_str])
                
        schema = "log_tables_{}".format(site.lower())
        tables = getSchemaTables(conn,schema)
        #print(search_criteria)
        for device_session in search_criteria:
            t1 = datetime.strptime(device_session[1],'%Y-%m-%d %H:%M:%S')
            t2 = datetime.strptime(device_session[2],'%Y-%m-%d %H:%M:%S')
            start_year = t1.year
            start_month = t1.month
            end_year = t2.year
            end_month = t2.month
            deviceID = device_session[0]

            full_table_name_array = getSchemaTableNames(site,start_year,start_month,end_year,end_month,tables)
            #print(full_table_name_array)
            for full_table_name in full_table_name_array:
                SQL_Hid_Ea_Device = getLogInfoForDevice(SQL_Hid_Ea_Device,site,full_table_name,deviceID,t1,t2)

    if len(SQL_Hid_Ea_Device) > 0:
        SQL_TABLE = "("
        for sql in SQL_Hid_Ea_Device:
            SQL_TABLE = SQL_TABLE+"("+sql+") UNION ALL "

        if len(SQL_TABLE) > 11:
            SQL_TABLE = SQL_TABLE[:-11]
            SQL_TABLE += ") as HID_Table"
    else:
        SQL_TABLE = None

    #print(SQL_TABLE)
    return SQL_TABLE, site


def getLogInfoForDevice(SQL_Hid_Ea_Device,site,full_table_name,deviceID,t1,t2):
        #print(full_table_name)
        sql_get_log_info_device = """
        SELECT  '{0}' as site,
                lid,
                device_id,
                sq_num,
                log_level,
                device_type,
                datetime_sent,
                cipher_id,
                message_content
        FROM {1}
        WHERE device_id = {2} AND datetime_sent BETWEEN '{3}' AND '{4}'
        ORDER BY datetime_sent""".format(site,full_table_name,deviceID,t1,t2)
        
        SQL_Hid_Ea_Device.append(sql_get_log_info_device)

        return SQL_Hid_Ea_Device


def findModules(Header,LogSearchResults,deviceID,hid):
    sql4Modules = []
    t_idx = Header.index("datetime_sent")
    id_idx = Header.index("device_id")
    start_idx = 0
    counter = 0
    sql = ""
    for ea_result in LogSearchResults:
        counter += 1
        device_id = ea_result[id_idx]
        datetime = ea_result[t_idx]

        sql += """
        SELECT(
            SELECT array_agg(module)
            FROM log_modules
            WHERE device_id = {0} AND dt_start <= '{1}' and dt_end >= '{1}' ) as module
        """.format(device_id,datetime)
        sql+= "UNION ALL"

        if counter % 1000 == 0:
            sql = sql[0:-9]
            #print(sql)
            sql4Modules.append([sql,start_idx,counter])
            start_idx += 1000
            sql = ""

    if sql != "":
        sql = sql[0:-9]
        sql4Modules.append([sql,start_idx,counter])

    return sql4Modules


def getModules(Header,LogSearchResults,deviceID,hid):
    Header.append('modules')
    sql4Modules = findModules(Header,LogSearchResults,deviceID,hid)
    threads = []
    if sql4Modules != []:
        q = Queue()
        num_worker_threads = min(10,len(LogSearchResults))
        for mods in sql4Modules:
            q.put(mods)

        for i in range(num_worker_threads):
            thread = Thread(target=worker, args=(q,LogSearchResults))
            thread.start()
            threads.append(thread)

        q.join()
        for t in threads:
            if t.is_alive():
                print('Thread Alive! Contact Administrator.')


def worker(q,LogSearchResults):
    pgsql_ = get_pgsql_connection('logdb',True)
    wait(pgsql_)
    while not q.empty():
        mods = q.get()
        try:
            appendModules(pgsql_,mods,LogSearchResults)
        except:
            print("Unexpected error:",sql)
        q.task_done()
    pgsql_.close()
    return True


def appendModules(pgsql_,sql4Modules,LogSearchResults):
    sql = sql4Modules[0]
    start_idx = sql4Modules[1]
    counter = sql4Modules[2]
    with pgsql_.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(sql)
        wait(cur.connection)
        modules = cur.fetchall()
        #print(start_idx, counter)
    for i in range(start_idx,counter):
        LogSearchResults[i].append(modules[i-start_idx])


def wait(conn):
    while 1:
        state = conn.poll()
        if state == psycopg2.extensions.POLL_OK:
            break
        elif state == psycopg2.extensions.POLL_WRITE:
            select.select([], [conn.fileno()], [])
        elif state == psycopg2.extensions.POLL_READ:
            select.select([conn.fileno()], [], [])
        else:
            raise psycopg2.OperationalError("poll() returned %s" % state)


def fullLogCSVfile(Header,LogSearchResults):
    if debugMode != True:
        path = '/home/ec2-user/science_server/static/dl/log_search_data.csv'
    else:
        path = '/Users/smccombie/Documents/sotera/Programming/SpittinOutRandom/static/dl/log_search_data.csv'  #for debug purposes

    # delete file if an older version is there
    os.remove(path)
    #print(LogSearchResults)
    ## now write local csv file
    orderedData = []
    n = 0
    with open(path, 'wb') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=Header)
        for row in LogSearchResults:
            orderedData.append({})
            for i in range(len(Header)):
                orderedData[n][Header[i]] = str(row[i])
            n+=1
        #orderedData = [{key:str(row[key]) for key in Header} for row in LogSearchResults]
        writer.writeheader()
        for row in orderedData:
            writer.writerow(row)


