import warnings
import boto3
import psycopg2
from time import sleep
from .. import resources as __resources__

try:
    __tunnel_active__ = __resources__["tunnel"]["active"].lower() == "true"
except:
    __tunnel_active__ = False

try:
    __access_key_id__ = __resources__["aws"]["aws_access_key_id"]
except:
    __access_key_id__ = None

try:
    __secret_access_key__ = __resources__["aws"]["aws_secret_access_key"]
except:
    __secret_access_key__ = None

try:
    __region__ = __resources__["aws"]["region"]
except:
    __region__ = "us-west-1"

try:
    if __tunnel_active__:
        __pgsql_host__ = "localhost"
    else:
        __pgsql_host__ = __resources__["sciencedb2"]["host"]
except:
    __pgsql_host__ = None

try:
    __pgsql_port__ = int(__resources__["sciencedb2"]["port"])
except:
    __pgsql_host__ = 5432

try:
    __pgsql_database__ = __resources__["sciencedb2"]["database"]
except:
    __pgsql_database__ = "sciencedb2"

try:
    __pgsql_user__ = __resources__["sciencedb2"]["user"]
except:
    __pgsql_user__ = "sotera"

try:
    __pgsql_password__ = __resources__["sciencedb2"]["password"]
except:
    __pgsql_password__ = None


def get_ses_connection():
    if __access_key_id__ is None or __secret_access_key__ is None:
        raise ValueError("aws access keys not set")
    ses_ = boto3.client(
        'ses',
        region_name ="us-west-2",
        aws_access_key_id=__access_key_id__,
        aws_secret_access_key=__secret_access_key__,
    )
    return ses_

def get_s3_connection():
    if __access_key_id__ is None or __secret_access_key__ is None:
        raise ValueError("aws access keys not set")
    s3_ = boto3.client('s3',
                       aws_access_key_id=__access_key_id__,
                       aws_secret_access_key=__secret_access_key__)
    return s3_


def get_boto3_session(region_name="us-west-1"):
    if __access_key_id__ is None or __secret_access_key__ is None:
        raise ValueError("aws access keys not set")
    session = boto3.session.Session(
        region_name=region_name,
        aws_access_key_id=__access_key_id__,
        aws_secret_access_key=__secret_access_key__,
    )
    return session


def get_pgsql_dsn(profile="sciencedb2", style="A"):
    if style == "A":
        dsn = "postgres://{user}:{password}@{host}:{port}?dbname={database}"
    else:
        dsn = (
            "dbname={database} user={user} password={password} host={host} port={port}"
        )
    if profile not in __resources__.keys() and "pgsql" in __resources__.keys():
        profile = "pgsql"
    return dsn.format(**__resources__[profile])


def get_pgsql_connection(profile="sciencedb2"):
    pgsql_ = None
    not_connected = True
    n = 0

    __resources__ = {
        "sciencedb2": {
            "host": "sciencedb2.cdgk3spxjflp.us-west-1.rds.amazonaws.com",
            "database": "sciencedb2",
            "user": "andyk",
            "password": "su88pgZwxD43BqCB",
            "port": "5432"
        }
    }

    if profile not in __resources__.keys() and "pgsql" in __resources__.keys():
        profile = "pgsql"

    while not_connected and n < 10:
        try:
            pgsql_ = psycopg2.connect(
                host=__resources__[profile]["host"],
                port=int(__resources__[profile]["port"]),
                dbname=__resources__[profile]["database"],
                user=__resources__[profile]["user"],
                password=__resources__[profile]["password"],
            )
        except:
            sleep(5)
            n += 1
        else:
            not_connected = False
    return pgsql_


try:
    from . import ipcluster
except ImportError:
    warnings.warn(
        "ipcluster failed to import ... no cluster tools available", ImportWarning
    )
