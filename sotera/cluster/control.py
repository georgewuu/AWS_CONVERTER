import gc
import cloudpickle
import traceback
import getpass
import datetime
from pathlib import Path
from collections.abc import Iterable
from re import compile as re_compile
from numpy import __name__ as np__name__, ndarray
from .. import aws
from psycopg2.extensions import QuotedString
import json

try:
    import StringIO as io

    def get_quoted(s):
        return QuotedString(s).getquoted()
except ImportError:
    import io

    def get_quoted(s):
        return QuotedString(s).getquoted().decode("utf-8")


def add_analysis(pgsql_, name, owner=None):
    """Adds a new analysis and returns aid.
       Stores date_start and date_stop as UTC."""

    if owner is None:
        owner = getpass.getuser()
    with pgsql_, pgsql_.cursor() as cursor:
        cursor.execute(
            f""" INSERT
                   INTO analysis_history (name,owner)
                 VALUES ('{name}','{owner}')
              RETURNING aid"""
        )
        return cursor.fetchone()[0]


def start_analysis(pgsql_, aid):
    """Sets date_start to now() for given analysis.
       Call this when processing begins."""
    with pgsql_, pgsql_.cursor() as cursor:
        cursor.execute(
            f"""UPDATE analysis_history
                   SET date_start =  '{str(datetime.now())}'
                 WHERE aid = {aid}"""
        )


def finish_analysis(pgsql_, aid):
    """ sets date_stop to now() for given analysis.
        Call this when completely done processing. """
    with pgsql_, pgsql_.cursor() as cursor:
        cursor.execute(
            f"""UPDATE analysis_history
                   SET date_stop =  '{str(datetime.now())}'
                 WHERE aid = {aid}"""
        )


def reset_analysis(pgsql_, aid):
    """ resets all jobs for given analysis: clears traceback and paylod,
        and sets is_error and is_complete to false. """
    with pgsql_, pgsql_.cursor() as cursor:
        cursor.execute(
            f"""UPDATE analysis_jobs
                   SET returns = DEFAULT,
                       is_complete  = DEFAULT,
                       is_error = DEFAULT,
                       traceback = DEFAULT
                 WHERE aid = {aid}"""
        )


def job_generator(
    pgsql_, aid, is_complete=False, is_error=False, radmonize=True, reset=False
):
    if reset:
        reset_analysis(pgsql_, aid)

    with pgsql_, pgsql_.cursor() as cursor:
        cursor.execute(
            f"""SELECT jobid
                  FROM analysis_jobs
                 WHERE aid = {aid}
                   AND is_complete = {is_complete}
                   AND is_error = {is_error}
            {'ORDER BY random()' if radmonize else ' '  } """
        )
        for r in cursor:
            yield aid, r[0]


def make_job(pgsql_, aid, args):
    """ Adds single job to a given analysis.  Expects args to be json. """
    with pgsql_, pgsql_.cursor() as cursor:
        cursor.execute(
            f""" INSERT
                   INTO analysis_jobs (aid,args)
                 VALUES ({aid},'{json.dumps(args)}')
              RETURNING jobid"""
        )
        return cursor.fetchone()[0]


def get_job(pgsql_, aid, jobid):
    """ gets args for specified job """
    with pgsql_, pgsql_.cursor() as cursor:
        cursor.execute(
            f""" SELECT args
                   FROM analysis_jobs
                  WHERE aid = {aid}
                    AND jobid = {jobid}"""
        )
        return cursor.fetchone()[0]


def get_results(pgsql_, aid, jobid):
    """ gets args for specified job """
    with pgsql_, pgsql_.cursor() as cursor:
        cursor.execute(
            f""" SELECT is_complete, returns, is_error, traceback
                   FROM analysis_jobs
                  WHERE aid = {aid}
                    AND jobid = {jobid}"""
        )
        return cursor.fetchone()


def job_complete(pgsql_, aid, jobid, returns):
    """ Updates returns, sets is_complete to true, is_error to false,
        and clears traceback. Expects returns to be a object that
        serializes to json using json.dumps."""

    sql = f""" UPDATE analysis_jobs
                  SET returns = '{json.dumps(returns)}',
                      is_complete = 'true',
                      is_error = 'false',
                      traceback = ''
                WHERE aid = {aid}
                      AND jobid = {jobid} """

    with pgsql_, pgsql_.cursor() as cursor:
        cursor.execute(sql)


def job_complete_from_string(pgsql_, aid, jobid, returns_str):
    """ updates returns, sets is_complete to true, is_error to false,
        and clears traceback. Expects returns to be a object that
        serializes to json using json.dumps."""

    sql = f""" UPDATE analysis_jobs
                  SET returns = '{returns_str}',
                      is_complete = 'true',
                      is_error = 'false',
                      traceback = ''
                WHERE aid = {aid}
                      AND jobid = {jobid} """

    with pgsql_, pgsql_.cursor() as cursor:
        cursor.execute(sql)


def _handle_exception():
    fp = io.StringIO()
    traceback.print_exc(file=fp)
    errorstr = fp.getvalue()
    fp.close()
    return errorstr


def job_handle_exception(pgsql_, aid, jobid, returns):
    """ sets traceback, sets is_complete to false, is_error to true,
        and updates returns """

    if pgsql_.closed:
        pgsql_ = aws.get_pgsql_connection()
    else:
        pgsql_.rollback()

    sql = f""" UPDATE analysis_jobs
                  SET traceback = {get_quoted(_handle_exception())},
                      is_complete = false,
                      is_error = true,
                      returns = '{json.dumps(returns)}'
                WHERE aid = {aid}
                      AND jobid = {jobid} """

    with pgsql_, pgsql_.cursor() as cursor:
        cursor.execute(sql)


def get_analysis_status(pgsql_, aid):
    sql = f"""SELECT is_complete, is_error, count(*)
                FROM analysis_jobs
               WHERE aid = {aid}
            GROUP BY is_complete, is_error"""

    with pgsql_, pgsql_.cursor() as cursor:
        cursor.execute(sql)
        vals = cursor.fetchall()
    total = sum([x[2] for x in vals])
    complete = sum([x[2] for x in vals if x[0]])
    error = sum([x[2] for x in vals if x[1]])
    return {
        "complete": complete,
        "error": error,
        "executed": complete + error,
        "total": total,
        "unfinished": (complete + error) - total,
    }


def dumps(value):

    regex = re_compile(r'(NaN|-Infinity|Infinity)')

    def default(obj):
        if type(obj).__module__ == np__name__:
            if isinstance(obj, ndarray):

                return obj.tolist()
            else:
                return obj.item()
        raise TypeError('Unknown type:', type(obj))

    return regex.sub(
        r'"\g<1>"',
        json.dumps(
            value,
            default=default
        )
    )


class cluster_decorate(object):
    def __init__(
        self, pgsql_profile="pgbouncer", code=None, include=None, **kwargs
    ):
        """Initialize cluster_decorate object.

        Parameters
        ----------
        pgsql_profile (optional) : string
            The profile to pass to get_pgsql_connection().
            Defaults to "pgbouncer".
        code (optional) : string, optional
            A block of code in a string that will be passed to exec().
            Defaults to None.
        include (optional): instance or iterable of string, pathlib.Path
            A filename or list of filenames of containing python code to be
            loaded and passed to exec(). Defaults to None
        **kwargs:
            All other keyword arguments will be packaged with cloudpickle and
            sent to the cluster where they will be unpickled.

        """

        def load_code(fn):
            code = None
            if fn is not None and Path(fn).exists():
                with open(fn) as fp:
                    code = "".join(fp.readlines())
                    exec(code)
            return code

        self.pgsql_profile = pgsql_profile
        self.code = code
        if self.code is not None:
            exec(self.code)
        self.pickled = {k: cloudpickle.dumps(v) for k, v in kwargs.items()}
        self.include_code = None
        if isinstance(include, (str, Path)):
            self.include_code = load_code(include)
        elif isinstance(include, Iterable):
            self.include_code = "\n".join([load_code(fn) for fn in include])

    def __call__(self, func):
        def wrapper(arg):
            aid, jobid = arg
            pgsql_ = aws.get_pgsql_connection(self.pgsql_profile)
            try:
                job = get_job(pgsql_, aid, jobid)
                if self.pickled:
                    globals().update(
                        {k: cloudpickle.loads(v) for k, v in self.pickled.items()}
                    )
                if self.code is not None:
                    exec(self.code, globals())
                if self.include_code is not None:
                    exec(self.include_code, globals())
                return_str = dumps(func(pgsql_, aid, jobid, job))
            except:  # noqa E722
                job_handle_exception(pgsql_, aid, jobid, None)
            else:
                try:
                    job_complete_from_string(pgsql_, aid, jobid, return_str)
                except:   # noqa E722
                    job_handle_exception(pgsql_, aid, jobid, None)
            finally:
                # do any clean up here
                pgsql_.close()
                gc.collect()
            return True

        return wrapper
