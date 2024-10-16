import os
import warnings
from configparser import ConfigParser

toy = 'bar'


def setup_resources():
    resources = {}
    for fn in (os.path.expanduser('~/.higgins'), '/etc/higgins', '/tmp/higgins'):
        if os.path.exists(fn):
            config = ConfigParser()
            config.read(fn)
            for section in config.sections():
                resources[section] = {}
                for k, v in config.items(section):
                    resources[section][k] = v
        break
    return resources

    # for fn in (os.path.expanduser('~/.higgins_api_key'),'/etc/higgins_api_key','/tmp/higgins_api_key'):
    #     if os.path.exists(fn):
    #         with open(fn) as fp:
    #             key = fp.readline().strip()
    #         return { 'api': {'key': key } }

    # warnings.warn("Cannot find higgins file. API and download functions will be disabled ",ImportWarning)

    # # add a pgbouncer if sciencedb2 is there
    # if 'sciencedb2' in resources.keys() and 'pgbouncer' not in resources.keys():
    #     resources['pgbouncer'] = { 'host': 'localhost',
    #                                'port': '5432',
    #                                'database': resources['sciencedb2']['database'],
    #                                'user':     resources['sciencedb2']['user'],
    #                                'password': resources['sciencedb2']['password'] }


resources = setup_resources()

try:
    from . import aws
except ImportError:
    warnings.warn("Unable to import sotera.aws",ImportWarning)

try:
    from . import util
except ImportError:
    warnings.warn("Unable to import sotera.util", ImportWarning)

try:
    from . import api
except ImportError:
    warnings.warn("Unable to import sotera.api", ImportWarning)

try:
    from . import io
except ImportError:
    warnings.warn("Unable to import sotera.io", ImportWarning)

try:
    from . import db
except ImportError:
    warnings.warn("Unable to import sotera.db", ImportWarning)

try:
    from . import analysis
except ImportError:
    warnings.warn("Unable to import sotera.analysis", ImportWarning)

try:
    from . import cluster
except ImportError:
    warnings.warn("Unable to import sotera.analysis", ImportWarning)
