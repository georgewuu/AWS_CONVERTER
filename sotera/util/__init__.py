import os
import warnings
from .time import get_timestamp, get_string_from_timestamp  # noqa F401
from .misc import *  # noqa F401


def setup_resources():

    for fn in (
        os.path.expanduser("~/.higgins_api_key"),
        "/etc/higgins_api_key",
        "/tmp/higgins_api_key",
    ):
        if os.path.exists(fn):
            with open(fn) as fp:
                key = fp.readline().strip()
            return {"api_key": key}

    warnings.warn(
        "Cannot find higgins file. API and download functions will be disabled ",
        ImportWarning,
    )
