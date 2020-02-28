import logging
import sys

from pyetltools.core import connector

import pyetltools.data.pandas.pandas_helper

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logging.getLogger().setLevel(logging.DEBUG)
logging.getLogger().setLevel(logging.WARNING)
logging.info(__name__ + "__init__.py")

connector.load_config()
connectors = connector.connectors





