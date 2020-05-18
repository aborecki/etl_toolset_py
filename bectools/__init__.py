import logging
import sys

from pyetltools.core import connector

import pyetltools.data.pandas.pandas_helper

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logging.getLogger().setLevel(logging.DEBUG)
logging.getLogger().setLevel(logging.WARNING)
logging.info(__name__ + "__init__.py")
import bectools.pyetltools_config
import bectools.pyetltools_passwords
import bectools.pyetltools_config_datasets
connector.validate_config()
connectors = connector.connectors





