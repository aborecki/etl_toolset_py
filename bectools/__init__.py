import logging
import sys

from pyetltools.core.connector import connectors

import pyetltools.data.pandas.pandas_helper

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logging.getLogger().setLevel(logging.DEBUG)
logging.getLogger().setLevel(logging.WARNING)
logging.info(__name__ + "__init__.py")

import bectools.bec_config
import bectools.bec_passwords
import bectools.bec_datasets





