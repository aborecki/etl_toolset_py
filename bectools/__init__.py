import logging
import sys
import warnings

from pyetltools import *

import bectools.tools.recon as recon


logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logging.getLogger().setLevel(logging.DEBUG)
logging.getLogger().setLevel(logging.WARNING)
logging.info(__name__ + "__init__.py")

import bectools.bec_config

try:
    import bectools.bec_config
except:
    print("WARNING: Default configuration not found. Place bec_config module in the modules search path to use default config.")

try:
    import bec_passwords
except:
    print(
            "WARNING: Default passwords configuration not found. Place bec_passwords module in the modules search path to use default passwords config.")
#try:
#    import bectools.bec_datasets
#except:
#    print(
#            "WARNING: Default datasets configuration not found. Place bec_datasets module in the modules search path to use default datasets config.")





