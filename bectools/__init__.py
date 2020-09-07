import logging
import sys
import warnings

from pyetltools import *

import bectools.tools.recon as recon
import bectools.tools.init as init
import bectools.tools.metadata.release_model as release_model
import bectools.tools.metadata.db_metadata as db_metadata
import bectools.tools.metadata.wf_relations as wf_relations

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logging.getLogger().setLevel(logging.DEBUG)
logging.getLogger().setLevel(logging.WARNING)
logging.getLogger("bectools").setLevel("INFO")
logging.info(__name__ + "__init__.py")
logger = logging.getLogger("bectools")

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
try:
    import bectools.bec_datasets
except:
    print(
            "WARNING: Default datasets configuration not found. Place bec_datasets module in the modules search path to use default datasets config.")





