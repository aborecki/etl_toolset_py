import logging
logger = logging.getLogger(__name__)
logger.setLevel("INFO")

def get_default_logger():
    return logger



import pyetltools.data.db_tools.db_metadata
