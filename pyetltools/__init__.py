import logging
logger = logging.getLogger(__name__)
logger.setLevel("INFO")

def get_default_logger():
    return logger

import pyetltools.data.pandas.tools as pandas_tools

try:
    import pyetltools.data.spark.tools as spark_tools
except:
    pass

try:
    import pyetltools.tools.misc as misc_tools
except:
    pass

try:
    import pyetltools.tools.test as test_tools
except:
    pass