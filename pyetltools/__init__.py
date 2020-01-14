import logging, sys

import pyetltools
import pyetltools.config
from pyetltools.bec.data.bec_db_context import BECDBContext

from pyetltools.data.spark import spark_helper
from pyetltools.data.core.context import DBContext
from pyetltools.data.spark.context import SparkContext
from pyetltools.jenkins.context import JenkinsContext
from pyetltools.jira import context
from pyetltools.jira.context import JiraContext

import pyetltools.bec
from pyetltools.neo4j.context import NEO4JContext

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logging.getLogger().setLevel(logging.DEBUG)
logging.getLogger().setLevel(logging.WARNING)

logging.info(__name__+":__init__.py")

context = pyetltools.core.context

spark_helper= spark_helper

def get_spark_session():
    return spark_helper.get_spark_session()


#DBContext.register_context_factory()
#JenkinsContext.register_context_factory()
JiraContext.register_context_factory()
BECDBContext.register_context_factory()
SparkContext.register_context_factory()
NEO4JContext.register_context_factory()
context.set_attributes_from_config()

import pandas;

pandas.set_option('display.max_rows', 200)
pandas.set_option('display.max_colwidth',1000)
pandas.set_option('display.max_columns',1000)
pandas.set_option('display.width', 1000)





