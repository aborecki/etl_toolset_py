from pyetltools.data.dataset_manager import DatasetManagerContext


def init():
    import logging, sys
    from  pyetltools.core import context
    import pyetltools.config
    from pyetltools.bec.data.context import BECDBContext

    from pyetltools.data.spark.context import SparkContext

    from pyetltools.jira.context import JiraContext

    import pyetltools.bec
    from pyetltools.neo4j.context import NEO4JContext

    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    logging.getLogger().setLevel(logging.DEBUG)
    logging.getLogger().setLevel(logging.WARNING)

    logging.info(__name__ + "init.py")

    context = context
    sql = pyetltools.bec.data.sql

    # DBContext.register_context_factory()
    # JenkinsContext.register_context_factory()
    JiraContext.register_context_factory()
    BECDBContext.register_context_factory()
    SparkContext.register_context_factory()
    NEO4JContext.register_context_factory()
    DatasetManagerContext.register_context_factory()
    context.set_attributes_from_config()

    import pandas;

    pandas.set_option('display.max_rows', 200)
    pandas.set_option('display.max_colwidth', 1000)
    pandas.set_option('display.max_columns', 1000)
    pandas.set_option('display.width', 1000)
