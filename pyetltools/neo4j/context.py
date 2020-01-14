from pyetltools.core import context
from pyetltools.core.context import Context
from pyetltools.neo4j.config import NEO4JConfig
from pyetltools.neo4j.connection import NEO4JConnection
from pandas import DataFrame

class NEO4JContext(Context):

    def __init__(self, config: NEO4JConfig, connection: NEO4JConnection):
        self.config = config
        self.connection = connection

    def get_graph(self):
        return self.connection.get_graph()

    def run_query_as_pandas_df(self, query):
        return DataFrame(self.get_graph().run(query));

    def get_session(self):
        return self.connection.get_session()

    @classmethod
    def create_from_config(cls, config: NEO4JConfig):
        return NEO4JContext(config, NEO4JConnection(config))

    @classmethod
    def register_context_factory(cls):
        context.register_context_factory(NEO4JConfig, NEO4JContext)
