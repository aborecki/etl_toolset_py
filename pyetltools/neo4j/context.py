from pyetltools.core import context
from pyetltools.core.context import Context
from pyetltools.neo4j.config import NEO4JConfig
from pyetltools.neo4j.connection import NEO4JConnection
from pandas import DataFrame

from pyetltools.neo4j.scripts import neo4jupyter



class NEO4JContext(Context):

    def __init__(self, config: NEO4JConfig, connection: NEO4JConnection):
        self.config = config
        self.connection = connection

    def get_graph(self):
        return self.connection.get_graph()

    def draw_graph(self, graph, options={}):
        return neo4jupyter.draw(graph, options)

    def draw_graph_data(self, data, options={}):
        return neo4jupyter.draw_data(data, options)

    def run_query_as_pandas_df(self, query, columns):
        return DataFrame(self.get_graph().run(query), columns=columns);

    def get_session(self):
        return self.connection.get_session()

    @classmethod
    def create_from_config(cls, config: NEO4JConfig):
        return NEO4JContext(config, NEO4JConnection(config))

    @classmethod
    def register_context_factory(cls):
        context.register_context_factory(NEO4JConfig, NEO4JContext)
