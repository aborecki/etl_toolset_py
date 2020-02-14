from neo4j import GraphDatabase
from py2neo import Graph

from pyetltools.core.connector import Connector
from pandas import DataFrame

from pyetltools.neo4j.scripts import neo4jupyter



class NEO4JConnector(Connector):

    def __init__(self, key, host, port, username, password=None):
        super().__init__(key=key)
        self.host = host
        self.port = port
        self.username = username
        self.password= password

    def get_url(self):
        return str(self.host) + ":" + str(self.port)

    def __init__(self, config):
        super().__init__(config)
        self.driver = GraphDatabase.driver("bolt://" + self.get_url(),
                                           auth=(self.username, self.get_password()))

    def get_graph(self):
        return Graph(self.get_url(), auth=(self.username, self.get_password()))

    def get_session(self):
        return self.driver.session()

    def draw_graph(self, graph, options={}):
        return neo4jupyter.draw(graph, options)

    def draw_graph_data(self, data, options={}):
        return neo4jupyter.draw_data(data, options)

    def run_query_as_pandas_df(self, query, columns):
        return DataFrame(self.get_graph().run(query), columns=columns);


