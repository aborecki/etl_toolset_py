from neo4j import GraphDatabase
from py2neo import Graph

from pyetltools.core.connector import Connector
from pandas import DataFrame

from pyetltools.neo4j.scripts import neo4jupyter



class NEO4JConnector(Connector):

    def __init__(self, key, host, port=None, username=None, password=None, routing=False):
        super().__init__(key)
        self.host = host
        self.port = port
        self.username = username
        self.password= password
        self.driver=None
        self.routing=routing

    def get_url(self):
        return str(self.host) +   ((":" + str(self.port)) if self.port is not None else "")

    def get_driver(self):
        if self.driver is None:
            self.driver = GraphDatabase.driver(  self.get_url(),
                                           auth=(self.username, self.get_password()))
        return self.driver

    def get_graph(self):
        return Graph(self.get_url(), auth=(self.username, self.get_password()), routing=self.routing)

    def get_session(self):
        return self.get_driver().session()

    def draw_graph(self, graph, options={}):
        return neo4jupyter.draw(graph, options)

    def draw_graph_data(self, data, options={}):
        return neo4jupyter.draw_data(data, options)

    def run_query_as_pandas_df(self, query, columns):
        return DataFrame(self.get_graph().run(query), columns=columns);

    def validate_config(self):
        pass

    def get_import_directory(self):
        try:
            self.get_graph().run("""LOAD CSV WITH HEADERS FROM "File:///xxxxxxxxxxx.csv" AS row WITH row 
            MERGE (w:X)""")
        except Exception as e:
            dirname = str(e).replace(
                "[Statement.ExternalResourceFailed] Couldn't load the external resource at: file:/", "").replace(
                "xxxxxxxxxxx.csv", "")
        return dirname

