from abc import ABC, abstractmethod

from py2neo import Graph

from pyetltools.core.connection import Connection
from neo4j import GraphDatabase


class NEO4JConnection(Connection):

    def get_url(self):
        return str(self.config.host)+":"+str(self.config.port)


    def __init__(self, config):
        super().__init__(config)
        self.driver = GraphDatabase.driver("bolt://"+self.get_url(),auth=(self.config.username,self.get_password()))

    def get_graph(self):
        return  Graph(self.get_url(),auth=(self.config.username,self.get_password()))

    def get_session(self):
        return self.driver.session()

