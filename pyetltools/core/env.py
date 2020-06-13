import pyetltools

from pyetltools.core import connector



class EnvManager:
    def __init__(self ):
        self._connectors={}


    def add_connector(self, resource_type, environment=None, resource_sub_id=None, connector=None):
        assert connector is not None, "Connector cannot be None"
        assert resource_type is not None, "resource_type cannot be None"
        c=pyetltools.core.connector.get(connector)
        conn_key=(resource_type, environment, resource_sub_id)
        if conn_key  in self._connectors:
            raise Exception("Connector already added for "+str(conn_key))
        self._connectors[conn_key]=c
        return c

    def get_connector(self,  resource_type, environment=None , resource_sub_id=None):
        conn_key = (resource_type, environment, resource_sub_id)
        if conn_key not in self._connectors:
            raise Exception("Connector not found for "+str(conn_key))
        return self._connectors[conn_key]

    def validate_config(self):
        for conn in self._connectors.values():
            connector.get(conn)  # try to get connector
