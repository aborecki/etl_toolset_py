import pyetltools

from pyetltools.core import connector

from enum import Enum

from pyetltools.core.env import EnvManager


class ResourceType(Enum):
    DB="DB"
    HGPROD="HGPROD"
    HG="HG"
    JENKINS= "JENKINS"
    JIRA="JIRA"
    INFA="INFA"

class Env(Enum):
    DEV="DEV"
    TEST="TEST"
    UTST="UTST"
    FTST2="FTST2"
    FOPROD="FOPROD"
    PROD="PROD"

class BECEnvManager(EnvManager):
    def __init__(self, key ):
        super().__init__(key)

    def get_environment(self, environment):
        if not isinstance(environment, Env) and environment is not None:
            if environment not in Env.__members__:
                raise Exception(environment+ " is not known environment. Allowed values: " + ", ".join([e.value for e in Env]))
            environment=Env[environment]
        return environment

    def get_resource_type(self, resource_type):
        if not isinstance(resource_type, ResourceType) and resource_type is not None:
            if resource_type not in ResourceType.__members__:
                raise Exception(resource_type + " is not known resource type. Allowed values: " + ", ".join([e.value for e in ResourceType]))
            resource_type=ResourceType[resource_type]
        return resource_type


    def get_db_connector(self, env, resource_sub_id):
        return self.get_connector(ResourceType.DB, env, resource_sub_id)

    def get_pc_rep_db_connector(self, env):
        return self.get_connector(ResourceType.DB, env, "SQL/PC_REP")

    def get_default_nz_meta_db_connector(self):
        return self.get_connector(ResourceType.DB, Env.FTST2, "NZ/META")

    def get_nz_meta_db_connector(self, env):
        return self.get_connector(ResourceType.DB,env, "NZ/META")


    def get_mainframe_meta_db_connector(self):
        return self.get_connector(ResourceType.DB, Env.PROD, "DB2/CD99")

    def add_connector(self, resource_type, environment=None, resource_sub_id=None, connector=None):

        assert connector is not None, "Connector cannot be None"
        assert resource_type is not None, "resource_type cannot be None"
        resource_type=self.get_resource_type(resource_type)
        environment=self.get_environment(environment)
        c=pyetltools.core.connector.get(connector)
        conn_key=(resource_type, environment, resource_sub_id)
        if conn_key  in self._connectors:
            raise Exception("Connector already added for "+str(conn_key))
        self._connectors[conn_key]=c
        return c

    def get_connector(self,  resource_type, environment=None , resource_sub_id=None):
        resource_type=self.get_resource_type(resource_type)
        environment=self.get_environment(environment)

        conn_key = (resource_type, environment, resource_sub_id)
        if conn_key not in self._connectors:
            raise Exception("Connector not found for "+str(conn_key))
        return self._connectors[conn_key]

    def validate_config(self):
        for conn in self._connectors.values():
            connector.get(conn)  # try to get connector
