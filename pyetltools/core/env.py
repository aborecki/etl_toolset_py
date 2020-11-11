import re
from enum import Enum
import getpass

from pyetltools import logger
from pyetltools.core import connector
from pyetltools.core.attr_dict import AttrDict
from pyetltools.core.connector import Connector


default_env_manager=None

def get_default_env_manager():
    global default_env_manager
    if not default_env_manager:
        raise Exception("Default environment manager is not set")
    return default_env_manager

def set_default_env_manager(env_manager):
    global default_env_manager
    default_env_manager=env_manager

class EnvManager:
    def __init__(self ):
        self.passwords = dict()
        self.connectors = AttrDict()
        self._resources= dict()
        self._connectors = dict()
        self._connector_to_resource_key=dict()

    def add_connector(self, environment=None, resource_type=None, resource_subtype=None, resource_sub_id=None, conn=None, add_as_attribute=True):
        assert conn is not None, "Connector cannot be None"
        assert resource_type is not None, "resource_type cannot be None"
        #c=self.get_connector(connector)
        res_key=(resource_type, environment, resource_subtype, resource_sub_id)
        if res_key in self._resources:
            raise Exception("Resource already added for "+str(res_key))
        self._resources[res_key]=conn
        self._connector_to_resource_key[conn]=res_key
        conn.set_env_manager(self)
        return self.add_connector_with_key(conn, connector_key=tuple([x.name if isinstance(x, Enum) else str(x) for x in res_key if x]), add_as_attribute=add_as_attribute)

    def add_connector_with_key(self, conn, connector_key=None, add_as_attribute=True):
        assert connector_key is not None or conn.key is not None, "Connector.key or connector_key parameter has to be set."
        if not connector_key:
            connector_key = conn.key
        if isinstance(connector_key, str):
            keys = connector_key.split("/")
            connector_key_str=connector_key
        else:
            keys= connector_key
            connector_key_str="/".join(connector_key)

        if isinstance(conn, Connector):
            if not conn.key:
                conn.key=connector_key_str

            conn.set_env_manager(self)

        if add_as_attribute:
            curr_group = self.connectors
            for group_name in keys[0:-1]:
                # add group name if not exists
                if not group_name in curr_group._data:
                    new_group = AttrDict()
                    curr_group._add_attr(group_name, new_group)
                    curr_group = new_group
                else:
                    curr_group = curr_group._data[group_name]
            if isinstance(curr_group, AttrDict):
                 curr_group._add_attr(keys[-1], conn)
            elif isinstance(curr_group, Connector):
                logger.warn(f"Cannot add {connector_key} as attribute as another connector on this path is already added.")
            else:
                class WithAttrDict(conn.__class__, AttrDict):
                    pass
                conn.__class__ = WithAttrDict
            self._connectors[connector_key_str] = conn

        return conn

    def get_connector(self,  environment=None , resource_type=None, resource_subtype=None, resource_sub_id=None):
        con_key = (resource_type, environment, resource_subtype, resource_sub_id)
        if con_key not in self._resources:
            raise Exception("Connector not found for "+str(con_key))
        return self._resources[con_key]

    def get_connectors(self):
        return self.connectors

    def set_password(self, key, password):
        self.passwords[key] = password

    def set_passwords(self, _passwords):
        for key in _passwords:
            self.passwords[key] = _passwords[key]

    def get_password(self, key: str):
        if key not in self.passwords:
            if key is not None:
                if isinstance(key, tuple):
                    key="/".join(([x.name if isinstance(x, Enum) else str(x) for x in key if x]))

                matched_passwords = [p for p in self.passwords if re.match(p, key)]
            else:
                matched_passwords = []
            if len(matched_passwords) == 0:
                self.passwords[key] = getpass.getpass(f"Enter password" + (f" to {key}" if key is not None else "") + ":")
            else:
                return self.passwords[matched_passwords[0]]
        return self.passwords[key]


    def get_connector_by_key_or_none(self, conn):
        try:
            return self.get_connector_by_key(conn)
        except Exception as ex:
            return None

    def get_connector_by_key(self, conn):
        if isinstance(conn, Connector):
            return conn
        if conn not in self._connectors:
            raise Exception(f"Connector {conn} not found. Available connectors: " + str(list(self._connectors.keys())))
        return self._connectors[conn]

    #def validate_config():
    #    for conn_key in self._connectors:
    #        self._connectors.get(conn_key).validate_config()

    def validate_config(self):
        for conn in self._connectors:
            self.get_connector_by_key(conn)  # try to get connector
