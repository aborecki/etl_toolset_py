import getpass
import importlib
import re
import logging
from abc import abstractmethod, ABCMeta

from pyetltools.core.attr_dict import AttrDict
import pyetltools.core.connector as connector


#  Module responsible for:
#  Creating connectors from config

class ConfigValue:
    def __init__(self, key: str, value):
        self.key = key
        self.value = value

class Connector(metaclass=ABCMeta):
    def __init__(self, key, password=None):
        self.key = key
        self.password = password

    @abstractmethod
    def validate_config(self):
        pass

    def get_password(self):
        if self.password is None:
            return connector.get_password(self.key)
        else:
            return self.password

    def __repr__(self):
        return f"{self.__class__.__name__} - {self.key}"

def set_password(key, password):
    passwords[key] = password

def set_passwords(_passwords):
    for key in _passwords:
        passwords[key] = _passwords[key]

def get_password(key: str):
    if key not in passwords:
        if key is not None:
            matched_passwords = [p for p in passwords if re.match(p, key)]
        else:
            matched_passwords=[]
        if len(matched_passwords) == 0:
            passwords[key] = getpass.getpass(f"Enter password"+ (f" to {key}"  if key is not None else "") +":")
        else:
            return passwords[matched_passwords[0]]
    return passwords[key]


passwords = dict()
connectors = AttrDict()
_connectors=dict()



def add(conn: Connector):
    connector_key = conn.key
    keys = connector_key.split("/");
    curr_group=connectors
    for group_name in keys[0:-1]:
        # add group name if not exists
        if not group_name in curr_group._data:
            new_group= AttrDict()
            curr_group._add_attr(group_name, new_group)
            curr_group=new_group
        else:
            curr_group=curr_group._data[group_name]
    curr_group._add_attr(keys[-1], conn)
    _connectors[connector_key]=conn
    #print("Connector added: " + str(type(connector).__name__) + " " + connector_key)
    return conn


def get(conn):
    if isinstance(conn, Connector):
        return conn
    if conn not in _connectors:
        raise Exception(f"Connector {conn} not found.")
    return _connectors[conn]


def validate_config():
    for conn_key in _connectors:
        _connectors.get(conn_key).validate_config()


