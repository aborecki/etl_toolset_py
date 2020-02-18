import copy
import getpass
import importlib
import re
import traceback
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
            return None


def set_password(key, password):
    passwords[key] = password

def set_passwords(_passwords):
    for key in _passwords:
        passwords[key] = _passwords[key]

def get_password(key: str):
    if key not in passwords:
        matched_passwords = [p for p in passwords if re.match(p, key)]
        if len(matched_passwords) == 0:
            passwords[key] = getpass.getpass(f"Enter password to {key}: ")
        else:
            return passwords[matched_passwords[0]]
    return passwords[key]


passwords = dict()
connectors = AttrDict()
_connectors=dict()

def add(connector: Connector):
    connector_key = connector.key
    keys = connector_key.split("/");
    if len(keys) > 1:
        group_name = keys[0]
        key = keys[1]
        # add group name if not exists
        if not group_name in connectors._data:
            connectors._add_attr(group_name, AttrDict())
        connectors._data[group_name]._add_attr(key, connector)
    else:
        key = keys[0]
        connectors._add_attr(key, connector)
    _connectors[connector_key]=connector
    print("Connector added: " + str(type(connector).__name__) + " " + connector_key)
    return connector


def get(connector):
    if isinstance(connector, Connector):
        return connector
    if connector not in _connectors:
        raise Exception(f"Connector {connector} not found.")
    return _connectors[connector]


def load_config():
    logging.info(__name__ + ":__init__.py")
    # trying to import pyetltools_config.py config-in-file

    pyetltools_config_lib = importlib.util.find_spec("pyetltools_config")
    found_config = pyetltools_config_lib is not None
    if found_config:
        import pyetltools_config
    else:
        print("Cannot import pyetltools_config.")

    pyetltools_passwords_lib = importlib.util.find_spec("pyetltools_passwords")
    found_passwords = pyetltools_config_lib is not None
    if found_passwords:
        import pyetltools_passwords
    else:
        print("pyetltools_passworimds module not found or does not contain passwords dictionary")

    for conn_key in _connectors:
        print("Validate "+conn_key)
        _connectors.get(conn_key).validate_config()
