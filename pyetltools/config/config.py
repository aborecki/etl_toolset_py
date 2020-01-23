import logging
from typing import Dict, Any
import traceback
import getpass



from typing import Dict


class Config:
    def __init__(self, key):
        self.key = key


class ConfigValue:
    def __init__(self, key, value):
        self.key = key
        self.value = value


_config_entries: Dict[str, Config] = {}
_passwords: Dict[str, str] = {}


def add(config):
    key = config.key
    if key not in _config_entries:
        _config_entries[key] = config
    return _config_entries.get(key)


def get_config(key):
    if key not in _config_entries:
        raise Exception("Configuration with key "+key+" not found.")
    return _config_entries.get(key)


def get_keys():
    return _config_entries.keys()


def filter_dict(dict, by_key=lambda x: True, by_value=lambda x: True):
    for k, v in dict.items():
        if by_key(k) and by_value(v):
            yield (k, v)


def get_password(key):
    if key not in _passwords:
        _passwords[key] =  getpass.getpass(f"Enter password to {key}: ")
    return _passwords[key]


logging.info(__name__ + ":__init__.py")
# trying to import pyetltools_config.py config-in-file
try:
    import pyetltools_config
except Exception as e:
    print("Cannot import pyetltools_config.")
    traceback.print_exc(e)

try:
    import pyetltools_passwords
    _passwords = dict(pyetltools_passwords.passwords)
except:
    print("pyetltools_passwords module not found or does not contain passwords dictionary")
    pass

