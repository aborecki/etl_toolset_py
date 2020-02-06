from __future__ import annotations

import importlib
import logging
from typing import Dict, Any
import traceback
import getpass

from typing import Dict



class Config:
    def __init__(self, key: str, template: Config = None):
        if template:
            # copy all field values from template config
            self.__dict__ = template.__dict__.copy()

        self.key = key


class ConfigValue:
    def __init__(self, key: str, value):
        self.key = key
        self.value = value


_config_entries: Dict[str, Config] = {}
_passwords: Dict[str, str] = {}


def add(config: Config):
    key = config.key
    if key not in _config_entries:
        _config_entries[key] = config


def get_config(key: str) -> Config:
    if key not in _config_entries:
        raise Exception("Configuration with key " + str(key) + " not found.")
    return _config_entries.get(key)


def get_keys():
    return _config_entries.keys()


def filter_dict(dict, by_key=lambda x: True, by_value=lambda x: True):
    for k, v in dict.items():
        if by_key(k) and by_value(v):
            yield (k, v)


def get_password(key: str):
    if key not in _passwords:
        _passwords[key] = getpass.getpass(f"Enter password to {key}: ")
    return _passwords[key]


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
    _passwords = dict(pyetltools_passwords.passwords)
else:
    print("pyetltools_passwords module not found or does not contain passwords dictionary")
