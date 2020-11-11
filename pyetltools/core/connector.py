import getpass
import importlib
import re
import logging
from abc import abstractmethod, ABCMeta

from pyetltools.core.attr_dict import AttrDict
from pyetltools import logger

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

    def set_env_manager(self, env_manager):
        assert env_manager, "env_manager cannot be None"
        self.env_manager=env_manager

    @abstractmethod
    def validate_config(self):
        pass

    def get_password(self):
        if self.password is None:
            return self.env_manager.get_password(self.key)
        else:
            return self.password

    def __repr__(self):
        return f"{self.__class__.__name__} - {self.key}"
