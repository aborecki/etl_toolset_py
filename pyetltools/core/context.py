from abc import abstractmethod
import logging

import pyetltools
import pyetltools.config.config as cm

_context_factory_map = {}


class Context:

    def __init__(self, config):
        self.config = config
        self.config_key = config.key

    def get_password(self):
        if self.config.password is None:
            return cm.get_password(self.config.key)

    @classmethod
    @abstractmethod
    def create_from_config(cls, config: cm.Config):
        pass


def set_attributes_from_config():
    for conf_key in cm.get_keys():
        try:
            c = get(conf_key)
            attr_name = str(conf_key.replace("/", "_"));
            #print("Setting" +attr_name)
            setattr(pyetltools.context, attr_name, c)
        except Exception as e:
            print("Unable to get "+conf_key);
            print(e);
            #if setting of the attribute is not possible, silently ignore (should we?)
            pass;

def get_config(config_key):
    return cm.get_config(config_key)

def get(config_key):
    config = cm.get_config(config_key)
    if type(config) not in _context_factory_map:
        raise Exception("Context factory for " + str(type(config)) + " not configured.")
    factory = _context_factory_map[type(config)];
    return factory.create_from_config(config)


def register_context_factory(config_type, context_factory_type):
    logging.info("Adding context factory " + str(context_factory_type) + " for " + str(config_type) + ".")
    _context_factory_map[config_type] = context_factory_type
