from abc import abstractmethod
import logging

import pyetltools
import pyetltools.config.config as cm

_context_factory_map = {}


class Container:
    pass;

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
            keys=conf_key.split("/");
            if len(keys)>1:
                group_name=keys[0]
                key=keys[1]
                # add group name if not exists
                if not hasattr(pyetltools.context, group_name):
                    setattr(pyetltools.context, group_name, Container())
                setattr(getattr(pyetltools.context, group_name), key, c)
            else:
                key = keys[0]
                setattr(pyetltools.context, key, c)
        except Exception as e:
            print("Unable to get " + conf_key);
            print(e);
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
