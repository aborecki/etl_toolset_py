import pyetltools.config.config as cm

_context_factory_map = {}


class Connection:

    def __init__(self, config: cm.Config):
        self.config=config

    def get_password(self):
        if self.config.password is None:
            return cm.get_password(self.config.key)
