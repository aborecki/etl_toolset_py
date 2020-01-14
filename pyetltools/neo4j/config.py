from pyetltools.config.config import Config


class NEO4JConfig(Config):
    def __init__(self, key, host, port, username, password=None):
        super().__init__(key=key)
        self.host = host
        self.port = port
        self.username = username
        self.password= password
