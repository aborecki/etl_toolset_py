from pyetltools.config.config import Config


class JiraConfig(Config):
    def __init__(self, key, url, username, password=None):
        super().__init__(key=key)
        self.url = url
        self.username = username
        self.password = password

