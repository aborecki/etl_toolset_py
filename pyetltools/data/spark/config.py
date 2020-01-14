from pyetltools.config.config import Config


class SparkConfig(Config):
    def __init__(self, key, options):
        super().__init__(key=key)
        self.options = options

