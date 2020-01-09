from pyetltools.config.config import Config
import enum

class ServerType(enum.Enum):
    NZ=1
    SQLSERVER=2

class DBConfig(Config):

    def __init__(self, key, db_type:ServerType, host=None, port=None, dsn=None, username=None, password=None, data_source=None,
                 driver=None,
                 integrated_security=False):
        super().__init__(key=key)
        self.db_type = db_type
        self.host = host
        self.port = port
        self.dsn = dsn
        self.username = username
        self.password = password
        self.data_source = data_source
        self.driver = driver
        self.integrated_security = integrated_security




