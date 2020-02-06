from pyetltools.config.config import Config
import enum

class ServerType(enum.Enum):
    NZ=1
    SQLSERVER=2
    DB2=3

class DBConfig(Config):

    def __init__(self, template=None, key=None, db_type:ServerType=None,
                 host=None,
                 port=None,
                 dsn=None,
                 username=None,
                 password=None,
                 data_source=None,
                 odbc_driver=None,
                 integrated_security=None,
                 supports_jdbc=None,
                 supports_odbc=None,
                 load_db_contexts=None,
                 spark_context="SPARK"):
        self.db_type = None
        self.host = None
        self.port = None
        self.dsn = None
        self.username = None
        self.password = None
        self.data_source = None
        self.odbc_driver = None
        self.integrated_security = None
        self.supports_jdbc=None
        self.supports_odbc=None
        self.load_db_contexts=None
        self.spark_context=None

        super().__init__(key=key, template=template)

        if db_type is not None:
            self.db_type = db_type
        if host is not None:
            self.host = host
        if port is not None:
            self.port = port
        if dsn is not None:
            self.dsn = dsn
        if username is not None:
            self.username = username
        if password is not None:
            self.password = password
        if data_source is not None:
            self.data_source = data_source
        if odbc_driver is not None:
            self.odbc_driver = odbc_driver
        if integrated_security is not None:
            self.integrated_security = integrated_security
        if supports_jdbc is not None:
            self.supports_jdbc=supports_jdbc
        if supports_odbc is not None:
            self.supports_odbc=supports_odbc
        if load_db_contexts is not None:
            self.load_db_contexts = load_db_contexts
        if spark_context is not None:
            self.spark_context = spark_context




