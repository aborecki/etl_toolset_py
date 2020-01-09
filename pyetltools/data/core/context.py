from pyetltools.core import context
from pyetltools.data.config import DBConfig, ServerType
from pyetltools.data.core.db_connection import DBConnection
from pyetltools.data.core.nz_db_connection import NZDBConnection
from pyetltools.data.core.sql_server_db_connection import SQLServerDBConnection


class DBContext:
    def __init__(self, config: DBConfig, connection: DBConnection):
        self.config = config
        self.connection = connection

    @classmethod
    def create_context_from_config(cls, config: DBConfig):

        if config.db_type == ServerType.NZ:
            return DBContext(config, NZDBConnection(config))
        if config.db_type == ServerType.SQLSERVER:
            return DBContext(config,  SQLServerDBConnection(config))

    @classmethod
    def register_context_factory(cls):
        context.register_context_factory(DBConfig, DBContext)