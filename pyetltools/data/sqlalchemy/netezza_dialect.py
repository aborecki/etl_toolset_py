'''SQLAlchemy dialect for Netezza'''
from sqlalchemy import sql, exc, Table, DATETIME
from sqlalchemy.dialects import registry
from sqlalchemy.engine import reflection
from sqlalchemy.connectors.pyodbc import PyODBCConnector
from sqlalchemy.dialects.postgresql.base import (
    PGDialect, PGTypeCompiler, PGCompiler, PGDDLCompiler, DOUBLE_PRECISION,
    INTERVAL, TIME, TIMESTAMP)
import sqlalchemy.types as sqltypes
from sqlalchemy.schema import DDLElement, SchemaItem
from sqlalchemy.sql import text, bindparam
import sqlalchemy.util as util
import pyodbc
import re
from sqlalchemy.ext.compiler import compiles
import sqlalchemy.types as types


# pylint:disable=R0901,W0212
from sqlalchemy.sql.ddl import CreateIndex


class ST_GEOMETRY(types.UserDefinedType, types.Binary):
    def get_col_spec(self, **kw):
        return "ST_GEOMETRY"


class BYTEINT(types.UserDefinedType):
    def get_col_spec(self, **kw):
        return "BYTEINT"



class NVARCHAR(sqltypes.NVARCHAR):
    '''Netezza NVARCHAR'''
    def __init__(self, length=None, collation=None,
                 convert_unicode='force',
                 unicode_error=None):
        super(NVARCHAR, self).__init__(
            length,
            collation=collation,
            convert_unicode=convert_unicode,
            unicode_error='ignore')


class OID(types.UserDefinedType, sqltypes.BigInteger):
    def get_col_spec(self, **kw):
        return "OID"


class NAME(NVARCHAR):
    def get_col_spec(self, **kw):
        return "NAME"


class ABSTIME(sqltypes.TIME):
    def get_col_spec(self, **kw):
        return "ABSTIME"


class TEXT(sqltypes.NVARCHAR):
    def get_col_spec(self, **kw):
        return "NVARCHAR"




# Weird types gleaned from _v_datatype
ischema_names = {
    'st_geometry': ST_GEOMETRY,
    'byteint': BYTEINT,
    'oid': OID,
    'name': NAME,
    'text': TEXT
}


class NetezzaTypeCompiler(PGTypeCompiler):
    '''Fills out unique netezza types'''

    def visit_ST_GEOMETRY(self, type_):
        return 'ST_GEOMETRY({})'.format(type_.length)

    def visit_BYTEINT(self, _type):
        return 'BYTEINT'

    def visit_OID(self, _type):
        return 'OID'

    def visit_NAME(self, _type):
        return 'NAME'

    def visit_ABSTIME(self, _type):
        return 'ABSTIME'

    def visit_TEXT(self, _type):
        return 'NVARCHAR(100)'

    def visit_TEXT(self, _type):
        return 'NVARCHAR(100)'

    def visit_TIMESTAMP(self, _type):
        return 'TIMESTAMP'

    def visit_FLOAT(self, _type):
        return 'DOUBLE'


class NetezzaCompiler(PGCompiler):
    '''Handles some quirks of netezza queries'''

    def limit_clause(self, select):
        '''Netezza doesn't allow sql params in the limit/offset piece'''
        text = ""
        if select._limit is not None:
            text += " \n LIMIT {limit}".format(limit=int(select._limit))
        if select._offset is not None:
            if select._limit is None:
                text += " \n LIMIT ALL"
            text += " OFFSET {offset}".format(offset=int(select._offset))
        return text


class DistributeOn(SchemaItem):
    '''Represents a distribute on clause'''

    def __init__(self, *column_names):
        '''Use like:
        my_table_1 = Table('my_table_1', metadata,
            Column('id_key', BIGINT),
            Column('nbr', BIGINT),
            DistributeOn('id_key')
        )
        my_table_2 = Table('my_table_2', metadata,
            Column('id_key', BIGINT),
            Column('nbr', BIGINT),
            DistributeOn('random')
        )
        '''
        self.column_names = column_names if column_names else ('RANDOM',)

    def _set_parent(self, parent):
        self.parent = parent
        parent.distribute_on = self


class NetezzaDDLCompiler(PGDDLCompiler):
    '''Adds Netezza specific DDL clauses'''

    def post_create_table(self, table):
        '''Adds the `distribute on` clause to create table expressions'''
        clause = ' DISTRIBUTE ON {columns}'
        if hasattr(table, 'distribute_on') and \
           table.distribute_on.column_names[0].lower() != 'random':
            column_list = ','.join(table.distribute_on.column_names)
            columns = '({})'.format(column_list)
        else:
            columns = 'RANDOM'
        return clause.format(columns=columns)

# Maps type ids to sqlalchemy types, plus whether they have variable precision
oid_datatype_map = {
    16: (sqltypes.Boolean, False),
    18: (sqltypes.CHAR, False),
    19: (NAME, False),
    20: (sqltypes.BigInteger, False),
    21: (sqltypes.SmallInteger, False),
    23: (sqltypes.Integer, False),
    25: (sqltypes.TEXT, False),
    26: (OID, False),
    700: (sqltypes.REAL, False),
    701: (DOUBLE_PRECISION, False),
    702: (ABSTIME, False),
    1042: (sqltypes.CHAR, True),
    1043: (sqltypes.String, True),
    1082: (sqltypes.Date, False),
    1083: (TIME, False),
    1184: (TIMESTAMP, False),
    1186: (INTERVAL, False),
    1266: (TIMESTAMP, False),
    1700: (sqltypes.Numeric, False),
    2500: (BYTEINT, False),
    2522: (sqltypes.NCHAR, True),
    2530: (sqltypes.NVARCHAR, True),
    2552: (ST_GEOMETRY, True),
    2568: (sqltypes.VARBINARY, True),
}


class NetezzaODBC(PyODBCConnector, PGDialect):
    '''Attempts to reuse as much as possible from the postgresql and pyodbc
    dialects.
    '''
    name = 'netezza'
    encoding = 'latin9'
    default_paramstyle = 'qmark'
    returns_unicode_strings = True
    supports_native_enum = False
    supports_sequences = True
    sequences_optional = False
    isolation_level = 'READ COMMITTED'
    max_identifier_length = 128
    type_compiler = NetezzaTypeCompiler
    statement_compiler = NetezzaCompiler
    ddl_compiler = NetezzaDDLCompiler
    description_encoding = None

    def initialize(self, connection):
        super(NetezzaODBC, self).initialize(connection)
        # PyODBC connector tries to set these to true...
        self.supports_unicode_statements = True
        self.supports_unicode_binds = True
        self.returns_unicode_strings = True
        self.convert_unicode = 'ignore'
        self.encoding = 'latin9'
        self.ischema_names.update(ischema_names)

    def has_table(self, connection, tablename, schema=None):
        '''Checks if the table exists in the current database'''
        # Have to filter by database name because the table could exist in
        # another database on the same machine
        dbname = connection.connection.getinfo(pyodbc.SQL_DATABASE_NAME)
        sql = ('select count(*) from _v_object_data where objname = ? '
               'and dbname = ?')
        result = connection.execute(sql, (str(tablename), dbname)).scalar()
        return bool(result)

    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        result = connection.execute(
            sql.text(
                    "select tablename as name from _v_table "
                    "where schema=:schema and tablename not like '_t_%'"
            ).columns(relname=sqltypes.Unicode),
            schema=schema if schema is not None else self.default_schema_name,
        )
        table_names = [r[0] for r in result]
        return table_names


    @reflection.cache
    def get_schema_names(self, connection, **kw):
        result = connection.execute(
            sql.text(
                "select schema from _v_schema"
                " ORDER BY schema"
            ).columns(nspname=sqltypes.Unicode)
        )
        return [name for name, in result]

    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, **kw):
        SQL_COLS = """
            SELECT --CAST(a.attname AS VARCHAR(128)) as name,
                   a.attname as name,
                   a.atttypid as typeid,
                   not a.attnotnull as nullable,
                   a.attcolleng as length,
                   a.format_type
            FROM _v_relation_column a
            WHERE a.name = :tablename and a.schema=:schema
            ORDER BY a.attnum
        """

        s = text(SQL_COLS,
                 bindparams=[bindparam('tablename', type_=sqltypes.String),
                             bindparam('schema', type_=sqltypes.String)],
                 typemap={'name': NAME,
                          'typeid': sqltypes.Integer,
                          'nullable': sqltypes.Boolean,
                          'length': sqltypes.Integer,
                          'format_type': sqltypes.String,
                          'tablename': sqltypes.String,
                          'schema': sqltypes.String
                          })
        c = connection.execute(s, tablename=table_name, schema=schema if schema is not None else self.default_schema_name)
        rows = c.fetchall()
        # format columns
        columns = []
        for name, typeid, nullable, length, format_type in rows:
            coltype_class, has_length = oid_datatype_map[typeid]
            if coltype_class is sqltypes.Numeric:
                precision, scale = re.match(
                    r'numeric\((\d+),(\d+)\)',format_type.lower()).groups()
                coltype = coltype_class(int(precision), int(scale))
            elif has_length:
                coltype = coltype_class(length)
            else:
                coltype = coltype_class()
            columns.append({
                'name': name,
                'type': coltype,
                'nullable': nullable,
            })
        return columns

    @reflection.cache
    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        '''Netezza doesn't have PK/unique constraints'''
        return {'constrained_columns': [], 'name': None}

    @reflection.cache
    def get_unique_constraints(
            self, connection, table_name, schema=None, **kw
    ):
        # TODO
        return []

    @reflection.cache
    def get_check_constraints(self, connection, table_name, schema=None, **kw):
        return [];

    @reflection.cache
    def get_table_comment(self, connection, table_name, schema=None, **kw):
        # TODO
        return {"text": ""}

    @reflection.cache
    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        '''Netezza doesn't have foreign keys'''
        return []

    @reflection.cache
    def get_indexes(self, connection, table_name, schema=None, **kw):
        '''Netezza doesn't have indexes'''
        return []

    @reflection.cache
    def get_view_names(self, connection, schema=None, **kw):
        if schema is not None:
            schema_where_clause = "schema = :schema"
        else:
            schema_where_clause = "1=1"
        result = connection.execute(
            "select viewname as name from _v_view"
            "where (%s) and viewname not like '_v_%'"
            % schema_where_clause)

        return [r[0] for r in result]

    def get_isolation_level(self, connection):
        return self.isolation_level

    def _get_default_schema_name(self, connection):
        return "DBO"

    def _check_unicode_returns(self, connection):
        '''Netezza doesn't *do* unicode (except in nchar & nvarchar)'''
        pass

    @reflection.cache
    def get_table_oid(self, connection, table_name, schema=None, **kw):
        """Fetch the oid for schema.table_name.

        Several reflection methods require the table oid.  The idea for using
        this method is that it can be fetched one time and cached for
        subsequent calls.

        """
        table_oid = None
        if schema is not None:
            schema_where_clause = "schema = :schema"
        else:
            schema_where_clause = "1=1"
        query = (
                """
                SELECT * FROM _V_TABLE
                WHERE (%s)
                AND tablename = :table_name 
            """
                % schema_where_clause
        )
        # Since we're binding to unicode, table_name and schema_name must be
        # unicode.
        table_name = util.text_type(table_name)
        if schema is not None:
            schema = util.text_type(schema)
        s = sql.text(query).bindparams(table_name=sqltypes.Unicode)
        s = s.columns(oid=sqltypes.Integer)
        if schema:
            s = s.bindparams(sql.bindparam("schema", type_=sqltypes.Unicode))
        c = connection.execute(s, table_name=table_name, schema=schema)
        table_oid = c.scalar()
        if table_oid is None:
            raise exc.NoSuchTableError(table_name)
        return table_oid



class CreateTableAs(DDLElement):
    """Create a CREATE TABLE AS SELECT ... statement."""

    def __init__(self,
                 new_table_name,
                 selectable,
                 temporary=False,
                 distribute_on='random'):
        '''Distribute_on may be a tuple of column names'''
        super(CreateTableAs, self).__init__()
        self.selectable = selectable
        self.temporary = temporary
        self.new_table_name = new_table_name
        self.distribute_on = distribute_on

    def distribute_clause(self):
        if self.distribute_on.lower() != 'random':
            column_list = ','.join(self.distribute_on)
            return '({})'.format(column_list)
        else:
            return 'RANDOM'



@compiles(CreateIndex)
def visit_create_index(create, compiler, **kw):
    # Nz does not support indexes - do nothing
    return "--"

@compiles(CreateTableAs)
def visit_create_table_as(element, compiler, **_kwargs):
    '''compiles a ctas statement'''
    return """
        CREATE {tmp} TABLE {name} AS (
        {select}
        ) DISTRIBUTE ON {distribute}
    """.format(
        tmp='TEMP' if element.temporary else '',
        name=element.new_table_name,
        select=compiler.sql_compiler.process(element.selectable),
        distribute=element.distribute_clause(),
    )


registry.register("netezza", "netezza_dialect", "NetezzaODBC")
registry.register(
    "netezza.pyodbc", "pyetltools.data.sqlalchemy.netezza_dialect", "NetezzaODBC")