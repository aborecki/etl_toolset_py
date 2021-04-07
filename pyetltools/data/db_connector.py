import copy
from abc import abstractmethod

import pyodbc
from pandas.io.sql import DatabaseError

import pandas
import jaydebeapi

from pyetltools.core import env_manager
from pyetltools.core.attr_dict import AttrDict
from pyetltools.core.connector import Connector

from sys import stderr


from pyetltools.data.db_tools import db_metadata
from pyetltools.data.spark import  tools as spark_tools

from pyetltools import logger



class DBConnector(Connector):

    class tools:
        pass

    def __init__(self,
                 key=None,
                 jdbc_conn_string=None,
                 odbc_conn_string=None,
                 host=None,
                 port=None,
                 dsn=None,
                 username=None,
                 password=None,
                 data_source=None,
                 odbc_driver=None,
                 jdbc_driver=None,
                 integrated_security=None,
                 supports_jdbc=None,
                 jdbc_access_method="jaydebeapi",
                 supports_odbc=None,
                 load_db_connectors=None,
                 spark_connector="SPARK",
                 db_dialect_class=None,
                 odbc_conn_options=None
                 ):
        super().__init__(key=key)
        self.jdbc_conn_string = jdbc_conn_string
        self.odbc_conn_string = odbc_conn_string
        self.host = host
        self.port = port
        self.dsn = dsn
        self.username = username
        self.password = password
        self.data_source = data_source
        self.odbc_driver = odbc_driver
        self.jdbc_driver = jdbc_driver
        self.odbc_conn_options = odbc_conn_options

        self.integrated_security = integrated_security
        self.supports_jdbc = supports_jdbc
        self.jdbc_access_method=jdbc_access_method
        self.supports_odbc = supports_odbc
        self.load_db_connectors = load_db_connectors
        self.spark_connector = spark_connector
        self.db_dialect_class = db_dialect_class

        self.db_dialect=self.db_dialect_class()

        if not self.jdbc_driver:
            self.jdbc_driver = self.db_dialect.get_jdbc_driver()
        self.DS=AttrDict(initializer=lambda _: self.load_db_sub_connectors() )
        self._odbcconnection=None

        from pyetltools.data.sql_alchemy_connector import SqlAlchemyConnector
        self.sql_alchemy_connector= SqlAlchemyConnector(self)

        if self.load_db_connectors:
            self.load_db_sub_connectors()

        self.env = env_manager.get_env_manager()


    def validate_config(self):
        super().validate_config()
        # check if connector exists
        self.env.get_connector_by_key(self.spark_connector)

    def get_spark_connector(self):
        return self.env.get_connector_by_key(self.spark_connector)

    def with_data_source(self, data_source):
        cp=self.clone()
        cp.data_source=data_source
        return cp

    def clone(self):
        return type(self)(key=self.key,
                 jdbc_conn_string=self.jdbc_conn_string,
                 odbc_conn_string=self.odbc_conn_string,
                 host=self.host,
                 port=self.port,
                 dsn=self.dsn,
                 username=self.username,
                 password=self.password,
                 data_source=self.data_source,
                 odbc_driver=self.odbc_driver,
                 integrated_security=self.integrated_security,
                 supports_jdbc=self.supports_jdbc,
                 supports_odbc=self.supports_odbc,
                 load_db_connectors=self.load_db_connectors,
                 spark_connector=self.spark_connector,
                 db_dialect_class=self.db_dialect_class,
                 odbc_conn_options=self.odbc_conn_options)

    def get_password(self):
        if self.integrated_security:
            return None
        else:
            return super().get_password()

    def get_sqlalchemy_conn_string(self):
        return self.db_dialect.get_sqlalchemy_conn_string(self.get_odbc_conn_string(), self.get_jdbc_conn_string())


    def get_odbc_conn_string(self):
        if self.odbc_conn_string:
            return self.odbc_conn_string
        return self.db_dialect.get_odbc_conn_string(
            self.dsn,
            self.host,
            self.port,
            self.data_source,
            self.username,
            self.get_password, #passing as callback so function will use it only if needed
            self.odbc_driver,
            self.integrated_security,
            self.odbc_conn_options)

    def get_jdbc_conn_string(self):
        if self.jdbc_conn_string:
            return self.jdbc_conn_string
        return self.db_dialect.get_jdbc_conn_string(
            self.dsn,
            self.host,
            self.port,
            self.data_source,
            self.username,
            self.get_password,  #passing as callback so function will use it only if needed
            self.odbc_driver,
            self.integrated_security)

    def get_odbc_connection(self, reuse_odbc_connection=False, autocommit=False):
        if not self._odbcconnection or reuse_odbc_connection==False:
            self._odbcconnection= pyodbc.connect(self.get_odbc_conn_string(), autocommit=autocommit)

        return self._odbcconnection;

    def query_table(self, table_name, where=None):
        cond_sql=""
        if where:
            cond_sql=f" WHERE {where}"
        return self.query(f"SELECT * FROM {table_name} {cond_sql}" )

    def query(self, query, reuse_odbc_connection=False, args=[]):
        """
            Runs query_pandas. If query does not start with "SELECT" then query will be transformed to "SELECT * FROM {query}"
        """
        if not query.upper().lstrip().startswith("SELECT") and not query.upper().lstrip().startswith("WITH"):
            query=f"SELECT * FROM {query}"

        return self.query_pandas(query, reuse_odbc_connection, args=args)

    def query_spark(self, query, register_temp_table=None):
        if self.supports_jdbc:
            logger.debug(f"Executing query (JDBC) on connector {str(self)}:"+query)
            ret=self.get_spark_connector().get_df_from_jdbc(self.get_jdbc_conn_string(),
                                                      query,
                                                      self.db_dialect.get_jdbc_driver(),
                                                      self.username,
                                                      self.get_password)
        else:
            print("Warning: conversion from panadas df to spark needed. Can be slow.")
            print(f"Executing query (ODBC) on connector {str(self)}:" + query)
            pdf = self.query_pandas(query)
            is_empty = pdf.empty
            ret=None
            if not is_empty:
                ret = self.get_spark_connector().convert_pandas_df_to_spark(pdf)
            else:
                print("No data returned.")
        if register_temp_table is not None:
            print("Registering temp table as:"+register_temp_table)
            ret.registerTempTable(register_temp_table)
        return ret

    def query_pandas(self, query, reuse_odbc_connection=False, args=[]):
        """
            Reads database source to pandas dataframe.
            Depending on the configuration of the connector either odbc, jdbc via spark or jaydebeapi jdbc will be used

            :param reuse_odbc_connection: if set connection will be reused (currently works only for ODBC sources)
        """
        logger.debug(f"Executing query on connector {str(self)}:" + query +" with args:"+str(args))
        if self.supports_odbc:
             logger.debug(" using ODBC")
             conn = self.get_odbc_connection(reuse_odbc_connection)
             return pandas.read_sql(query, conn, coerce_float=False, parse_dates=None, params=args)
        else:
            if len(args)>0:
                raise Exception("Query args not supported for spark od jaydebeapi.")
            if self.jdbc_access_method=="spark":
                logger.debug("\nWarning: conversion from spark df to pandas needed.")
                return self.query_spark(query).toPandas()
            else:
                logger.debug("\nUsing jaydebeapi jdbc access method")
                return self.read_jdbc_to_pd_df(query, self.jdbc_driver, self.get_jdbc_conn_string(),[self.username, self.get_password()])


    def read_jdbc_to_pd_df(self, sql, jclassname, con, driver_args, jars=None, libs=None):
        '''
        Reads jdbc compliant data sources and returns a Pandas DataFrame

        uses jaydebeapi.connect and doc strings :-)
        https://pypi.python.org/pypi/JayDeBeApi/

        :param sql: select statement
        :param jclassname: Full qualified Java class name of the JDBC driver.
            e.g. org.postgresql.Driver or com.ibm.db2.jcc.DB2Driver
        :param driver_args: Argument or sequence of arguments to be passed to the
           Java DriverManager.getConnection method. Usually the
           database URL. See
           http://docs.oracle.com/javase/6/docs/api/java/sql/DriverManager.html
           for more details
        :param jars: Jar filename or sequence of filenames for the JDBC driver
        :param libs: Dll/so filenames or sequence of dlls/sos used as
           shared library by the JDBC driver
        :return: Pandas DataFrame
        '''

        try:
            conn = jaydebeapi.connect(jclassname, con, driver_args, jars, libs)
        except jaydebeapi.DatabaseError as de:
            raise

        try:
            curs = conn.cursor()
            logger.info("Executing:" +sql)
            curs.execute(sql)
            columns = [desc[0] for desc in curs.description]  # getting column headers
            # convert the list of tuples from fetchall() to a df
            data=curs.fetchall()
            logger.info("Fetching DONE.")
            return pandas.DataFrame(data, columns=columns)

        except jaydebeapi.DatabaseError as de:
            raise

        finally:
            curs.close()
            conn.close()

    def copy_data(self, destination_db_connector, query, destination_tb_name,  batch_size=10000,
                  truncate_destination_table=False, fast_executemany=True):

        if self.supports_odbc:
            import time
            src_conn = self.get_odbc_connection()
            dest_conn = destination_db_connector.get_odbc_connection()

            src_curs= src_conn.cursor()
            tgt_curs = dest_conn.cursor()

            tgt_curs.fast_executemany = fast_executemany

            src_curs.execute(f"select  count(*) cnt from ({query}) x")
            src_cnt=int(src_curs.fetchone()[0])
            src_curs.commit()

            if truncate_destination_table:
                destination_db_connector.execute_statement(f"TRUNCATE TABLE {destination_tb_name}; COMMIT;")

            start = time.time()
            tot=0
            logger.info(destination_tb_name+": Copying "+str(src_cnt)+" records.")

            src_curs = self.get_odbc_connection().cursor()
            src_curs.execute(query)
            columns = [column[0] for column in src_curs.description]

            COLUMNS = ",".join(columns)
            PARAMS = ",".join(["?" for c in columns])

            time_batch_start = time.time()
            src_data = src_curs.fetchmany(batch_size)
            print(len(src_data))

            while len(src_data) > 0:
                print(destination_tb_name+": Inserting "+str(len(src_data))+".", end="")

                tgt_curs.executemany(f'INSERT INTO {destination_tb_name} ({COLUMNS}) VALUES ({PARAMS})', src_data)
                tgt_curs.commit()
                time_batch_end = time.time()
                tot=tot+len(src_data)
                logger.info(" DONE "+ str(round(tot/src_cnt*100))+"%" )
                time_passed=round(time.time() - start)
                total_time=round(time_passed*(1.0/(tot/src_cnt)))
                time_left=round(total_time-time_passed)
                logger.info(destination_tb_name+": Time passed: "+str(time_passed)+
                      " sec. Total time est: "+str(total_time)+
                      " sec. Time left: "+str(time_left) +"sec ("+str(round(time_left/60/60,2))+" hours)"+
                      " Speed from beginning: "+ str(round(tot/time_passed,2))+" rec/sec" +
                      " Speed last batch:" + str(round(len(src_data)/(time_batch_end-time_batch_start),2)) +" rec/sec")

                time_batch_start = time.time()
                src_data = src_curs.fetchmany(batch_size)
                time_batch_start = time.time()
            logger.info(destination_tb_name+": Copying time: "+ str((time.time()) - start))
        else:
            raise Exception("ODBC support required")

    def execute_statement(self, statement, add_col_names=False, reuse_odbc_connection=False, ignore_error=False, args=[], commit=False):

        if self.supports_odbc:
            conn = self.get_odbc_connection(reuse_odbc_connection )
            cursor = conn.cursor()
            try:
                logger.debug(f"Executing statement:{statement}")
                cursor.execute(statement, *args)
            except DatabaseError as ex:
                logger.info(f"DB Exception caught:{ex}.\nReconnecting.")
                conn = self.get_odbc_connection(reuse_odbc_connection=False)
                cursor.execute(statement, *args)
            except Exception as ex:
                if ignore_error:
                    logger.debug(f"Ignored exception when running {statement}:" + str(ex))
                else:
                    raise ex


            res = []
            rowcount = cursor.rowcount
            try:
                recs = cursor.fetchall()
                if add_col_names:
                    fields = tuple(map(lambda x: x[0], cursor.description))
                    recs.insert(0, fields)
                res.append(recs)
            except pyodbc.ProgrammingError:
                pass
            while cursor.nextset():  # NB: This always skips the first resultset
                try:
                    recs = cursor.fetchall()
                    if add_col_names:
                        fields = tuple(map(lambda x: x[0], cursor.description))
                        recs.insert(0, fields)
                    res.append(recs)
                except pyodbc.ProgrammingError:
                    continue
            if commit:
                conn.execute("COMMIT;")
            return (rowcount,res);
        else:
             if self.jdbc_access_method=="spark":
                print("\nWarning: conversion from spark df to pandas needed.")
                return self.query_spark(statement).toPandas()
             else:
                print("\nUsing jaydebeapi jdbc access method")
                return self.read_jdbc_to_pd_df(statement, self.jdbc_driver, self.get_jdbc_conn_string(),[self.username, self.get_password()])


    def get_databases_by_name_part(self, name_part):
        return list(filter(lambda x: name_part.upper() in x.upper(), self.get_databases()))

    def get_databases(self):
        ret = self.query_pandas(self.db_dialect.get_sql_list_databases())
        ret.columns = ret.columns.str.upper()
        return list(ret["NAME"])

    def get_object_by_name_regex(self, regex):
        objects=self.query_pandas(self.db_dialect.get_sql_list_objects())
        objects.columns = map(str.lower, objects.columns)
        return objects[objects.name.str.match(regex)]

    def get_objects(self):
        return self.query_pandas(self.db_dialect.get_sql_list_objects())

    def get_columns_all_objects(self):
        return self.query_pandas(self.db_dialect.get_sql_list_columns_all_objects())

    # replaced by cached version from db_metadata.py
    #def get_columns_for_table(self, table_name):
    #    return self.query_pandas(self.db_dialect.get_sql_list_columns(table_name))

    def get_objects_for_databases(self, databases):
        ret = None
        for db in databases:
            try:
                print("Retrieving objects from: " + db)
                ds=self.with_data_source(db)
                ret_tmp = ds.query_pandas(self.db_dialect.get_sql_list_objects())
                ret_tmp["DATABASE_NAME"] = db
                if ret is None:
                    ret = ret_tmp
                else:
                    ret = ret.append(ret_tmp)
            except Exception as e:

                print(e, file=stderr)
        return ret;

    def get_object_for_databases_single_query(self, databases):
        return self.query_pandas(
            self.db_dialect.get_sql_list_objects_from_datbases_single_query(databases))

    def get_connector_for_each_database(self):
        ret={}
        for db in self.get_databases():

            new_conn = self.with_data_source(db)
            new_conn.load_db_connectors=False
            ret[db] = new_conn
        return ret

    def load_db_sub_connectors(self):
        """
        Adds database connectors for each database
        :return:
        """
        for db, con in self.get_connector_for_each_database().items():
            self.DS._add_attr(db, con)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # maybe we should close the connection here ????
        pass

    def __repr__(self):
        return f"{self.__class__.__name__} - {self.key} "+ ( " data source:"+self.data_source if self.data_source is not None else "" )

    @classmethod
    def df_to_excel(filename):
        spark_tools.df_to_excel(filename)

    @classmethod
    def df_to_csv(dir):
        spark_tools.df_to_csv(dir)

# Extending db_connector with methods
DBConnector.get_objects_cached=db_metadata.get_objects_cached
DBConnector.get_objects_for_multiple_dbs_cached=db_metadata.get_objects_for_multiple_dbs_cached
DBConnector.get_objects_by_name_cached=db_metadata.get_objects_by_name_cached
DBConnector.get_objects_by_name_regex_cached=db_metadata.get_objects_by_name_regex_cached

DBConnector.get_columns_for_multiple_dbs_cached=db_metadata.get_columns_for_multiple_dbs_cached
DBConnector.get_columns=db_metadata.get_columns
DBConnector.get_columns_by_object_name_cached=db_metadata.get_columns_by_object_name_cached
DBConnector.get_columns_by_object_name_regex_cached=db_metadata.get_columns_by_object_name_regex_cached

DBConnector.get_table_counts = db_metadata.get_table_counts
DBConnector.get_tables_metadata =db_metadata.get_tables_metadata
