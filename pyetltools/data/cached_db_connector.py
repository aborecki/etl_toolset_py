from pyetltools.core.env_manager import get_env_manager
from pyetltools.data.db_connector import DBConnector


class BECDBConnector(DBConnector):
    def validate_config(self):
        super().validate_config()

    def query_cached(self, query, force_reload_from_source=False, days_in_cache=None):
        return self.query_pandas_cached(query, force_reload_from_source=force_reload_from_source, days_in_cache=days_in_cache)

    def query_pandas_cached(self, query, force_reload_from_source=False, days_in_cache=None):
        import hashlib
        return get_env_manager().get_default_cache().get_from_cache("TEMP_QUERY_CACHE_" + self.key +
                                                               "_" + self.data_source +"_" +query[0:50] +"_" +
                                                                     str(hashlib.md5(query.encode('utf-8')).hexdigest()),
                                                                     lambda : self.query_pandas(query),
                                                                            force_reload_from_source=force_reload_from_source,
                                                                            days_in_cache=days_in_cache)
