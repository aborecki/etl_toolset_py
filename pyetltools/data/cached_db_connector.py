from pyetltools.core.env_manager import get_env_manager
from pyetltools.data.db_connector import DBConnector


class CachedDBConnector(DBConnector):
    def validate_config(self):
        super().validate_config()

    def query_cached(self, query, cache_key=None, force_reload_from_source=False, days_in_cache=None):
        return self.query_pandas_cached(query, cache_key=cache_key, force_reload_from_source=force_reload_from_source, days_in_cache=days_in_cache)

    def query_pandas_cached(self, query, cache_key=None,force_reload_from_source=False, days_in_cache=None):
        import hashlib
        if cache_key is None:
            cache_key="TEMP_QUERY_CACHE_" + self.key + "_" + self.data_source +"_" +query[0:50] +"_" + str(hashlib.md5(query.encode('utf-8')).hexdigest())
        return get_env_manager().get_default_cache().get_from_cache(cache_key,
                                                                     lambda : self.query_pandas(query),
                                                                            force_reload_from_source=force_reload_from_source,
                                                                            days_in_cache=days_in_cache)
