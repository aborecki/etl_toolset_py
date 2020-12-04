import os

import tempfile

import pandas

from pyetltools.core.connector import Connector
import logging
from pathlib import Path
import os.path as path
import time
import pickle



from pyetltools import get_default_logger
from pyetltools.tools.misc import get_now_timestamp

logger= get_default_logger()

class CacheConnector(Connector):
    def __init__(self, key, folder=None, force_reload_from_source=False, default_days_in_cache=None):
        super().__init__(key),
        self.folder = folder
        if folder is None:
            self.folder = os.path.join(tempfile.gettempdir(),"PYETLTOOLS_CACHE")
        Path(self.folder).mkdir(parents=True, exist_ok=True)
        self._cache=dict()
        self._cache_insert_time = dict()
        self.force_reload_from_source=force_reload_from_source
        self.default_days_in_cache=default_days_in_cache

    def validate_config(self):
        super().validate_config()

    def add_to_cache(self, key, obj):
        cache_key = self.get_cache_key(key)
        self._add_to_cache(key, obj)

    def _add_to_cache(self, cache_key, obj):
        file=os.path.join(self.folder, cache_key)
        Path(self.folder).mkdir(parents=True, exist_ok=True)
        if isinstance(obj, pandas.DataFrame):
            obj.to_parquet(file + ".parquet")
        else:
            with open(file + ".pickle.dump", 'wb') as f:
                pickle.dump(obj, f)
        self._add_to_mem_cache(cache_key, obj)

    def remove_cached_data(self):
        self._cache = dict()
        self._cache_insert_time = dict()
        import os
        os.rename(self.folder, self.folder+ get_now_timestamp())

    def get_from_cache_temp(self, retriever, force_reload_from_source=False, cache_keys=[]):
        import hashlib
        import inspect
        func_src=inspect.getsource(retriever)
        for ch in cache_keys:
            func_src+= str(ch)
        key= "TEMP_OBJECT_CACHE_"+retriever.__name__+"_"+str(hashlib.md5(func_src.encode('utf-8')).hexdigest())
        return self.get_from_cache(key, retriever, force_reload_from_source)


    def _add_to_mem_cache(self,cache_key, obj, lastModTime=None):
        self._cache[cache_key] = obj
        self._cache_insert_time[cache_key]=time.time() if not lastModTime else lastModTime

    def get_from_cache(self, key, retriever=None, force_reload_from_source=False, days_in_cache=None):
        cache_key = self.get_cache_key(key)

        reload_from_source=self.force_reload_from_source or force_reload_from_source

        if not days_in_cache:
            days_in_cache = self.default_days_in_cache

        if ( self.force_reload_from_source):
            logger.debug("Cache lookup disabled - force_reload_from_source flag is ON. ")
        if (force_reload_from_source):
            logger.debug("Cache lookup for this operation disabled - force_reload_from_source flag is ON")

        # returning memory cached value if reload_from_source is not forced
        # and value is cached and
        # days_in_cache is set and value stored in cache is not expired
        if not reload_from_source and cache_key in self._cache and (days_in_cache is None or (time.time() - self._cache_insert_time[cache_key]) < days_in_cache * 24 * 60 * 60):
            logger.debug(f"Cached data retured (cache_key:{cache_key}). Data is "+ str(round((time.time() - self._cache_insert_time[cache_key])/60/60,2)) +" hours old.")
            return self._cache[cache_key]
        else:
            file_base = os.path.join(self.folder, cache_key)
            file_parquet = file_base + ".parquet"
            file_pickle_dump = file_base + ".pickle.dump"
            try:
                if not reload_from_source:
                    obj=None
                    if path.exists(file_parquet):
                        logger.debug(f"Reading cache file: {file_parquet}")
                        obj = pandas.read_parquet(file_parquet)
                        file=file_parquet
                    elif path.exists(file_pickle_dump):
                        logger.debug(f"Reading cache file: {file_pickle_dump}")
                        obj = pandas.read_pickle(file_pickle_dump)
                        file = file_pickle_dump
                    else:
                        logger.debug(f"No cache file found: {file_base}.*")
                    if obj is not None:
                        if not self.is_file_older_than_x_days(file, days_in_cache):
                            self._add_to_mem_cache(cache_key, obj, self.get_file_last_mod_time(file))
                            logger.debug(f"File cached data retured (cache_key:{cache_key}). Data is " + str(
                                round((time.time() - self._cache_insert_time[cache_key]) / 60 / 60, 2)) + " hours old.")
                            return obj
                        else:
                            logger.debug(f"Cache older than {days_in_cache} days. Refreshing.")
            except Exception as e:
                logger.error(e)
                logger.debug("Cannot read file from cache.")
            if retriever:
                logger.debug("Loading from source")
                obj=retriever()
                if obj is None:
                    logger.debug("None object returned from retriever function for key "+cache_key)
                    return None
                self._add_to_cache(cache_key,obj)
                logger.debug("Done.")
                return obj
            else:
                logger.debug("Object not retreived from cache and no retriever function given.")
                return None

    def get_cache_key(self, s):
        return "PYETLTOOLS_CACHE_"+self._escape_str(s)

    def set_force_reload(self, flag):
        self.force_reload_from_source=flag


    def _escape_str(self,s):
        return "".join(x for x in str(s).replace("/", "_") if x.isalnum() or x == "_")


    def is_file_older_than_x_days(self, file, days=1):
        if not path.exists(file):
            return False
        file_time = path.getmtime(file)
        if (time.time() - file_time) / 3600 > 24*days:
            return True
        else:
            return False


    def get_file_last_mod_time(self, file):
        return  path.getmtime(file)


