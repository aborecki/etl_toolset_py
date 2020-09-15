import os

import tempfile

import pandas

from pyetltools.core.connector import Connector
import logging
from pathlib import Path

logger = logging.getLogger("bectools")

class CacheConnector(Connector):
    def __init__(self, key, folder=None):
        super().__init__(key),
        self.folder = folder
        if folder is None:
            self.folder = os.path.join(tempfile.gettempdir(),"PYETLTOOLS_CACHE")
        Path(self.folder).mkdir(parents=True, exist_ok=True)
        self.cache=dict()
        self.force_reload_from_source=False

    def validate_config(self):
        super().validate_config()

    def add_to_cache(self, key, obj):
        cache_key = self.get_cache_key(key)
        file=os.path.join(self.folder, cache_key + ".parquet")

        obj.to_parquet(file)
        self.cache[cache_key] = obj

    def reset_cache(self):
        self.cache = dict()
        import os
        f = []
        files_to_del = [os.path.join(self.folder, x) for x in os.listdir(self.folder) if
                        x.startswith("PYETLTOOLS_CACHE_")]
        for f in files_to_del:
            logger.info("Removed " + str(f))
            os.remove(f)

    def get_from_cache_temp(self, retriever, force_reload_from_source=False):
        import hashlib
        import inspect
        func_src=inspect.getsource(retriever)
        key= "TEMP_OBJECT_CACHE_"+str(hashlib.md5(func_src.encode('utf-8')).hexdigest())
        return self.get_from_cache(key, retriever, force_reload_from_source)

    def get_from_cache(self, key, retriever=None, force_reload_from_source=False):
        cache_key = self.get_cache_key(key)

        reload_from_source=self.force_reload_from_source or force_reload_from_source

        if ( self.force_reload_from_source):
            logger.info("Cache lookup disabled - force_reload_from_source flag is ON. ")
        if (force_reload_from_source):
            logger.info("Cache lookup for this operation disabled - force_reload_from_source flag is ON")

        if not reload_from_source and cache_key in self.cache:
            return self.cache[cache_key]
        else:
            file = os.path.join(self.folder, cache_key + ".parquet")
            try:
                if not reload_from_source:
                    logger.info(f"Reading cache file: {file}")
                    obj = pandas.read_parquet(file)
                    self.cache[cache_key] = obj
                    return obj
            except Exception as e:
                logger.info("Cannot read file from cache.")
            if retriever:
                logger.info("Loading from source")
                obj=retriever()
                if obj is None:
                    logger.info("None object returned from retriever function for key "+cache_key)
                    return None
                self.cache[cache_key]=obj
                if isinstance(obj, pandas.DataFrame):
                    obj.to_parquet(file)
                else:
                    raise Exception("Cache does not supported data format: "+  str(type(obj)))

                # todo: support other types of obj
                return obj
            else:
                return None

    def get_cache_key(self, s):
        return "PYETLTOOLS_CACHE_"+self._escape_str(s)

    def set_force_reload(self, flag):
        self.force_reload_from_source=flag


    def _escape_str(self,s):
        return "".join(x for x in s.replace("/", "_") if x.isalnum() or x == "_")


