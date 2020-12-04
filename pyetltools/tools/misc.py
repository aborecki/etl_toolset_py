import functools

from pyetltools import get_default_logger
from pyetltools.core.env_manager import get_default_cache
import math

def gen_insert(tablename, columns=None,df=None):

    def isnan(x):
        try:
            return math.isnan(x)
        except Exception as e:
            return False
    if columns is not None:
        values=["'"+i+"'" for i in  columns]
    elif df is not None and len(df) >0:
        values=["'"+str(i)+"'" if i and not isnan(i) else "NULL" for i in  df.iloc[0].values]
        columns=df.columns
    else:
        raise Exception("no columns nor df given.")
    return "insert into "+tablename+" ("+ ",\n".join([i for i in  columns])+") values \n ("+ ",\n".join(values)+")"

from datetime import datetime
def get_now_timestamp():
    return datetime.now().strftime('%Y%m%d_%H%M%S')


import io
from contextlib import redirect_stdout

def capture_stdout(func):
    f = io.StringIO()
    with redirect_stdout(f):
        res=func()
    stdout = f.getvalue()
    return (res,stdout)


import time


def profile(func):
    def wrap(*args, **kwargs):
        started_at = time.time()
        result = func(*args, **kwargs)
        print("Profile func:"+func.__name__+" Time (sec):"+str(time.time() - started_at))
        return result

    return wrap


def input_YN( prompt):
    ans = None
    while ans not in ['Y', 'N']:
        ans = input(prompt).upper()
    return ans == 'Y'


class CachedDecorator(object):
    def __init__(self, cache=None, cache_key=None, force_reload_from_source=None, days_in_cache=None):
        self.force_reload_from_source=force_reload_from_source
        self.days_in_cache = days_in_cache
        self.cache_key=cache_key
        self.cache=cache

    def __call__(self, fn):
        @functools.wraps(fn)
        def decorated(*args, **kwargs):
            if not self.cache:
                self.cache= get_default_cache()
            def retriever():
                return fn(*args, **kwargs)
            return self.cache.get_from_cache(self.cache_key if self.cache_key else (args, kwargs) , retriever=retriever,
                                                              force_reload_from_source=self.force_reload_from_source,
                                                              days_in_cache=self.days_in_cache)

        return decorated

class RetryDecorator(object):
    def __init__(self, auto_retry=3, manual_retry=True, fail_on_error=True):
        self.auto_retry=auto_retry
        self.manual_retry=manual_retry
        self.fail_on_error=fail_on_error
    def __call__(self, fn):
        @functools.wraps(fn)
        def decorated(*args, **kwargs):
            retry=True
            retries=0
            while retry:
                try:
                    result = fn(*args, **kwargs)
                    return result
                except Exception as ex:
                    get_default_logger().info("Exception caught {0}".format(ex))
                    retries+=1
                    if retries > self.auto_retry:
                        if self.manual_retry:
                            retry=input_YN("Do you want to retry?")
                        else:
                            retry=False
                    if not retry:
                        if self.fail_on_error:
                            raise ex
                        else:
                            get_default_logger().warn("fail_on_error is not set so continuing.")
                    else:
                        get_default_logger().info("Retrying...")
        return decorated


def batch(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]

def str_to_list(p):
    if isinstance(p, str):
        p=[p]
    return p
