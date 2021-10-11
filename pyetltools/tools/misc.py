import functools

from pyetltools import get_default_logger
import math

logger = get_default_logger()

def gen_insert(tablename, columns=None,df=None, rows=1):

    def isnan(x):
        try:
            return math.isnan(x)
        except Exception as e:
            return False
    if columns is not None:
        values=[["'"+i+"'" for i in  columns]]
    elif df is not None and len(df) >0:
        values=[]
        for i in range(0, min(len(df), rows)):
            values.append(["'"+str(i)+"'" if i and not isnan(i) else "NULL" for i in  df.iloc[i].values])
        columns=df.columns
    else:
        raise Exception("no columns nor df given.")

    return "\n".join([ "insert into "+tablename+" ("+ ",\n".join([i for i in  columns])+") values \n ("+ ",\n".join(val)+");" for val in values] )

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

def input_answer( prompt, answers, true_ans):
    ans = None
    while ans not in [a.upper() for a in answers]:
        ans = input(prompt).upper()
    return ans == true_ans.upper()


def get_text_hexdigest(text):
    import hashlib
    md5_hash = hashlib.md5()
    md5_hash.update(text)
    digest = md5_hash.hexdigest()
    return str(digest)


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
