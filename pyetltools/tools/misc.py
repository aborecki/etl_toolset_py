import functools


def gen_insert(tablename, columns):
    return "insert into "+tablename+" ("+ ",\n".join(["'"+i+"'" for i in  columns])+") values ("+ ",\n".join(["'"+i+"'" for i in  columns])+")"

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


class RetryDecorator(object):
    def __init__(self, auto_retry=0):
        self.auto_retry=auto_retry
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
                    print("Exception caught {0}".format(ex))
                    retries+=1
                    if retries > self.auto_retry:
                        retry=input_YN("Do you want to retry?")
                    if not retry:
                        raise ex
                    else:
                        print("Retrying...")
        return decorated


def batch(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]
