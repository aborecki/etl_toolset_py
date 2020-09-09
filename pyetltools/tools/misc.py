from pyetltools import logger

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


def debug(function):
    def wrapper(*args,**kwargs):
        result = function(*args, **kwargs)
        logger.debug(f'''
            ########################################################
            ##############{function.__name__}
            ########################################################
            ''')
        for i, arg in enumerate(args, start=1):
            logger.debug(f'param {i} = {arg}')
        logger.debug(f''' function result:
                {result}''')
        return result
    return wrapper

