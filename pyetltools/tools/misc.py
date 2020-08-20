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