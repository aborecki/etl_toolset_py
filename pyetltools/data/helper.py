def gen_insert(tablename, columns):
    return "insert into "+tablename+" ("+ ",\n".join(["'"+i+"'" for i in  columns])+") values ("+ ",\n".join(["'"+i+"'" for i in  columns])+")"