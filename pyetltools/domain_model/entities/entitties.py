class Table:
    def __init__(self, name, schema):
        self.name=name
        self.schema=schema

class TableInstance:
    def __init__(self, table:Table, database: Database):
        self.table = table
        self.database = database

class Database:
    def __init__(self, name):
        self.name = name


