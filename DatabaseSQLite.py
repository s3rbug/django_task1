from DatabasePostgreSQL import DatabasePostgreSQL


class DatabaseSQLite:
    def __init__(self):
        #   TODO
        self.connect()
        print("init")

    def connect(self):
        #   TODO
        print("connect")

    def migrate_from_postgresql(self, db_postgresql: DatabasePostgreSQL, export_fields: [str]):
        print(export_fields)
