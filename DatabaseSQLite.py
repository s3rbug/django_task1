import sqlite3

from PyQt5.QtWidgets import QTableWidget, QTableWidgetItem
from PyQt5.QtCore import Qt

from DatabasePostgreSQL import DatabasePostgreSQL
from Logger import Logger
from utils import is_number


class DatabaseSQLite:
    def __init__(self, filename: str, table_widget: QTableWidget, logger: Logger):
        self.tableWidget = table_widget
        self.table_name = "internet_store_licenses"
        self.editable = True
        self.export_indexes = self.column_names = self.fields = []
        self.table_columns = self.table_rows = self.id_index = 0
        self.logger = logger
        self.db: None | sqlite3.Connection = None
        self.cursor = None
        self.connect(filename)

    def connect(self, filename: str):
        try:
            self.db = sqlite3.connect(filename)
            self.cursor = self.db.cursor()
            self.logger.log(f"Successful connection to SQLite database filename {filename}")
        except sqlite3.DatabaseError as error:
            self.logger.error_message_box(f"Error connecting to SQLite database! {error}")

    def init_table_widget_axis(self):
        try:
            self.cursor.execute(f"SELECT * FROM {self.table_name} LIMIT 0")
            columns = self.cursor.description
            self.column_names = [i[0] for i in columns]
            self.table_columns = len(self.column_names)
            self.tableWidget.setColumnCount(self.table_columns)
            self.tableWidget.setHorizontalHeaderLabels(self.column_names)
            self.cursor.execute(f"SELECT * FROM {self.table_name}")
            self.fields = self.cursor.fetchall()
            self.table_rows = len(self.fields)
            self.tableWidget.setRowCount(self.table_rows)
            self.id_index = self.column_names.index("id")
            self.tableWidget.setVerticalHeaderLabels("" for _ in self.fields)
        except sqlite3.DatabaseError as error:
            self.logger.error_message_box(f"SQLite error connecting table! {error}")

    def init_table_widget_fields(self):
        for i, field in enumerate(self.fields):
            for j, value in enumerate(field):
                table_widget_item = QTableWidgetItem(str(value))
                table_widget_item.setFlags(table_widget_item.flags() & ~Qt.ItemIsEditable)
                table_widget_item.setTextAlignment(Qt.AlignHCenter | Qt.AlignVCenter)
                self.tableWidget.setItem(i, j, table_widget_item)

    def update_table_widget(self):
        try:
            self.editable = False
            self.init_table_widget_axis()
            self.init_table_widget_fields()
            self.editable = True
        except sqlite3.DatabaseError as error:
            self.logger.error_message_box(f"PostgreSQL error trying to update table! {error}")

    @staticmethod
    def query_field_names(columns: [str]):
        sql_query = ""
        for i, column_name in enumerate(columns):
            comma = ", " if not i == 0 else ""
            sql_query = sql_query + comma + column_name
        return sql_query

    def query_field_values(self, field_values):
        sql_query = ""
        for i, export_index in enumerate(self.export_indexes):
            comma = ", " if not i == 0 else ""
            sql_query = sql_query + comma + self.str_to_sqlite_typo(field_values[export_index])
        return sql_query

    @staticmethod
    def str_to_sqlite_typo(var):
        if var == "" or var is None or str(var).lower() == "null":
            return "NULL"
        if is_number(var):
            return str(var)
        return f'"{var}"'

    def migrate_from_postgresql(self, db_postgresql: DatabasePostgreSQL, export_fields: [str]):
        def field_statement(value, statement):
            return f", {value}" if statement else ""

        try:
            #   Drop table before exporting
            self.cursor.execute(f"DROP TABLE IF EXISTS {self.table_name}")
            #   Create empty table
            sql_query_create_table = f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                        id INTEGER PRIMARY KEY AUTOINCREMENT
                        {field_statement("price INT NOT NULL",
                                         "price" in export_fields)}
                        {field_statement("count INT",
                                         "count" in export_fields)}
                        {field_statement("rating FLOAT",
                                         "rating" in export_fields)}
                        {field_statement("program_name TEXT NOT NULL",
                                         "program_name" in export_fields)}
                        {field_statement("program_description TEXT",
                                         "program_description" in export_fields)}
                        {field_statement("license_expire_year INT",
                                         "license_expire_year" in export_fields)}
                        {field_statement("is_unlimited_license BOOLEAN NOT NULL",
                                         "is_unlimited_license" in export_fields)}
                    )
            """
            self.cursor.execute(sql_query_create_table)
            self.export_indexes = [db_postgresql.column_names.index(export_field)
                                   for export_field in export_fields]
            for field in db_postgresql.fields:
                sql_query = f"""
                        INSERT INTO {self.table_name} 
                        ({self.query_field_names(export_fields)}) 
                        VALUES ({self.query_field_values(field)});
                    """
                self.cursor.execute(sql_query)
            self.db.commit()
            self.update_table_widget()
            self.logger.log(f"Successfully migrated fields {export_fields} to SQLite")
        except sqlite3.DatabaseError as error:
            self.logger.error_message_box(f"Error migrating to SQLite database! {error}")
