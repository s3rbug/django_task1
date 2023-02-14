import psycopg2
from PyQt5.QtWidgets import QTableWidget, QTableWidgetItem
from Logger import Logger
from DatabaseMySQL import DatabaseMySQL
from utils import sql_query_values, is_number
from PyQt5.QtCore import Qt


class DatabasePostgreSQL:
    def __init__(self, host, user, password, database, logger: Logger, table_widget: QTableWidget):
        self.tableWidget = table_widget
        self.table_name = "internet_store_licenses"
        self.column_names = self.fields = []
        self.id_index = self.table_columns = self.table_rows = 0
        self.logger = logger
        self.db: None | psycopg2.connection = None
        self.connect(host, user, password, database)
        self.cursor = self.db.cursor()

    def connect(self, host, user, password, database):
        try:
            self.db = psycopg2.connect(
                host=host,
                user=user,
                password=password,
                database=database,
            )
            self.logger.log(f"Successful connection to PostgreSQL database on host {host}")
        except psycopg2.DatabaseError as error:
            self.logger.error_message_box(f"Error trying to connect to PostgreSQL! {error}", should_abort=True)

    def init_table_widget_axis(self):
        try:
            self.cursor.execute(f"SELECT * FROM {self.table_name} LIMIT 0")
            columns = self.cursor.description
            print(columns)
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
        except psycopg2.DatabaseError as error:
            self.logger.error_message_box(f"MySQL error connecting table! {error}")

    def init_table_widget_fields(self):
        for i, field in enumerate(self.fields):
            for j, value in enumerate(field):
                table_widget_item = QTableWidgetItem(str(value))
                table_widget_item.setFlags(table_widget_item.flags() & ~Qt.ItemIsEditable)
                table_widget_item.setTextAlignment(Qt.AlignHCenter | Qt.AlignVCenter)
                self.tableWidget.setItem(i, j, table_widget_item)

    def update_table_widget(self):
        try:
            self.init_table_widget_axis()
            self.init_table_widget_fields()
        except psycopg2.DatabaseError as error:
            self.logger.error_message_box(f"MySQL error trying to update table! {error}")

    @staticmethod
    def str_to_postgresql_typo(var, field_type):
        if var == "" or var is None or str(var).lower() == "null":
            return "NULL"
        if field_type == b'tinyint(1)':
            return "TRUE" if var == 1 else "FALSE"
        if is_number(var):
            return str(var)
        return f"'{var}'"

    def postgresql_query_values(self, field_values, column_types, with_id_field=False):
        return sql_query_values(
            field_values=field_values,
            id_index=self.id_index,
            str_to_sql_typo=self.str_to_postgresql_typo,
            with_id_field=with_id_field,
            column_types=column_types
        )

    def export_from_mysql(self, mysql_db: DatabaseMySQL):
        def wrap_foo():
            self.id_index = mysql_db.id_index
            try:
                #   Drop table before exporting
                self.cursor.execute(f"DROP TABLE IF EXISTS {self.table_name}")
                #   Create empty table
                self.cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {self.table_name} (
                            id SERIAL PRIMARY KEY,
                            price INT NOT NULL,
                            count INT,
                            rating FLOAT,
                            program_name TEXT NOT NULL,
                            program_description TEXT,
                            license_expire_year INT,
                            is_unlimited_license BOOLEAN NOT NULL
                        )
                """)
                for field in mysql_db.fields:
                    sql_query = f"""
                        INSERT INTO {self.table_name} 
                        ({mysql_db.sql_query_field_names(with_id_field=True)}) 
                        VALUES ({self.postgresql_query_values(field, mysql_db.column_types, with_id_field=True)});
                    """
                    self.cursor.execute(sql_query)
                self.db.commit()
                self.update_table_widget()
                self.logger.log("Successfully exported MySQL table data to PostgreSQL")
            except psycopg2.DatabaseError as error:
                self.logger.error_message_box(f"Error trying to export to PostgreSQL! {error}", should_abort=True)
        return wrap_foo

