import psycopg2
import re
from PyQt5.QtWidgets import QTableWidget, QTableWidgetItem, QLineEdit
from Logger import Logger
from DatabaseMySQL import DatabaseMySQL
from utils import sql_query_values, is_number
from PyQt5.QtCore import Qt


class DatabasePostgreSQL:
    def __init__(self, host, user, password, database, logger: Logger, table_widget: QTableWidget):
        #   Ініціалізація змінних
        self.tableWidget = table_widget
        self.table_name = "internet_store_licenses"
        self.editable = True
        self.column_names = self.fields = self.column_types = []
        self.id_index = self.table_columns = self.table_rows = 0
        self.logger = logger
        self.db: None | psycopg2.connection = None
        #   З'єднання з базою даних
        self.connect(host, user, password, database)
        self.cursor = self.db.cursor()
        #   Обробка зміни комірки в таблиці
        self.tableWidget.itemChanged.connect(self.update_field)

    def connect(self, host, user, password, database):
        """З'єднання з базою PostgreSQL"""
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
        """Ініціалізація назв комірок у таблиці"""
        try:
            self.cursor.execute(f"SELECT * FROM {self.table_name} LIMIT 0")
            self.column_names = [i[0] for i in self.cursor.description]
            self.table_columns = len(self.column_names)
            self.tableWidget.setColumnCount(self.table_columns)
            self.tableWidget.setHorizontalHeaderLabels(self.column_names)
            self.cursor.execute(f"SELECT * FROM {self.table_name} LIMIT 100")
            self.fields = self.cursor.fetchall()
            self.table_rows = len(self.fields)
            self.tableWidget.setRowCount(self.table_rows)
            self.id_index = self.column_names.index("id")
            self.tableWidget.setVerticalHeaderLabels("" for _ in self.fields)
        except psycopg2.DatabaseError as error:
            self.logger.error_message_box(f"PostgreSQL error connecting table! {error}")
            self.db.rollback()

    def init_table_widget_fields(self):
        """Заповнення таблиці значеннями"""
        for i, field in enumerate(self.fields):
            for j, value in enumerate(field):
                table_widget_item = QTableWidgetItem(str(value))
                table_widget_item.setTextAlignment(Qt.AlignHCenter | Qt.AlignVCenter)
                self.tableWidget.setItem(i, j, table_widget_item)

    def update_table_widget(self):
        """Оновити PyQt віджет для зображення бази даних"""
        try:
            self.editable = False
            self.init_table_widget_axis()
            self.init_table_widget_fields()
            self.editable = True
        except psycopg2.DatabaseError as error:
            self.logger.error_message_box(f"PostgreSQL error trying to update table! {error}")
            self.db.rollback()

    @staticmethod
    def str_to_postgresql_typo(var, field_type):
        """Конвертація значення змінної у PostgreSQL вигляд"""
        if var == "" or var is None or str(var).lower() == "null":
            return "NULL"
        if field_type == b'tinyint(1)':
            return "TRUE" if var == 1 else "FALSE"
        if is_number(var):
            return str(var)
        return f"'{var}'"

    def postgresql_query_values(self, field_values, column_types, with_id_field=True):
        """Повертає значення змінних в PostgreSQL форматі"""
        return sql_query_values(
            field_values=field_values,
            id_index=self.id_index,
            str_to_sql_typo=self.str_to_postgresql_typo,
            with_id_field=with_id_field,
            column_types=column_types
        )

    def migrate_from_mysql(self, mysql_db: DatabaseMySQL):
        """Міграція з MySQL"""
        def wrap_foo():
            self.id_index = mysql_db.id_index
            self.column_types = mysql_db.column_types
            try:
                #   Очистити таблицю перед міграцією
                self.cursor.execute(f"DROP TABLE IF EXISTS {self.table_name}")
                #   Створення порожньої таблиці
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
                    query_field_names = mysql_db.sql_query_field_names(with_id_field=True)
                    query_field_values = self.postgresql_query_values(
                        field_values=field,
                        column_types=mysql_db.column_types,
                    )
                    sql_query = f"""
                        INSERT INTO {self.table_name} 
                        ({query_field_names}) 
                        VALUES ({query_field_values});
                    """
                    self.cursor.execute(sql_query)
                self.db.commit()
                self.update_table_widget()
                self.logger.log("Successfully exported MySQL table data to PostgreSQL")
            except psycopg2.DatabaseError as error:
                self.logger.error_message_box(f"Error trying to export to PostgreSQL! {error}", should_abort=True)
                self.db.rollback()

        return wrap_foo

    def update_field(self, item: QTableWidgetItem):
        """Оновлення значення в БД"""
        if not self.editable:
            return
        row, column = item.row(), item.column()
        field = self.column_names[column]
        field_id = self.fields[row][self.id_index]
        new_value = self.str_to_postgresql_typo(item.text(), self.column_types[column])
        try:
            self.cursor.execute(f"UPDATE {self.table_name} SET {field}={new_value} WHERE id={field_id}")
            self.db.commit()
            self.logger.log(f"Updated PostgreSQL cell {field} with id {field_id}. New value is {new_value}")
        except psycopg2.DatabaseError as error:
            self.logger.error_message_box(f"PostgreSQL error trying to update table item! {error}")
            self.db.rollback()
        self.update_table_widget()

    def export_to_sqlite(self, export_fields_lineedit: QLineEdit, db_sqlite):
        """Експорт в SQLite"""
        def wrap_foo():
            export_fields_text = export_fields_lineedit.text()
            export_fields = re.split(",\\s*|\\s+", export_fields_text)
            for export_field in export_fields:
                if export_field not in self.column_names:
                    self.logger.error_message_box(f"{export_field} is not a valid field name.\n"
                                                  f"Valid field names are: {self.column_names}")
                    return
            db_sqlite.migrate_from_postgresql(self, export_fields)

        return wrap_foo
