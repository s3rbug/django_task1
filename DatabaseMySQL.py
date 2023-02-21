import mysql.connector as connector
from mysql.connector import MySQLConnection

from PyQt5.QtWidgets import QTableWidget, QTableWidgetItem, QPushButton
from PyQt5.QtCore import Qt

from utils import is_number, sql_query_values, sql_query_field_names
from Logger import Logger


class DatabaseMySQL:
    def __init__(self, host, user, password, database, table_widget: QTableWidget, logger: Logger):
        self.fields = self.column_names = self.column_types = []
        self.id_index = self.table_rows = self.table_columns = 0
        self.editable = True
        self.db: MySQLConnection | None = None
        self.table_name = "internet_store_licenses"
        self.tableWidget = table_widget
        self.logger = logger
        self.connect(host=host, user=user, password=password, database=database)
        self.cursor = self.db.cursor()
        self.create_sql_table()
        self.update_table_widget()
        self.tableWidget.itemChanged.connect(self.update_field)

    def __del__(self):
        self.cursor.close()
        self.db.close()

    def connect(self, host, user, password, database):
        try:
            self.db = connector.connect(
                host=host,
                user=user,
                password=password,
                database=database,
            )
            self.logger.log(f"Successful connection to MySQL database on host {host}")
        except connector.Error as error:
            self.logger.error_message_box("MySQL connection error! " + error.msg, should_abort=True)

    def create_sql_table(self):
        try:
            self.cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.table_name} (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    price INT NOT NULL,
                    count INT,
                    rating FLOAT,
                    program_name TEXT NOT NULL,
                    program_description TEXT,
                    license_expire_year YEAR,
                    is_unlimited_license BOOLEAN NOT NULL
                )
            """)
            self.db.commit()
        except connector.Error as error:
            self.logger.error_message_box("MySQL error creating table! " + error.msg, should_abort=True)

    def init_table_widget_axis(self):
        try:
            self.cursor.execute(f"SHOW COLUMNS FROM {self.table_name}")
            columns = self.cursor.fetchall()
            self.column_names = [i[0] for i in columns]
            self.column_types = [i[1] for i in columns]
            self.table_columns = len(self.column_names)
            self.tableWidget.setColumnCount(self.table_columns + 1)
            header_labels = self.column_names.copy()
            header_labels.append("")
            self.tableWidget.setHorizontalHeaderLabels(header_labels)
            self.cursor.execute(f"SELECT * FROM {self.table_name} LIMIT 100")
            self.fields = self.cursor.fetchall()
            self.table_rows = len(self.fields)
            self.tableWidget.setRowCount(self.table_rows + 1)
            self.id_index = self.column_names.index("id")
            vertical_labels = ["" for _ in self.fields]
            vertical_labels.append("New item:")
            self.tableWidget.setVerticalHeaderLabels(vertical_labels)
        except connector.Error as error:
            self.logger.error_message_box("MySQL error connecting table! " + error.msg)

    def init_table_widget_fields(self):
        for i, field in enumerate(self.fields):
            for j, value in enumerate(field):
                table_widget_item = QTableWidgetItem(str(value))
                table_widget_item.setTextAlignment(Qt.AlignHCenter | Qt.AlignVCenter)
                self.tableWidget.setItem(i, j, table_widget_item)
            delete_button = QPushButton("Delete item")
            delete_button.clicked.connect(self.delete_field(field[self.id_index]))
            self.tableWidget.setCellWidget(i, len(field), delete_button)

    def init_table_widget_create_item(self):
        add_button = QPushButton("Create item")
        add_button.clicked.connect(self.create_field)
        self.tableWidget.setCellWidget(self.table_rows, self.table_columns, add_button)
        for i in range(self.table_columns):
            if i == self.id_index:
                table_widget_item = QTableWidgetItem("")
            else:
                table_widget_item = QTableWidgetItem()
            table_widget_item.setTextAlignment(Qt.AlignHCenter | Qt.AlignVCenter)
            self.tableWidget.setItem(self.table_rows, i, table_widget_item)

    def update_table_widget(self):
        try:
            self.editable = False
            self.init_table_widget_axis()
            self.init_table_widget_fields()
            self.init_table_widget_create_item()
            self.editable = True
        except connector.Error as error:
            self.logger.error_message_box("MySQL error trying to update table! " + error.msg)

    def delete_field(self, field_id):
        def wrap_foo():
            sql_query = f"DELETE FROM {self.table_name} WHERE id={field_id}"
            self.cursor.execute(sql_query)
            try:
                self.cursor.execute(sql_query)
                self.db.commit()
                self.update_table_widget()
                self.logger.log(f"Deleted MySQL field with id={field_id}")
            except connector.Error as error:
                self.logger.error_message_box("MySQL error trying to delete table item! " + error.msg)

        return wrap_foo

    @staticmethod
    def str_to_mysql_typo(var, _):
        if var == "" or var is None or str(var).lower() == "null":
            return "NULL"
        if is_number(var):
            return str(var)
        return f'"{var}"'

    def mysql_query_values(self, field_values, with_id_field=True):
        return sql_query_values(
            field_values=field_values,
            id_index=self.id_index,
            str_to_sql_typo=self.str_to_mysql_typo,
            with_id_field=with_id_field,
            column_types=self.column_types
        )

    def sql_query_field_names(self, with_id_field=False):
        sql_query = ""
        for i, column_name in enumerate(self.column_names):
            if not with_id_field and i == self.id_index:
                continue
            sql_query = sql_query + column_name
            if not i + 1 == len(self.column_names):
                sql_query = sql_query + ", "
        return sql_query

    def create_field(self):
        new_field_widgets = [self.tableWidget.item(self.table_rows, i) for i in range(self.table_columns)]
        new_field_items = [field_widget.text() if field_widget else "" for field_widget in new_field_widgets]
        with_id_field = False if new_field_items[self.id_index] == "" else True
        create_field_names = sql_query_field_names(
            column_names=self.column_names,
            id_index=self.id_index,
            with_id_field=with_id_field
        )
        create_field_values = self.mysql_query_values(
            field_values=new_field_items,
            with_id_field=with_id_field
        )
        sql_query = f"INSERT {self.table_name}" \
                    f"({create_field_names}) " \
                    f"VALUES ({create_field_values});"
        try:
            self.cursor.execute(sql_query)
            new_field_id = self.cursor.lastrowid
            self.db.commit()
            self.update_table_widget()
            self.logger.log(f"Created MySQL new field with id={new_field_id}")
        except connector.Error as error:
            self.logger.error_message_box("MySQL error trying to add table item! " + error.msg)

    def update_field(self, item: QTableWidgetItem):
        if not self.editable:
            return
        row, column = item.row(), item.column()
        #   Check if SQL field cell
        if row == self.table_rows or column == self.table_columns:
            return
        field = self.column_names[column]
        field_id = self.fields[row][self.id_index]
        new_value = self.str_to_mysql_typo(item.text(), None)
        try:
            self.cursor.execute(f"UPDATE {self.table_name} SET {field}={new_value} WHERE id={field_id}")
            self.db.commit()
            self.logger.log(f"Updated MySQL cell {field} with id {field_id}. New value is {new_value}")
        except connector.Error as error:
            self.logger.error_message_box("MySQL error trying to delete table item! " + error.msg)
        self.update_table_widget()
