import sys
from PyQt5.QtWidgets import QApplication
from PyQt5 import QtWidgets, uic

from DatabaseMySQL import DatabaseMySQL
from DatabasePostgreSQL import DatabasePostgreSQL
from DatabaseSQLite import DatabaseSQLite
from config import MYSQL, POSTGRESQL, SQLITE
from Logger import Logger


class MainWindow(QtWidgets.QMainWindow):
    def __init__(self):
        super(MainWindow, self).__init__()
        uic.loadUi('MainWindowForm.ui', self)
        self.setWindowTitle("Лабораторна робота №1")
        self.logger = Logger(self.label_log)
        self.dbMySql = DatabaseMySQL(
            host=MYSQL["host"],
            user=MYSQL["user"],
            password=MYSQL["password"],
            database=MYSQL["database"],
            table_widget=self.mysql_table,
            logger=self.logger
        )
        self.dbPostgreSQL = DatabasePostgreSQL(
            host=POSTGRESQL["host"],
            user=POSTGRESQL["user"],
            password=POSTGRESQL["password"],
            database=POSTGRESQL["database"],
            table_widget=self.postgresql_table,
            logger=self.logger
        )
        self.dbSQLite = DatabaseSQLite(
            filename=SQLITE["filename"],
            table_widget=self.sqlite_table,
            logger=self.logger
        )
        self.export_to_postgres_button.clicked.connect(
            self.dbPostgreSQL.migrate_from_mysql(self.dbMySql)
        )
        self.export_fields_button.clicked.connect(
            self.dbPostgreSQL.export_to_sqlite(self.export_fields_lineedit, self.dbSQLite)
        )
        self.show()


if __name__ == "__main__":
    app = QApplication(sys.argv)
    widget = MainWindow()
    app.exec_()
