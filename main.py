import sys
from MainWindow import MainWindow
from PyQt5.QtWidgets import QApplication

if __name__ == "__main__":
    app = QApplication(sys.argv)
    widget = MainWindow()
    app.exec_()
