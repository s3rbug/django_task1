import sys

from PyQt5.QtWidgets import QLabel, QMessageBox


class Logger:
    """Клас для логування"""
    def __init__(self, label: QLabel):
        self.label = label

    def log(self, message, tag="INFO"):
        self.label.setText(f"{self.label.text()}[{tag}] - {message}<br/>")

    def error_message_box(self, text, window_title="Error", should_abort=False):
        message_box = QMessageBox()
        message_box.setText(text)
        message_box.setIcon(QMessageBox.Critical)
        message_box.setWindowTitle(window_title)
        message_box.exec_()
        self.log(text, tag="ERROR")
        if should_abort:
            sys.exit(1)
