# Практикум №1

## Використання СУБД SQLite, MySQL, PostgreSQL з мовою програмування Python 3

### Запуск програми
Для того, щоб коректно запустити програму потрібно налаштувати файл **config.py:**

```
MYSQL = {
    "host": "назва хоста MySQL",
    "user": "ім'я користувача MySQL",
    "password": "пароль користувача MySQL",
    "database": "назва бази даних MySQL",
}

POSTGRESQL = {
    "host": "назва хоста PostgreSQL",
    "user": "ім'я користувача PostgreSQL",
    "password": "пароль користувача PostgreSQL",
    "database": "назва бази даних PostgreSQL",
}

SQLITE = {
    "filename": "назва файлу для створення SQLite бази даних"
}
```