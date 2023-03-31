def is_number(n):
    """Перевірка на числове значення"""
    try:
        float(n)
    except ValueError:
        return False
    return True

def sql_query_field_names(column_names, id_index, with_id_field=True):
    """Назви полів у SQL формат"""
    sql_query = ""
    for i, column_name in enumerate(column_names):
        if not with_id_field and i == id_index:
            continue
        sql_query = sql_query + column_name
        if not i + 1 == len(column_names):
            sql_query = sql_query + ", "
    return sql_query

def sql_query_values(field_values, id_index, column_types, str_to_sql_typo, with_id_field=True):
    """Значення полів у SQL формат"""
    sql_query = ""
    for i, field_value in enumerate(field_values):
        if not with_id_field and i == id_index:
            continue
        sql_query = sql_query + str_to_sql_typo(field_value, column_types[i])
        if not i + 1 == len(field_values):
            sql_query = sql_query + ", "
    return sql_query


def connector_decorator(func):
    def wrapper(*args, **kwargs):
        def function():
            func(*args, **kwargs)
        return function
    return wrapper
