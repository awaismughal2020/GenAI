from LangChain.dbConnection import connection
from LangChain.queries import dbQueries


def execute_task(task_func, session, *args, **kwargs):
    return task_func(session, *args, **kwargs)



def call_session():
    db_connection = connection()
    return db_connection.callSession()


def get_product_families():
    session = call_session()
    queries = dbQueries()
    family_name_task = lambda: execute_task(queries.get_all_family_names, session)
    return family_name_task()


def get_product_sales_details(product_type, day, month, year, store, is_sum):
    session = call_session()
    queries = dbQueries()
    values = execute_task(queries.get_product_details, session, product_type, day, month, year, store, is_sum)
    return values


