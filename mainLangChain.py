from LangChain.dbConnection import connection
from LangChain.queries import dbQueries


def execute_task(task_func, session):
    return task_func(session)

def execute_dynamic_task(task_func, session, var1, var2):
    return task_func(session, var1, var2)

def generate_insights(family_sales_summary, store_sales_summary, sales_trends):
    insights = f"Sales Summary by Family:\n{family_sales_summary}\n\n"
    insights += f"Sales Summary by Store:\n{store_sales_summary}\n\n"
    insights += f"Sales Trends Over Time:\n{sales_trends}\n"
    return insights




if __name__ == "__main__":
    # Create an instance of the connection class
    db_connection = connection()

    # Call the callSession method on the instance
    session = db_connection.callSession()

    # Create an instance of dbQueries
    queries = dbQueries()

    # Define the tasks
    family_sales_summary_task = lambda: execute_task(queries.get_sales_summary_by_family, session)
    store_sales_summary_task = lambda: execute_task(queries.get_sales_summary_by_store, session)
    sales_trends_task = lambda: execute_task(queries.get_sales_trends, session)

    # Execute the tasks
    family_sales_summary = family_sales_summary_task()
    store_sales_summary = store_sales_summary_task()
    sales_trends = sales_trends_task()


    # insights = generate_insights(family_sales_summary, store_sales_summary, sales_trends)
    # print(insights)
    year = ['2017','2018']
    summary_type = 'family'
    dynamic_sales_task = lambda: execute_dynamic_task(queries.get_dynamic_sales_summary, session, year, summary_type)

    print(dynamic_sales_task())
    # execute_task(, session, summary_type)
