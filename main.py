import os
from connectDb import DatabaseManager
from etl import ETL
from load_dotenv import load_dotenv

load_dotenv()

if __name__ == "__main__":
    db_params = {
        'username': os.getenv('LOCAL_USER'),
        'password': os.getenv('LOCAL_PASS'),
        'host': os.getenv('LOCAL_DB_HOST'),
        'database_name': os.getenv('LOCAL_DATABASE')
    }

    if None in db_params.values():
        raise ValueError("One or more database connection parameters are not set correctly.")

    # Create DatabaseManager instance
    db_manager = DatabaseManager(
        username=db_params['username'],
        password=db_params['password'],
        host=db_params['host'],
        database_name=db_params['database_name']
    )

    # Check database connection
    if db_manager.test_connection():
        print("Database connection successful.")

        # Initialize ETL process
        etl = ETL(db_manager)
        # etl.load_data()
        # Create tables if they do not exist
        # etl.model.create_tables()
        # Load transformed data into database
        # etl.load_to_db()

    else:
        print("Database connection failed. ETL process aborted.")
