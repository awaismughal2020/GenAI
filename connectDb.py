from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import declarative_base, sessionmaker
import pandas as pd

Base = declarative_base()

class DatabaseManager:
    def __init__(self, username, password, host, database_name):
        self.username = username
        self.password = password
        self.host = host
        self.database_name = database_name
        self.engine = self.create_engine()
        self.Session = sessionmaker(bind=self.engine)
        self.metadata = MetaData()
        self.reflect_metadata()

    def create_engine(self):
        db_url = f'postgresql+psycopg2://{self.username}:{self.password}@{self.host}/{self.database_name}'
        return create_engine(db_url)

    def reflect_metadata(self):
        self.metadata.reflect(bind=self.engine)

    def test_connection(self):
        try:
            with self.engine.connect() as connection:
                print("Successfully connected to the database!")
                return True
        except Exception as e:
            print(f"Error connecting to the database: {e}")
            return False

    def get_engine(self):
        return self.engine

    def get_session(self):
        return self.Session()

    def query(self, sql):
        with self.engine.connect() as connection:
            df = pd.read_sql(sql, connection)
        return df
