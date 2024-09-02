import os

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from load_dotenv import load_dotenv

load_dotenv()

class connection:
    def callSession(self):
        DATABASE_URL = os.getenv('LOCAL_DATABASE_URL')  # Update with your database URL
        engine = create_engine(DATABASE_URL)
        Session = sessionmaker(bind=engine)
        return Session()

