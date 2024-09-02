from sqlalchemy import Table, Column, Integer, Float, String, Date, Boolean, MetaData, UniqueConstraint
from load_dotenv import load_dotenv

load_dotenv()

class Model:
    def __init__(self, engine):
        self.engine = engine
        self.metadata = MetaData()

        # Reflect existing tables into metadata
        self.metadata.reflect(bind=self.engine)

        # Ensure all table attributes are set
        self.dim_oil = self.metadata.tables.get('dim_oil')
        self.dim_store = self.metadata.tables.get('dim_store')
        self.dim_product_family = self.metadata.tables.get('dim_product_family')
        self.dim_date = self.metadata.tables.get('dim_date')
        self.dim_holiday = self.metadata.tables.get('dim_holiday')
        self.dim_city_state = self.metadata.tables.get('dim_city_state')
        self.fact_sales = self.metadata.tables.get('fact_sales')
        self.aggregate_sales = self.metadata.tables.get('aggregate_sales')
        self.summary_family_sales = self.metadata.tables.get('summary_family_sales')
        self.summary_store_sales = self.metadata.tables.get('summary_store_sales')

    def create_tables(self):
        tables_to_create = []

        if not self.dim_oil:
            self.dim_oil = Table(
                'dim_oil', self.metadata,
                Column('id', Integer, primary_key=True, autoincrement=True),
                Column('date', Date),
                Column('price', Float),
                Column('year', Integer)
            )
            tables_to_create.append(self.dim_oil)

        if not self.dim_store:
            self.dim_store = Table(
                'dim_store', self.metadata,
                Column('store_nbr', Integer, primary_key=True),
                Column('city', String),
                Column('state', String),
                Column('type', String),
                Column('cluster', Integer)
            )
            tables_to_create.append(self.dim_store)

        if not self.dim_product_family:
            self.dim_product_family = Table(
                'dim_product_family', self.metadata,
                Column('family_id', Integer, primary_key=True),
                Column('family', String)
            )
            tables_to_create.append(self.dim_product_family)

        if not self.dim_date:
            self.dim_date = Table(
                'dim_date', self.metadata,
                Column('date', Date, primary_key=True),
                Column('year', Integer),
                Column('month', Integer),
                Column('day', Integer),
                Column('weekday', Integer),
                Column('is_weekend', Boolean)
            )
            tables_to_create.append(self.dim_date)

        if not self.dim_holiday:
            self.dim_holiday = Table(
                'dim_holiday', self.metadata,
                Column('date', Date, primary_key=True),
                Column('type', String),
                Column('locale', String),
                Column('locale_name', String),
                Column('description', String),
                Column('transferred', String),
                Column('is_transfered', Boolean),
                Column('day_of_week', Integer),
                Column('is_weekend', Boolean),
                UniqueConstraint('date', 'locale', 'locale_name', name='uq_dim_holiday')
            )
            tables_to_create.append(self.dim_holiday)

        if not self.dim_city_state:
            self.dim_city_state = Table(
                'dim_city_state', self.metadata,
                Column('location_id', Integer, primary_key=True),
                Column('city', String),
                Column('state', String),
                UniqueConstraint('city', 'state', name='uq_dim_city_state')
            )
            tables_to_create.append(self.dim_city_state)

        if not self.fact_sales:
            self.fact_sales = Table(
                'fact_sales', self.metadata,
                Column('date', Date),
                Column('store_nbr', Integer),
                Column('family_id', Integer),
                Column('sales', Float),
                Column('onpromotion', Integer),
                UniqueConstraint('date', 'store_nbr', 'family_id', name='uq_fact_sales')
            )
            tables_to_create.append(self.fact_sales)

        if not self.aggregate_sales:
            self.aggregate_sales = Table(
                'aggregate_sales', self.metadata,
                Column('date', Date),
                Column('day', Integer),
                Column('month', Integer),
                Column('year', Integer),
                Column('is_holiday', Boolean),
                Column('is_weekend', Boolean),
                Column('holiday_type', String),
                Column('holiday_description', String),
                Column('store_nbr', Integer),
                Column('store_city', String),
                Column('store_state', String),
                Column('store_type', String),
                Column('family_id', Integer),
                Column('family_name', String),
                Column('sale_amount', Float),
                Column('onpromotion', Integer),
                UniqueConstraint('date', 'store_nbr', 'family_id', name='uq_aggregate_sales')
            )
            tables_to_create.append(self.aggregate_sales)

        if not self.summary_family_sales:
            self.summary_family_sales = Table(
                'summary_family_sales', self.metadata,
                Column('family_id', Integer),
                Column('family_name', String),
                Column('SalesSum2013', Float),
                Column('SalesSum2014', Float),
                Column('SalesSum2015', Float),
                Column('SalesSum2016', Float),
                Column('SalesSum2017', Float),
                Column('SalesSum2018', Float),
                UniqueConstraint('family_id', name='uq_summary_family_sales')
            )
            tables_to_create.append(self.summary_family_sales)

        if not self.summary_store_sales:
            self.summary_store_sales = Table(
                'summary_store_sales', self.metadata,
                Column('store_nbr', Integer),
                Column('SalesSum2013', Float),
                Column('SalesSum2014', Float),
                Column('SalesSum2015', Float),
                Column('SalesSum2016', Float),
                Column('SalesSum2017', Float),
                Column('SalesSum2018', Float),
                UniqueConstraint('store_nbr', name='uq_summary_store_sales')
            )
            tables_to_create.append(self.summary_family_sales)

        if tables_to_create:
            self.metadata.create_all(self.engine)
            print("Tables created successfully.")
        else:
            print("All tables already exist. Skipping table creation.")

