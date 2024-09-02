import pandas as pd
from sqlalchemy import insert
from sqlalchemy.dialects.postgresql import insert as pg_insert
from models import Model
from datetime import date, timedelta


class ETL:
    def __init__(self, db_manager):
        self.db_manager = db_manager
        self.model = Model(db_manager.engine)

        # Initialize data attributes
        self.dim_product_family = pd.DataFrame()  # Placeholder for product family data
        self.dim_date = []  # Placeholder for date data
        self.dim_holiday = pd.DataFrame()  # Placeholder for holiday data
        self.fact_sales = pd.DataFrame()  # Placeholder for sales data
        self.dim_city_state = pd.DataFrame()
        self.dim_oil = pd.DataFrame()
        self.dim_store = pd.DataFrame()
        self.aggregate_sales = pd.DataFrame()

    def load_data(self):
        self.sales = pd.read_csv('Data/sales.csv')
        self.stores = pd.read_csv('Data/stores.csv')
        self.oil = pd.read_csv('Data/oil.csv')
        self.holidays = pd.read_csv('Data/holidays.csv')

    def load_dim_oil(self):
        self.dim_oil['date'] = pd.to_datetime(self.oil['date']).dt.date
        self.dim_oil['price'] = self.oil['dcoilwtico'].astype(float)
        self.dim_oil['year'] = self.oil['year'].astype(int)

    def load_dim_store(self):
        self.dim_store = self.stores[['store_nbr', 'city', 'state', 'type', 'cluster']].drop_duplicates().reset_index(
            drop=True)

    def load_dim_products_family(self):
        self.dim_product_family['family'] = self.sales['family'].drop_duplicates().reset_index(drop=True)
        self.dim_product_family['family_id'] = self.dim_product_family.index + 1

    def load_dim_holiday(self):
        self.dim_holiday['date'] = pd.to_datetime(self.holidays['date']).dt.date
        self.dim_holiday['type'] = self.holidays['type'].astype(str)
        self.dim_holiday['locale'] = self.holidays['locale'].astype(str)
        self.dim_holiday['locale_name'] = self.holidays['locale_name'].astype(str)
        self.dim_holiday['description'] = self.holidays['description'].astype(str)
        self.dim_holiday['transferred'] = self.holidays['transferred'].astype(str)
        self.dim_holiday['is_transfered'] = self.holidays['is_transfered'].astype(bool)
        self.dim_holiday['day_of_week'] = self.holidays['day_of_week'].astype(int)
        self.dim_holiday['is_weekend'] = self.holidays['is_weekend'].astype(bool)

    def load_dim_date(self, start_year=2013, end_year=2017):
        current_date = date(start_year, 1, 1)
        end_date = date(end_year + 1, 1, 1)  # Up to but not including end_year + 1
        dim_date_entries = []

        while current_date < end_date:
            weekday = current_date.weekday()
            is_weekend = weekday >= 5
            dim_date_entry = {
                'date': current_date,
                'year': current_date.year,
                'month': current_date.month,
                'day': current_date.day,
                'weekday': weekday,
                'is_weekend': is_weekend
            }
            dim_date_entries.append(dim_date_entry)
            current_date += timedelta(days=1)

        self.dim_date = dim_date_entries  # Store as list of dictionaries

    def load_dim_city_state(self):
        self.dim_city_state['city'] = self.stores['city'].drop_duplicates().reset_index(drop=True)
        self.dim_city_state['state'] = self.stores['state'].drop_duplicates().reset_index(drop=True)
        self.dim_city_state['location_id'] = self.dim_city_state.index + 1

    def load_fact_sale(self):
        self.fact_sales['date'] = self.sales['date']
        self.fact_sales['store_nbr'] = self.sales['store_nbr']
        self.fact_sales['sales'] = self.sales['sales']
        self.fact_sales['onpromotion'] = self.sales['onpromotion']

        self.fact_sales['family_id'] = self.sales['family'].map(
            self.dim_product_family.set_index('family')['family_id']
        )

    def load_aggregate_sales(self):
        try:
            print("\nLoading Aggregate Sales Data Initiated....")

            # Ensure 'date' column is in self.aggregate_sales
            self.aggregate_sales = self.fact_sales[['date', 'store_nbr', 'family_id', 'sales', 'onpromotion']].copy()

            # Convert 'date' column to datetime
            self.aggregate_sales['date'] = pd.to_datetime(self.aggregate_sales['date'], errors='coerce')

            # Rename 'sales' to 'sale_amount'
            self.aggregate_sales.rename(columns={'sales': 'sale_amount'}, inplace=True)

            # Extract day, month, and year from 'date' and add them as new columns
            self.aggregate_sales['day'] = self.aggregate_sales['date'].dt.day
            self.aggregate_sales['month'] = self.aggregate_sales['date'].dt.month
            self.aggregate_sales['year'] = self.aggregate_sales['date'].dt.year

            # Ensure 'date' columns in dim_product_family and dim_holiday are also in datetime format
            self.dim_holiday['date'] = pd.to_datetime(self.dim_holiday['date'], errors='coerce')

            # Merge with dim_product_family to get family_name
            if 'family_id' in self.dim_product_family.columns:
                self.aggregate_sales = self.aggregate_sales.merge(
                    self.dim_product_family[['family_id', 'family']],
                    on='family_id',
                    how='left'
                )
                self.aggregate_sales.rename(columns={'family': 'family_name'}, inplace=True)
            else:
                raise ValueError("Column 'family_id' not found in self.dim_product_family.")

            # Merge with dim_holiday to get holiday information
            self.aggregate_sales = self.aggregate_sales.merge(
                self.dim_holiday[['date', 'type', 'is_weekend', 'description']],
                on='date',
                how='left'
            )
            self.aggregate_sales.rename(columns={'type': 'holiday_type', 'description': 'holiday_description'},
                                        inplace=True)

            # Set holiday_type and holiday_description to None where is_holiday is False
            self.aggregate_sales.loc[
                self.aggregate_sales['holiday_type'].isna(), ['holiday_type', 'holiday_description']] = None

            self.aggregate_sales['is_holiday'] = self.aggregate_sales['holiday_type'].notna().astype(int)

            # Merge with dim_store to get store information
            self.aggregate_sales = self.aggregate_sales.merge(
                self.dim_store[['store_nbr', 'city', 'state', 'type']],
                on='store_nbr',
                how='left'
            )
            self.aggregate_sales.rename(columns={'type': 'store_type', 'state': 'store_state', 'city': 'store_city'},
                                        inplace=True)

            # Convert boolean columns with NaN to False
            bool_columns = ['is_weekend']
            self.aggregate_sales[bool_columns] = self.aggregate_sales[bool_columns].fillna(False)

            # Ensure only expected columns are included
            expected_columns = ['date', 'store_nbr', 'family_id', 'is_holiday', 'store_type', 'store_state',
                                'store_city',
                                'family_name', 'sale_amount', 'onpromotion', 'is_weekend', 'day', 'month', 'year']
            self.aggregate_sales = self.aggregate_sales[expected_columns]

            print("\nLoading Aggregate Sales Data Successfully Executed....")

        except KeyError as e:
            print(f"KeyError: {str(e)}")
            print("Ensure that all required columns are present in the fact_sales DataFrame.")
            raise
        except Exception as e:
            print(f"Error preparing data for database insertion: {str(e)}")
            raise

    def load_to_db(self, chunk_size=10000):
        try:
            self.load_dim_oil()
            self.load_dim_store()
            self.load_dim_holiday()
            self.load_dim_date()
            self.load_dim_city_state()
            self.load_dim_products_family()
            self.load_fact_sale()
            self.load_aggregate_sales()

            connection = self.db_manager.engine.connect()

            try:

                if not self.dim_oil.empty:
                    transaction = connection.begin()
                    self.dim_oil.fillna(0, inplace=True)
                    data_to_insert = self.dim_oil.to_dict(orient='records')
                    insert_stmt = pg_insert(self.model.dim_oil).values(data_to_insert)
                    on_conflict_stmt = insert_stmt.on_conflict_do_nothing()
                    connection.execute(on_conflict_stmt)
                    transaction.commit()
                    print("\nData Successfully stored into Oil Dimension\n")

                if not self.dim_store.empty:
                    transaction = connection.begin()
                    insert_stmt = pg_insert(self.model.dim_store).values(self.dim_store.to_dict(orient='records'))
                    on_conflict_stmt = insert_stmt.on_conflict_do_nothing(index_elements=['store_nbr'])
                    connection.execute(on_conflict_stmt)
                    transaction.commit()
                    print("\nData Successfully stored into Store Dimension\n")

                if not self.dim_product_family.empty:
                    transaction = connection.begin()
                    insert_stmt = pg_insert(self.model.dim_product_family).values(self.dim_product_family.to_dict(orient='records'))
                    on_conflict_stmt = insert_stmt.on_conflict_do_nothing(index_elements=['family_id'])
                    connection.execute(on_conflict_stmt)
                    transaction.commit()
                    print("\nData Successfully stored into Product Family Dimension\n")

                if self.dim_date:  # Check if dim_date is not empty
                    transaction = connection.begin()
                    insert_stmt = pg_insert(self.model.dim_date).values(self.dim_date)
                    on_conflict_stmt = insert_stmt.on_conflict_do_nothing(index_elements=['date'])
                    connection.execute(on_conflict_stmt)
                    transaction.commit()
                    print("\nData Successfully stored into Date Dimension\n")

                if not self.dim_holiday.empty:
                    transaction = connection.begin()
                    insert_stmt = pg_insert(self.model.dim_holiday).values(self.dim_holiday.to_dict(orient='records'))
                    on_conflict_stmt = insert_stmt.on_conflict_do_nothing(index_elements=['date', 'locale', 'locale_name'])
                    connection.execute(on_conflict_stmt)
                    transaction.commit()
                    print("\nData Successfully stored into Holiday Dimension\n")

                if not self.dim_city_state.empty:
                    transaction = connection.begin()
                    insert_stmt = pg_insert(self.model.dim_city_state).values(self.dim_city_state.to_dict(orient='records'))
                    on_conflict_stmt = insert_stmt.on_conflict_do_nothing(index_elements=['city', 'state'])
                    connection.execute(on_conflict_stmt)
                    transaction.commit()
                    print("\nData Successfully stored into City-State Dimension\n")

                if not self.fact_sales.empty:
                    transaction = connection.begin()
                    insert_stmt = pg_insert(self.model.fact_sales).values(self.fact_sales.to_dict(orient='records'))
                    on_conflict_stmt = insert_stmt.on_conflict_do_nothing(index_elements=['date', 'store_nbr', 'family_id'])
                    connection.execute(on_conflict_stmt)
                    transaction.commit()
                    print("\nData Successfully stored into Sales Fact\n")


                if not self.aggregate_sales.empty:
                    self._insert_chunked(connection, self.model.aggregate_sales, self.aggregate_sales, chunk_size,
                                         index_elements=['date', 'store_nbr', 'family_id'])
                    print("\nData Successfully stored into Sale Aggregate\n")

            except Exception as e:
                print(f"Error loading data to database: {str(e)}")
            finally:
                connection.close()
                print("Database connection closed!!!")

        except Exception as e:
            print(f"Error preparing data for database insertion: {str(e)}")

    def _insert_chunked(self, connection, table_model, data_frame, chunk_size, index_elements=None):
        count = 1
        try:
            for start in range(0, len(data_frame), chunk_size):
                print(f"\nData Insertion Chunk No: {count}\n")
                end = min(start + chunk_size, len(data_frame))
                chunk = data_frame.iloc[start:end].to_dict(orient='records')

                transaction = connection.begin()
                insert_stmt = pg_insert(table_model).values(chunk)
                if index_elements:
                    on_conflict_stmt = insert_stmt.on_conflict_do_nothing(index_elements=index_elements)
                else:
                    on_conflict_stmt = insert_stmt.on_conflict_do_nothing()
                connection.execute(on_conflict_stmt)
                transaction.commit()

                count += 1

            # Insert remaining rows (less than chunk_size)
            remaining_rows = len(data_frame) % chunk_size
            if remaining_rows > 0:
                print(f"\nInserting remaining {remaining_rows} rows...\n")
                remaining_chunk = data_frame.tail(remaining_rows).to_dict(orient='records')
                transaction = connection.begin()
                insert_stmt = pg_insert(table_model).values(remaining_chunk)
                if index_elements:
                    on_conflict_stmt = insert_stmt.on_conflict_do_nothing(index_elements=index_elements)
                else:
                    on_conflict_stmt = insert_stmt.on_conflict_do_nothing()
                connection.execute(on_conflict_stmt)
                transaction.commit()

        except Exception as e:
            print(f"Error inserting chunked data into database: {str(e)}")
            raise

