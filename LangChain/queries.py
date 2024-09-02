import pandas as pd


class dbQueries:
    def get_sales_summary_by_family(self, session):
        query = """
        SELECT family_name, SUM("SalesSum2013") AS Sales2013, SUM("SalesSum2014") AS Sales2014,
               SUM("SalesSum2015") AS Sales2015, SUM("SalesSum2016") AS Sales2016,
               SUM("SalesSum2017") AS Sales2017, SUM("SalesSum2018") AS Sales2018
        FROM summary_family_sales
        GROUP BY family_name
        """
        df = pd.read_sql(query, session.bind)
        return df

    def get_sales_summary_by_store(self, session):
        query = """
        SELECT store_nbr, SUM("SalesSum2013") AS Sales2013, SUM("SalesSum2014") AS Sales2014,
               SUM("SalesSum2015") AS Sales2015, SUM("SalesSum2016") AS Sales2016,
               SUM("SalesSum2017") AS Sales2017, SUM("SalesSum2018") AS Sales2018
        FROM summary_store_sales
        GROUP BY store_nbr
        """
        df = pd.read_sql(query, session.bind)
        return df

    def get_sales_trends(self, session):
        query = """
        SELECT date, SUM(sale_amount) AS total_sales
        FROM aggregate_sales
        GROUP BY date
        ORDER BY date
        """
        df = pd.read_sql(query, session.bind)
        return df

    def get_dynamic_sales_summary(self, session, years, summary_type):
        if isinstance(years, str):  # If years is a single string (single year)
            years = [years]

        sales_columns = ', '.join([f'"SalesSum{year}" AS Sales{year}' for year in years])

        if summary_type == 'family':
            query = f"""
            SELECT family_name, {sales_columns}
            FROM summary_family_sales
            """
        elif summary_type == 'store':
            query = f"""
            SELECT store_nbr, {sales_columns}
            FROM summary_store_sales
            """

        df = pd.read_sql(query, session.bind)
        return df

    def get_all_family_names(self, session):
        query = """
           SELECT family_id, family_name
           FROM summary_family_sales
           """
        df = pd.read_sql(query, session.bind)
        return df

    def handle_none(self, value):
        return 0 if value is None or value == "None" else value

    def get_product_details(self, session, product_type, day, month, year, store, isSum):
        # Handle None values
        product_type = self.handle_none(product_type)
        day = self.handle_none(day)
        month = self.handle_none(month)
        year = self.handle_none(year)
        store = self.handle_none(store)
        isSum = self.handle_none(isSum)

        if isSum:
            if year:
                if not day and not month:
                    if store and not product_type:
                        query = f"""SELECT * FROM summary_store_sales WHERE store_nbr = {store}"""
                        df = pd.read_sql(query, session.bind)
                        column_name = f'SalesSum{year}'
                        return df[column_name]

                    if not store and product_type:
                        query = f"SELECT * FROM summary_family_sales WHERE family_id = {product_type}"
                        df = pd.read_sql(query, session.bind)
                        column_name = f'SalesSum{year}'
                        return df[column_name]

                    if store and product_type:
                        query = f"""
                            SELECT SUM(sale_amount) FROM aggregate_sales 
                            WHERE store_nbr = {store} AND year = {year} AND family_id = {product_type}
                        """
                        df = pd.read_sql(query, session.bind)
                        return df

                if day and month and product_type:
                    query = f"""
                        SELECT SUM(sale_amount) FROM aggregate_sales 
                        WHERE day = {day} AND month = {month} AND year = {year} AND family_id = {product_type}
                    """

                    if store:
                        query += f" AND store_nbr = {store}"

                    df = pd.read_sql(query, session.bind)
                    return df
                else:
                    if day and month and not product_type:
                        query = f"""SELECT SUM(sale_amount) FROM aggregate_sales WHERE
                                day = {day} AND month = {month} AND year = {year} AND store_nbr = {store}"""
                        df = pd.read_sql(query, session.bind)
                        return df
            else:
                if product_type:
                    if not day and not year and not month:
                        query = f"""SELECT SUM(sale_amount) FROM aggregate_sales WHERE
                                                       family_id = {product_type}"""
                        df = pd.read_sql(query, session.bind)
                        return df

        return None


