import os
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
from analysis import Analysis
from connectDb import DatabaseManager
from cachetools import cached, TTLCache
from multiprocessing import Pool
import plotly.express as px


def create_figure(params, sales_data):
    title, filter_condition, x, y, color = params
    filtered_data = sales_data[filter_condition]
    fig = px.histogram(filtered_data, x=x, y=y, color=color, title=title)
    return fig


def create_bar_figure(sales_data):
    # Melt the DataFrame to long format
    sales_data_long = sales_data.melt(id_vars=['family_name'],
                                      value_vars=['SalesSum2013', 'SalesSum2014', 'SalesSum2015',
                                                  'SalesSum2016', 'SalesSum2017', 'SalesSum2018'],
                                      var_name='Year',
                                      value_name='SalesSum')
    sales_data_long['Year'] = sales_data_long['Year'].str.replace('SalesSum', '')
    sales_data_long['Year'] = sales_data_long['Year'].astype(int)

    # Create the bar plot
    fig = px.bar(sales_data_long, x='Year', y='SalesSum', color='family_name',
                 title='Sales Sum by Family (2013-2018)', barmode='group',
                 category_orders={'Year': list(range(2013, 2019))},
                 log_y=True)  # Set log scale for y-axis

    # Update layout to improve readability
    fig.update_layout(
        xaxis_title='Year',
        yaxis_title='Sales Sum (Log Scale)',
        legend_title='Family Name'
    )

    return fig


def create_store_bar_figure(sales_data):
    # Melt the DataFrame to long format
    sales_data_long = sales_data.melt(id_vars=['store_nbr'],
                                      value_vars=['SalesSum2013', 'SalesSum2014', 'SalesSum2015',
                                                  'SalesSum2016', 'SalesSum2017', 'SalesSum2018'],
                                      var_name='Year',
                                      value_name='SalesSum')
    sales_data_long['Year'] = sales_data_long['Year'].str.replace('SalesSum', '')
    sales_data_long['Year'] = sales_data_long['Year'].astype(int)

    # Create the bar plot
    fig = px.bar(sales_data_long, x='Year', y='SalesSum', color='store_nbr',
                 title='Sales Sum by Store Number (2013-2018)', barmode='group',
                 category_orders={'Year': list(range(2013, 2019))},
                 log_y=True)  # Set log scale for y-axis

    # Update layout to improve readability
    fig.update_layout(
        xaxis_title='Year',
        yaxis_title='Sales Sum (Log Scale)',
        legend_title='Store Number'
    )

    return fig


class Dashboard:
    def __init__(self, analysis_instance):
        self.analysis = analysis_instance
        self.app = dash.Dash(__name__)
        self.app.layout = html.Div([
            html.H1('Analytics Dashboard'),
            dcc.Tabs([
                dcc.Tab(label='Sales Count', children=[
                    dcc.Graph(id='sales-count-city'),
                    dcc.Graph(id='sales-count-store-type'),
                    dcc.Graph(id='sales-count-store-state'),
                    dcc.Graph(id='sales-count-years'),
                    dcc.Graph(id='sales-count-months')
                ]),
                dcc.Tab(label='Family Type Sales Count', children=[
                    dcc.Graph(id='family-sales-count-city'),
                    dcc.Graph(id='family-sales-count-store-type'),
                    dcc.Graph(id='family-sales-count-store-state'),
                    dcc.Graph(id='family-sales-count-years'),
                    dcc.Graph(id='family-sales-count-months'),
                    dcc.Graph(id='family-sales-sum-bar')
                ]),
                dcc.Tab(label='Special Analysis', children=[
                    dcc.Graph(id='highest-sales'),
                    dcc.Graph(id='lowest-sales'),
                    dcc.Graph(id='store-sales-sum-bar')  # Added new tab for store sales bar chart
                ]),
            ]),
            dcc.Interval(
                id='interval-component',
                interval=60 * 1000,  # in milliseconds
                n_intervals=0
            )
        ])

        self.register_callbacks()

    @cached(cache=TTLCache(maxsize=10, ttl=300))
    def query_aggregate_sales_data(self):
        sales_data = self.analysis.query_aggregate_sales_data()
        print("Sales Data Loaded:\n", sales_data.head())  # Debugging print
        return sales_data

    @cached(cache=TTLCache(maxsize=10, ttl=300))
    def query_family_sales_data(self):
        query = """
        SELECT 
            "family_name", 
            "SalesSum2013", 
            "SalesSum2014", 
            "SalesSum2015", 
            "SalesSum2016", 
            "SalesSum2017", 
            "SalesSum2018" 
        FROM "summary_family_sales"
        """
        df = self.analysis.db_manager.query(query)
        print("Family Sales Data Loaded:\n", df.head())  # Debugging print
        return df

    @cached(cache=TTLCache(maxsize=10, ttl=300))
    def query_store_sales_data(self):
        query = """
        SELECT 
            "store_nbr", 
            "SalesSum2013", 
            "SalesSum2014", 
            "SalesSum2015", 
            "SalesSum2016", 
            "SalesSum2017", 
            "SalesSum2018" 
        FROM "summary_store_sales"
        """
        df = self.analysis.db_manager.query(query)
        print("Store Sales Data Loaded:\n", df.head())  # Debugging print
        return df

    def register_callbacks(self):
        @self.app.callback(
            [Output('sales-count-city', 'figure'),
             Output('sales-count-store-type', 'figure'),
             Output('sales-count-store-state', 'figure'),
             Output('sales-count-years', 'figure'),
             Output('sales-count-months', 'figure'),
             Output('family-sales-count-city', 'figure'),
             Output('family-sales-count-store-type', 'figure'),
             Output('family-sales-count-store-state', 'figure'),
             Output('family-sales-count-years', 'figure'),
             Output('family-sales-count-months', 'figure'),
             Output('highest-sales', 'figure'),
             Output('lowest-sales', 'figure'),
             Output('family-sales-sum-bar', 'figure'),
             Output('store-sales-sum-bar', 'figure')],  # Added new output for store sales bar chart
            Input('interval-component', 'n_intervals')
        )
        def update_graphs(n):
            sales_data = self.query_aggregate_sales_data()
            family_sales_data = self.query_family_sales_data()
            store_sales_data = self.query_store_sales_data()

            params_list = [
                ("Sales Count by City", sales_data['store_city'].isin(['Quito', 'Guayaquil']), 'store_city',
                 'sale_amount', None),
                ("Sales Count by Store Type", sales_data['store_type'].isin(['D', 'E']), 'store_type', 'sale_amount',
                 None),
                ("Sales Count by Store State", sales_data['store_state'].isin(['Pichincha', 'Guayas']), 'store_state',
                 'sale_amount', None),
                ("Sales Count by Year", sales_data['year'].isin([2013, 2014, 2017]), 'year', 'sale_amount', None),
                ("Sales Count by Month", (sales_data['year'] == 2013) & (sales_data['month'].isin([6, 7, 10, 11])) |
                 (sales_data['year'] == 2014) & (sales_data['month'].isin([7, 10, 12])), 'month', 'sale_amount', None),
                ("Family Type Sales Count by City", sales_data['store_city'].isin(['Quito', 'Guayaquil']), 'store_city',
                 'sale_amount', 'family_name'),
                ("Family Type Sales Count by Store Type", sales_data['store_type'].isin(['D', 'E']), 'store_type',
                 'sale_amount', 'family_name'),
                ("Family Type Sales Count by Store State", sales_data['store_state'].isin(['Pichincha', 'Guayas']),
                 'store_state', 'sale_amount', 'family_name'),
                ("Family Type Sales Count by Year", sales_data['year'].isin([2013, 2014, 2017]), 'year', 'sale_amount',
                 'family_name'),
                ("Family Type Sales Count by Month",
                 (sales_data['year'] == 2013) & (sales_data['month'].isin([6, 7, 10, 11])) |
                 (sales_data['year'] == 2014) & (sales_data['month'].isin([7, 10, 12])), 'month', 'sale_amount',
                 'family_name'),
                (
                "Highest Sales in 2014, 2015, 2016", sales_data['year'].isin([2014, 2015, 2016]), 'year', 'sale_amount',
                'family_name'),
                ("Lowest Sales in 2013, 2017", sales_data['year'].isin([2013, 2017]), 'year', 'sale_amount',
                 'family_name')
            ]

            with Pool() as pool:
                figures = pool.starmap(create_figure, [(params, sales_data) for params in params_list])

            bar_figure = create_bar_figure(family_sales_data)
            store_bar_figure = create_store_bar_figure(store_sales_data)  # New function for store sales bar chart
            figures.append(bar_figure)
            figures.append(store_bar_figure)  # Append new bar chart to figures list

            return figures

    def run(self):
        self.app.run_server(debug=True)


if __name__ == '__main__':
    db_params = {
        'username': os.getenv('LOCAL_USER'),
        'password': os.getenv('LOCAL_PASS'),
        'host': os.getenv('LOCAL_DB_HOST'),
        'database_name': os.getenv('LOCAL_DATABASE')
    }

    # Initialize DatabaseManager instance
    db_manager = DatabaseManager(
        username=db_params['username'],
        password=db_params['password'],
        host=db_params['host'],
        database_name=db_params['database_name']
    )

    # Check database connection
    if db_manager.test_connection():
        print("Database connection successful.")
        analysis = Analysis(db_manager)
        dashboard = Dashboard(analysis)
        dashboard.run()
    else:
        print("Database connection failed. Analysis process aborted.")
import os
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
from analysis import Analysis
from connectDb import DatabaseManager
from cachetools import cached, TTLCache
from multiprocessing import Pool
import plotly.express as px


def create_figure(params, sales_data):
    title, filter_condition, x, y, color = params
    filtered_data = sales_data[filter_condition]
    fig = px.histogram(filtered_data, x=x, y=y, color=color, title=title)
    return fig


def create_bar_figure(sales_data):
    # Melt the DataFrame to long format
    sales_data_long = sales_data.melt(id_vars=['family_name'],
                                      value_vars=['SalesSum2013', 'SalesSum2014', 'SalesSum2015',
                                                  'SalesSum2016', 'SalesSum2017', 'SalesSum2018'],
                                      var_name='Year',
                                      value_name='SalesSum')
    sales_data_long['Year'] = sales_data_long['Year'].str.replace('SalesSum', '')
    sales_data_long['Year'] = sales_data_long['Year'].astype(int)

    # Create the bar plot
    fig = px.bar(sales_data_long, x='Year', y='SalesSum', color='family_name',
                 title='Sales Sum by Family (2013-2018)', barmode='group',
                 category_orders={'Year': list(range(2013, 2019))},
                 log_y=True)  # Set log scale for y-axis

    # Update layout to improve readability
    fig.update_layout(
        xaxis_title='Year',
        yaxis_title='Sales Sum (Log Scale)',
        legend_title='Family Name'
    )

    return fig


def create_store_bar_figures(sales_data):
    figures = []
    for store_nbr in sales_data['store_nbr'].unique():
        store_data = sales_data[sales_data['store_nbr'] == store_nbr]
        # Melt the DataFrame to long format
        sales_data_long = store_data.melt(id_vars=['store_nbr'],
                                          value_vars=['SalesSum2013', 'SalesSum2014', 'SalesSum2015',
                                                      'SalesSum2016', 'SalesSum2017', 'SalesSum2018'],
                                          var_name='Year',
                                          value_name='SalesSum')
        sales_data_long['Year'] = sales_data_long['Year'].str.replace('SalesSum', '')
        sales_data_long['Year'] = sales_data_long['Year'].astype(int)

        # Create the bar plot
        fig = px.bar(sales_data_long, x='Year', y='SalesSum', color='store_nbr',
                     title=f'Sales Sum by Store Number {store_nbr} (2013-2018)', barmode='group',
                     category_orders={'Year': list(range(2013, 2019))},
                     log_y=True)  # Set log scale for y-axis

        # Update layout to improve readability
        fig.update_layout(
            xaxis_title='Year',
            yaxis_title='Sales Sum (Log Scale)',
            legend_title='Store Number'
        )

        figures.append(fig)

    return figures


class Dashboard:
    def __init__(self, analysis_instance):
        self.analysis = analysis_instance
        self.app = dash.Dash(__name__)
        self.app.layout = html.Div([
            html.H1('Analytics Dashboard'),
            dcc.Tabs([
                dcc.Tab(label='Sales Count', children=[
                    dcc.Graph(id='sales-count-city'),
                    dcc.Graph(id='sales-count-store-type'),
                    dcc.Graph(id='sales-count-store-state'),
                    dcc.Graph(id='sales-count-years'),
                    dcc.Graph(id='sales-count-months')
                ]),
                dcc.Tab(label='Family Type Sales Count', children=[
                    dcc.Graph(id='family-sales-count-city'),
                    dcc.Graph(id='family-sales-count-store-type'),
                    dcc.Graph(id='family-sales-count-store-state'),
                    dcc.Graph(id='family-sales-count-years'),
                    dcc.Graph(id='family-sales-count-months'),
                    dcc.Graph(id='family-sales-sum-bar')
                ]),
                dcc.Tab(label='Special Analysis', children=[
                    dcc.Graph(id='highest-sales'),
                    dcc.Graph(id='lowest-sales'),
                    dcc.Graph(id='store-sales-sum-bar')  # Added new tab for store sales bar chart
                ]),
            ]),
            dcc.Interval(
                id='interval-component',
                interval=60 * 1000,  # in milliseconds
                n_intervals=0
            )
        ])

        self.register_callbacks()

    @cached(cache=TTLCache(maxsize=10, ttl=300))
    def query_aggregate_sales_data(self):
        sales_data = self.analysis.query_aggregate_sales_data()
        print("Sales Data Loaded:\n", sales_data.head())  # Debugging print
        return sales_data

    @cached(cache=TTLCache(maxsize=10, ttl=300))
    def query_family_sales_data(self):
        query = """
        SELECT 
            "family_name", 
            "SalesSum2013", 
            "SalesSum2014", 
            "SalesSum2015", 
            "SalesSum2016", 
            "SalesSum2017", 
            "SalesSum2018" 
        FROM "summary_family_sales"
        """
        df = self.analysis.db_manager.query(query)
        print("Family Sales Data Loaded:\n", df.head())  # Debugging print
        return df

    @cached(cache=TTLCache(maxsize=10, ttl=300))
    def query_store_sales_data(self):
        query = """
        SELECT 
            "store_nbr", 
            "SalesSum2013", 
            "SalesSum2014", 
            "SalesSum2015", 
            "SalesSum2016", 
            "SalesSum2017", 
            "SalesSum2018" 
        FROM "summary_store_sales"
        """
        df = self.analysis.db_manager.query(query)
        print("Store Sales Data Loaded:\n", df.head())  # Debugging print
        return df

    def register_callbacks(self):
        @self.app.callback(
            [Output('sales-count-city', 'figure'),
             Output('sales-count-store-type', 'figure'),
             Output('sales-count-store-state', 'figure'),
             Output('sales-count-years', 'figure'),
             Output('sales-count-months', 'figure'),
             Output('family-sales-count-city', 'figure'),
             Output('family-sales-count-store-type', 'figure'),
             Output('family-sales-count-store-state', 'figure'),
             Output('family-sales-count-years', 'figure'),
             Output('family-sales-count-months', 'figure'),
             Output('highest-sales', 'figure'),
             Output('lowest-sales', 'figure'),
             Output('family-sales-sum-bar', 'figure'),
             Output('store-sales-sum-bar', 'figure')],  # Added new output for store sales bar chart
            Input('interval-component', 'n_intervals')
        )
        def update_graphs(n):
            sales_data = self.query_aggregate_sales_data()
            family_sales_data = self.query_family_sales_data()
            store_sales_data = self.query_store_sales_data()

            params_list = [
                ("Sales Count by City", sales_data['store_city'].isin(['Quito', 'Guayaquil']), 'store_city',
                 'sale_amount', None),
                ("Sales Count by Store Type", sales_data['store_type'].isin(['D', 'E']), 'store_type', 'sale_amount',
                 None),
                ("Sales Count by Store State", sales_data['store_state'].isin(['Pichincha', 'Guayas']), 'store_state',
                 'sale_amount', None),
                ("Sales Count by Year", sales_data['year'].isin([2013, 2014, 2017]), 'year', 'sale_amount', None),
                ("Sales Count by Month", (sales_data['year'] == 2013) & (sales_data['month'].isin([6, 7, 10, 11])) |
                 (sales_data['year'] == 2014) & (sales_data['month'].isin([7, 10, 12])), 'month', 'sale_amount', None),
                ("Family Type Sales Count by City", sales_data['store_city'].isin(['Quito', 'Guayaquil']), 'store_city',
                 'sale_amount', 'family_name'),
                ("Family Type Sales Count by Store Type", sales_data['store_type'].isin(['D', 'E']), 'store_type',
                 'sale_amount', 'family_name'),
                ("Family Type Sales Count by Store State", sales_data['store_state'].isin(['Pichincha', 'Guayas']),
                 'store_state', 'sale_amount', 'family_name'),
                ("Family Type Sales Count by Year", sales_data['year'].isin([2013, 2014, 2017]), 'year', 'sale_amount',
                 'family_name'),
                ("Family Type Sales Count by Month",
                 (sales_data['year'] == 2013) & (sales_data['month'].isin([6, 7, 10, 11])) |
                 (sales_data['year'] == 2014) & (sales_data['month'].isin([7, 10, 12])), 'month', 'sale_amount',
                 'family_name'),
                (
                "Highest Sales in 2014, 2015, 2016", sales_data['year'].isin([2014, 2015, 2016]), 'year', 'sale_amount',
                'family_name'),
                ("Lowest Sales in 2013, 2017", sales_data['year'].isin([2013, 2017]), 'year', 'sale_amount',
                 'family_name')
            ]

            with Pool() as pool:
                figures = pool.starmap(create_figure, [(params, sales_data) for params in params_list])

            bar_figure = create_bar_figure(family_sales_data)
            store_bar_figures = create_store_bar_figures(store_sales_data)  # Updated to create store bar figures

            figures.append(bar_figure)
            figures.extend(store_bar_figures)  # Extend figures with store bar charts

            return figures

    def run(self):
        self.app.run_server(debug=True)


if __name__ == '__main__':
    db_params = {
        'username': os.getenv('LOCAL_USER'),
        'password': os.getenv('LOCAL_PASS'),
        'host': os.getenv('LOCAL_DB_HOST'),
        'database_name': os.getenv('LOCAL_DATABASE')
    }

    # Initialize DatabaseManager instance
    db_manager = DatabaseManager(
        username=db_params['username'],
        password=db_params['password'],
        host=db_params['host'],
        database_name=db_params['database_name']
    )

    # Check database connection
    if db_manager.test_connection():
        print("Database connection successful.")
        analysis = Analysis(db_manager)
        dashboard = Dashboard(analysis)
        dashboard.run()
    else:
        print("Database connection failed. Analysis process aborted.")
