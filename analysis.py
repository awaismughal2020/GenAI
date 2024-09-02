import os
import pandas as pd
from sqlalchemy import select, func, Table, MetaData, insert, create_engine, union_all, Column, Integer, Date, Boolean, String, Float, UniqueConstraint, case
from sqlalchemy.sql import and_
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from dotenv import load_dotenv
from connectDb import DatabaseManager
from transformers import pipeline

from sklearn.neural_network import MLPRegressor
from sklearn.metrics import mean_squared_error
from sqlalchemy.dialects.postgresql import insert as pg_insert
from models import Model


load_dotenv()


class Analysis:
    def __init__(self, db_manager):
        self.db_manager = db_manager
        self.model = Model(db_manager.engine)
        self.connection = self.db_manager.engine.connect()
        self.metadata = self.db_manager.metadata

        # Load aggregate_sales table
        self.aggregate_sales = Table(
            'aggregate_sales', self.metadata,
            autoload=True, autoload_with=self.db_manager.engine
        )
        self.summary_sales = pd.DataFrame()


    def query_aggregate_sales_data(self):
        subqueries = []
        for year in [2013, 2014, 2015, 2016, 2017]:
            # Subquery for the highest 12,000 values (descending order)
            subquery_desc = select(self.aggregate_sales).where(
                (self.aggregate_sales.c.sale_amount != 0.0) &
                (self.aggregate_sales.c.year == year)
            ).order_by(self.aggregate_sales.c.sale_amount.desc()).limit(12000)

            # Subquery for the lowest 8,000 values (ascending order)
            subquery_asc = select(self.aggregate_sales).where(
                (self.aggregate_sales.c.sale_amount != 0.0) &
                (self.aggregate_sales.c.year == year)
            ).order_by(self.aggregate_sales.c.sale_amount.asc()).limit(8000)

            # Append both subqueries to the list
            subqueries.append(subquery_desc)
            subqueries.append(subquery_asc)

        # Combine subqueries with union_all
        query = union_all(*subqueries)

        # Execute the query
        result = self.connection.execute(query)

        # Fetch the results into a DataFrame
        sales_data = pd.DataFrame(result.fetchall(), columns=result.keys())
        return sales_data

    def query_sales_by_store_and_year(self):
        query = select(
            self.aggregate_sales.c.store_nbr,
            func.sum(
                case(
                    (self.aggregate_sales.c.year == 2013, self.aggregate_sales.c.sale_amount),
                    else_=0
                )
            ).label('SalesSum2013'),
            func.sum(
                case(
                    (self.aggregate_sales.c.year == 2014, self.aggregate_sales.c.sale_amount),
                    else_=0
                )
            ).label('SalesSum2014'),
            func.sum(
                case(
                    (self.aggregate_sales.c.year == 2015, self.aggregate_sales.c.sale_amount),
                    else_=0
                )
            ).label('SalesSum2015'),
            func.sum(
                case(
                    (self.aggregate_sales.c.year == 2016, self.aggregate_sales.c.sale_amount),
                    else_=0
                )
            ).label('SalesSum2016'),
            func.sum(
                case(
                    (self.aggregate_sales.c.year == 2017, self.aggregate_sales.c.sale_amount),
                    else_=0
                )
            ).label('SalesSum2017')
        ).group_by(self.aggregate_sales.c.store_nbr)

        # Execute the query
        result = self.connection.execute(query)

        # Fetch the results into a DataFrame
        sales_data = pd.DataFrame(result.fetchall(), columns=result.keys())

        return sales_data

    def predict_year_2018_sales_data_old(self):
        data = self.query_aggregate_sales_data()

        # Proceed if data is not empty
        if data.empty:
            print("No sales data available for prediction.")
            return

        aggregate_data = data.groupby(['family_id', 'family_name']).agg(
            SalesSum2013=pd.NamedAgg(column='sale_amount', aggfunc=lambda x: x[data['year'] == 2013].sum()),
            SalesSum2014=pd.NamedAgg(column='sale_amount', aggfunc=lambda x: x[data['year'] == 2014].sum()),
            SalesSum2015=pd.NamedAgg(column='sale_amount', aggfunc=lambda x: x[data['year'] == 2015].sum()),
            SalesSum2016=pd.NamedAgg(column='sale_amount', aggfunc=lambda x: x[data['year'] == 2016].sum()),
            SalesSum2017=pd.NamedAgg(column='sale_amount', aggfunc=lambda x: x[data['year'] == 2017].sum())
        ).reset_index()

        # Prepare the features and target for prediction
        X = aggregate_data[['SalesSum2013', 'SalesSum2014', 'SalesSum2015', 'SalesSum2016', 'SalesSum2017']].values

        # Check if there are enough samples to split
        if len(X) == 0:
            print("Insufficient data for prediction.")
            return

        # Normalize the features
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)

        # Prepare the Linear Regression model
        model = LinearRegression()

        # Fit the model
        model.fit(X_scaled, X)

        # Predict the 2018 sales
        X_pred_scaled = scaler.transform(X)  # Use the same features for prediction
        y_pred_scaled = model.predict(X_pred_scaled)

        # Inverse transform the predictions
        y_pred = scaler.inverse_transform(y_pred_scaled)

        # Save predictions to a CSV file
        predictions_df = pd.DataFrame({
            'family_id': aggregate_data['family_id'],
            'family_name': aggregate_data['family_name'],
            'SalesSum2018': y_pred[:, 0]
        })

        predictions_df.to_csv('predictions_2018.csv', index=False)
        print("Predictions saved to 'predictions_2018.csv'")

    def predict_year_2018_sales_data(self):
        data = self.query_aggregate_sales_data()

        # Proceed if data is not empty
        if data.empty:
            print("No sales data available for prediction.")
            return

        aggregate_data = data.groupby(['family_id', 'family_name']).agg(
            SalesSum2013=pd.NamedAgg(column='sale_amount', aggfunc=lambda x: x[data['year'] == 2013].sum()),
            SalesSum2014=pd.NamedAgg(column='sale_amount', aggfunc=lambda x: x[data['year'] == 2014].sum()),
            SalesSum2015=pd.NamedAgg(column='sale_amount', aggfunc=lambda x: x[data['year'] == 2015].sum()),
            SalesSum2016=pd.NamedAgg(column='sale_amount', aggfunc=lambda x: x[data['year'] == 2016].sum()),
            SalesSum2017=pd.NamedAgg(column='sale_amount', aggfunc=lambda x: x[data['year'] == 2017].sum()),
            SalesSum2018=pd.NamedAgg(column='sale_amount', aggfunc=lambda x: x[data['year'] == 2018].sum())
        ).reset_index()

        # Prepare the features and target for prediction
        X = aggregate_data[['SalesSum2013', 'SalesSum2014', 'SalesSum2015', 'SalesSum2016', 'SalesSum2017']].values
        y = aggregate_data['SalesSum2018'].values  # Target variable

        # Check if there are enough samples to split
        if len(X) == 0:
            print("Insufficient data for prediction.")
            return

        # Normalize the features
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)

        # Split data into train and test sets
        X_train, X_test, y_train, y_test = train_test_split(X_scaled, y, test_size=0.2, random_state=42)

        # Initialize the MLP Regressor (Neural Network)
        model = MLPRegressor(hidden_layer_sizes=(100, 50), max_iter=500, random_state=42)

        # Fit the model
        model.fit(X_train, y_train)

        # Predict on test set
        y_pred = model.predict(X_test)

        # Evaluate the model
        mse = mean_squared_error(y_test, y_pred)
        print(f"Mean Squared Error: {mse}")

        # Predict the 2018 sales
        X_pred_scaled = scaler.transform(X)  # Use the same features for prediction
        y_pred_scaled = model.predict(X_pred_scaled)

        # Save predictions to a CSV file
        predictions_df = pd.DataFrame({
            'family_id': aggregate_data['family_id'],
            'family_name': aggregate_data['family_name'],
            'SalesSum2018_predicted': y_pred_scaled
        })

        predictions_df.to_csv('predictions_2018_nn.csv', index=False)
        print("Predictions saved to 'predictions_2018_nn.csv'")

        return predictions_df

    def predict_sales_2018_by_store(self):
        data = self.query_sales_by_store_and_year()

        if data.empty:
            print("No sales data available for prediction.")
            return

        X = data[['SalesSum2013', 'SalesSum2014', 'SalesSum2015', 'SalesSum2016', 'SalesSum2017']].values

        if len(X) == 0:
            print("Insufficient data for prediction.")
            return

        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)

        X_train, X_test, y_train, y_test = train_test_split(X_scaled, X_scaled[:, -1], test_size=0.2, random_state=42)

        model = LinearRegression()
        model.fit(X_train, y_train)  # Fit the model using the last year sales as target

        y_pred_scaled = model.predict(X_scaled)

        # Enforce non-negativity on the predictions
        y_pred_scaled = [max(0, y) for y in y_pred_scaled]

        # Post-process to ensure reasonable predictions
        y_pred_adjusted = []
        for i, pred in enumerate(y_pred_scaled):
            historical_max = max(X[i])
            historical_min = min(X[i])
            # Ensure the prediction is within a reasonable range of the historical data
            adjusted_pred = max(historical_min, min(pred, historical_max * 1.5))
            y_pred_adjusted.append(adjusted_pred)

        predictions_df = pd.DataFrame({
            'store_nbr': data['store_nbr'],
            'SalesSum2013': data['SalesSum2013'],
            'SalesSum2014': data['SalesSum2014'],
            'SalesSum2015': data['SalesSum2015'],
            'SalesSum2016': data['SalesSum2016'],
            'SalesSum2017': data['SalesSum2017'],
            'SalesSum2018': y_pred_adjusted
        })

        predictions_df.to_csv('predictions_2018_by_store.csv', index=False)
        print("Predictions saved to 'predictions_2018_by_store.csv'")

        for _, row in predictions_df.iterrows():
            self.connection.commit()
            transaction = self.connection.begin()
            insert_stmt = insert(self.model.summary_store_sales).values(
                store_nbr=row['store_nbr'],
                SalesSum2013=row['SalesSum2013'],
                SalesSum2014=row['SalesSum2014'],
                SalesSum2015=row['SalesSum2015'],
                SalesSum2016=row['SalesSum2016'],
                SalesSum2017=row['SalesSum2017'],
                SalesSum2018=row['SalesSum2018']
            )
            self.connection.execute(insert_stmt)
            transaction.commit()

        return predictions_df

    def get_sales_summary_with_predictions(self):
        # Query data from database
        query = select(
            self.aggregate_sales.c.family_id,
            self.aggregate_sales.c.family_name,
            func.sum(case((self.aggregate_sales.c.year == 2013, self.aggregate_sales.c.sale_amount), else_=0)).label(
                'SalesSum2013'),
            func.sum(case((self.aggregate_sales.c.year == 2014, self.aggregate_sales.c.sale_amount), else_=0)).label(
                'SalesSum2014'),
            func.sum(case((self.aggregate_sales.c.year == 2015, self.aggregate_sales.c.sale_amount), else_=0)).label(
                'SalesSum2015'),
            func.sum(case((self.aggregate_sales.c.year == 2016, self.aggregate_sales.c.sale_amount), else_=0)).label(
                'SalesSum2016'),
            func.sum(case((self.aggregate_sales.c.year == 2017, self.aggregate_sales.c.sale_amount), else_=0)).label(
                'SalesSum2017')
        ).group_by(self.aggregate_sales.c.family_id, self.aggregate_sales.c.family_name)

        # Execute the query
        result = self.connection.execute(query)
        db_data = pd.DataFrame(result.fetchall(), columns=result.keys())

        # Read predictions from CSV
        predictions_df = pd.read_csv('predictions_2018.csv')

        # Merge data from database with predictions on 'family_id' and 'family_name'
        summary_with_predictions = db_data.merge(predictions_df, on=['family_id', 'family_name'], how='left')
        self.summary_sales = summary_with_predictions


        if not self.summary_sales.empty:
            self.connection.commit()
            transaction = self.connection.begin()
            self.summary_sales.fillna(0, inplace=True)
            data_to_insert = self.summary_sales.to_dict(orient='records')
            insert_stmt = pg_insert(self.model.summary_family_sales).values(data_to_insert)
            on_conflict_stmt = insert_stmt.on_conflict_do_nothing()
            self.connection.execute(on_conflict_stmt)
            transaction.commit()

            print("\nData Successfully stored into Sales Summary\n")

        summary_with_predictions.drop(columns=['family_id'], inplace=True)

        return summary_with_predictions

    def generate_ai_based_analysis(self):
        data = self.query_aggregate_sales_data()

        # Proceed if data is not empty
        if data.empty:
            print("No sales data available for analysis.")
            return

        # Initialize Hugging Face pipeline for text generation
        summarizer = pipeline('summarization', model='facebook/bart-large-cnn')

        # Summarize data insights
        insights = []
        for year in data['year'].unique():
            yearly_data = data[data['year'] == year]
            summary = yearly_data.describe().to_string()
            insight = summarizer(summary, max_length=100, min_length=30, do_sample=False)
            insights.append(f"Year {year} Analysis: {insight[0]['summary_text']}")

        # Print and return insights
        for insight in insights:
            print(insight)
        return insights


if __name__ == "__main__":
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

    # Create Analysis instance and predict 2018 sales data
    analysis = Analysis(db_manager)
    analysis.generate_ai_based_analysis()
    # analysis.predict_year_2018_sales_data()
    # print()

    # Get sales summary with predictions
    # summary_with_predictions = analysis.get_sales_summary_with_predictions()
    # print(summary_with_predictions)
