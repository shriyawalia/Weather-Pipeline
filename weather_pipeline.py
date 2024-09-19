
import logging
import os
from sqlalchemy import create_engine
import pandas as pd
import numpy as np  # For handling NaN values

# Set up logging configuration

#Save absolute path to script location in variable to pass to logging filename

script_dir = os.path.abspath( os.path.dirname( __file__ ) )

logging.basicConfig(filename= f"{script_dir}/weather_pipeline.log", level=logging.DEBUG,
                    format='%(asctime)s:%(levelname)s:%(message)s')

# Creating functions for each step:

# Creates a new database engine

def new_engine(dialect, server, database, user=None, password=None, integrated_security=True):

    try:
        eng = f"mssql://{server}/{database}?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server"
        logging.info("Database engine created successfully")
        return create_engine(eng)
    except Exception as e:
        logging.error(f"Error creating engine: {e}", exc_info=True)
        raise

# Establishes connection to the database

def connect_to_db(engine):
    
    try:
        connection = engine.connect()
        logging.info("Database connection established successfully")
        return connection
    except Exception as e:
        logging.error(f"Error connecting to database: {e}", exc_info=True)
        raise

# Loads data from the SQL database into a pandas DataFrame

def load_data(engine, query):

    try:
        data = pd.read_sql(query, con=engine)
        logging.info("Data loaded successfully from SQL")
        return data
    except Exception as e:
        logging.error(f"Error loading data: {e}", exc_info=True)
        raise

# Processes data by converting columns, handling NaN values and extracting year and month

def process_data(data):
    
    try:
        # Log initial data types
        initial_dtypes = data.dtypes
        logging.info(f"Initial data types:\n{initial_dtypes}")

        # Convert 'date' to datetime
        try:
            data['date'] = pd.to_datetime(data['date'])
            logging.info("'date' column converted to datetime")
        except Exception as e:
            logging.error(f"Failed to convert 'date' to datetime: {e}", exc_info=True)
            # Continue with invalid date entries set to NaT
            data['date'] = pd.to_datetime(data['date'], errors='coerce')

        # Convert numerical columns to float, filling invalid entries with NaN
        for column in ['precipitation', 'temp_max', 'temp_min', 'wind']:
            try:
                data[column] = data[column].astype(float)
                logging.info(f"'{column}' column converted to float")
            except Exception as e:
                logging.error(f"Failed to convert '{column}' to float: {e}", exc_info=True)
                # Set invalid entries to NaN and continue
                data[column] = pd.to_numeric(data[column], errors='coerce')

        # Extract year and month from 'date'
        data['year'] = data['date'].dt.year
        data['month'] = data['date'].dt.month
        logging.info("Year and month columns extracted")

        # Log data types after conversion
        final_dtypes = data.dtypes
        logging.info(f"Data types after conversion:\n{final_dtypes}")

        return data

    except Exception as e:
        logging.error(f"Error processing data: {e}", exc_info=True)
        raise
    
# Calculates monthly average temperature

def calculate_monthly_avg_temp(data):

    try:
        # Calculate average temperature per month
        monthly_avg_temp = data.groupby(['year', 'month'])[['temp_max', 'temp_min']].mean().reset_index()
        monthly_avg_temp.rename(columns={'temp_max': 'avg_temp_max', 'temp_min': 'avg_temp_min'}, inplace=True)

        # Round the results to 2 decimal places
        monthly_avg_temp[['avg_temp_max', 'avg_temp_min']] = monthly_avg_temp[['avg_temp_max', 'avg_temp_min']].round(2)
        logging.info("Monthly average temperatures calculated and rounded")

        return monthly_avg_temp
    except Exception as e:
        logging.error(f"Error calculating monthly averages: {e}", exc_info=True)
        raise

# Updates SQL with an edited weather table and a new table with average temperatures

def save_to_sql(data, engine, table_name):
    
    try:
        data.to_sql(table_name, con=engine, if_exists='replace', index=False)
        logging.info(f"Data saved to SQL table '{table_name}'")
    except Exception as e:
        logging.error(f"Error saving data to SQL: {e}", exc_info=True)
        raise

# Runs all the functions step by step 

def main():
    try:
        # Initialize the engine
        engine = new_engine('mssql', 'DESKTOP-3PDJ58N', 'Weather', integrated_security=True)

        # Connect to the database
        connection = connect_to_db(engine)

        # Query the data from the table
        query = "SELECT * FROM SeattleWeather"
        data = load_data(engine, query)

        # View the data
        print(data.head())
        logging.info(f"Data preview: {data.head()}")

        # Process data
        processed_data = process_data(data)

        # Calculate monthly average temperature
        monthly_avg_temp = calculate_monthly_avg_temp(processed_data)

        # Save the aggregated data to a new SQL table
        save_to_sql(monthly_avg_temp, engine, 'MonthlyAvgTemperature')

        # View the result
        print(monthly_avg_temp)

    except Exception as e:
        logging.error(f"An error occurred in the main pipeline: {e}", exc_info=True)
        print(f"Error: {e}")

if __name__ == "__main__":
    main()


