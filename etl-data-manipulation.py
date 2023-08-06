import pandas as pd
import sqlite3
import logging

# Configure logging
logging.basicConfig(filename='etl_log.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_data(source_file):
    try:
        logging.info("Fetching data from source...")
        data = pd.read_csv(source_file)
        return data
    except Exception as e:
        logging.error(f"Error fetching data: {e}")

def transform_data(data):
    try:
        logging.info("Transforming data...")
        data['Total'] = data['Quantity'] * data['Price']  # Adding 'Total' column
        data['Date'] = pd.to_datetime(data['Date'])  # Convert 'Date' to datetime
        return data
    except Exception as e:
        logging.error(f"Error transforming data: {e}")

def load_to_csv(data, destination_file_csv):
    try:
        logging.info("Loading transformed data to CSV...")
        data.to_csv(destination_file_csv, index=False)
    except Exception as e:
        logging.error(f"Error loading to CSV: {e}")

def load_to_database(data, database_file):
    try:
        logging.info("Loading transformed data to database...")
        conn = sqlite3.connect(database_file)
        data.to_sql('sales', conn, if_exists='replace', index=False)
        conn.close()
    except Exception as e:
        logging.error(f"Error loading to database: {e}")

if __name__ == "__main__":
    source_file = 'input_data.csv'
    destination_file_csv = 'output_data.csv'
    database_file = 'sales_database.db'

    data = fetch_data(source_file)
    if data is not None:
        transformed_data = transform_data(data)
        if transformed_data is not None:
            load_to_csv(transformed_data, destination_file_csv)
            load_to_database(transformed_data, database_file)

    logging.info("ETL process completed.")
