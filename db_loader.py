import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import time
import os

# Environment variables matching docker-compose
DB_USER = os.environ.get("POSTGRES_USER", "postgres")
DB_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "postgres")
DB_HOST = os.environ.get("POSTGRES_HOST", "db")
DB_NAME = os.environ.get("POSTGRES_DB", "sales_db")
DB_PORT = os.environ.get("POSTGRES_PORT", "5432")

def load_data():
    csv_file = 'sales_data_sample.csv'
    df = pd.read_csv(csv_file, encoding='unicode_escape')
    
    # Clean up column names for SQL compatibility
    df.columns = df.columns.str.lower().str.replace('[^a-z0-9_]', '', regex=True)
    df.rename(columns={'ordernumber': 'order_number', 'orderdate': 'order_date', 'productline': 'product_line', 'msrp': 'msrp', 'dealsize': 'deal_size'}, inplace=True)
    df['order_date'] = pd.to_datetime(df['order_date'])

    conn_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(conn_string)
    
    # Connection retry logic to wait for the database service
    max_retries = 10
    retry_count = 0
    while retry_count < max_retries:
        try:
            conn = psycopg2.connect(conn_string)
            conn.close()
            print("Successfully connected to the database! Loading data...")
            break
        except psycopg2.OperationalError:
            time.sleep(5)
            retry_count += 1
            if retry_count == max_retries:
                raise ConnectionError("Failed to connect to PostgreSQL.")

    # Load data into PostgreSQL
    try:
        df.to_sql('sales_data', engine, if_exists='replace', index=False)
        print("Data loaded successfully into the 'sales_data' table.")
    except Exception as e:
        print(f"Error loading data: {e}")

if __name__ == "__main__":
    load_data()