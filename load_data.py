import pandas as pd
from connect import connect_to_db
from psycopg2 import sql

def load_data():
    try:
        conn = connect_to_db()
        if conn is None:
            raise ConnectionError("Failed to connect to the database.")
        
        query = "SELECT ME_CODE, ME_PERIOD, ME_VAL FROM mev_data;"
        df = pd.read_sql(query, conn)
        
        print(f"Loaded data: {df.head()}") 
        
        conn.close()
        
        if df.empty:
            raise ValueError("No data found in the 'mev_data' table.")
        
        return df
    except Exception as e:
        print(f"Error loading data: {str(e)}")
        return None
