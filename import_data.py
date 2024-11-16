import pandas as pd
from connect import connect_to_db
from psycopg2 import sql

def import_data_to_postgresql(df):
    try:
        conn = connect_to_db()
        if conn is None:
            return {"error": "Failed to connect to the database."}
        
        cur = conn.cursor()

        create_table_query = """
            CREATE TABLE IF NOT EXISTS mev_data (
                ME_CODE VARCHAR(50),
                ME_PERIOD DATE,
                ME_VAL FLOAT
            );
        """
        cur.execute(create_table_query)

        for _, row in df.iterrows():
            insert_query = sql.SQL("""
                INSERT INTO mev_data (ME_CODE, ME_PERIOD, ME_VAL)
                VALUES (%s, %s, %s);
            """)
            cur.execute(insert_query, (row['ME_CODE'], row['ME_PERIOD'], row['ME_VAL']))

        conn.commit() 
        cur.close()
        conn.close()

        return {"message": "Data successfully imported into PostgreSQL."}
    except Exception as e:
        print(f"Error in import_data_to_postgresql: {str(e)}")
        return {"error": str(e)}