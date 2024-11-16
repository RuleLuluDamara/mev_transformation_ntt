import pandas as pd
from connect import connect_to_db
from psycopg2 import sql
from psycopg2.extras import execute_values

def transformasi_data(df):
    pivot_df = df.pivot_table(index='me_period', columns='me_code', values='me_val')
    pivot_df.reset_index(inplace=True)

    mev_transformed_lagged = pivot_df.copy()
    for column in pivot_df.columns:
        if column != 'me_period':
            for i in range(1, 13):
                mev_transformed_lagged[f'{column}_lag_{i}'] = mev_transformed_lagged[column].shift(i)
    
    mev_transformed_lagged.fillna(0, inplace=True)
    
    return mev_transformed_lagged


def save_transformed_data(transformed_df):
    try:
        conn = connect_to_db()
        if conn is None:
            return {"error": "Failed to connect to the database."}
        
        cur = conn.cursor()

        columns = transformed_df.columns
        column_definitions = []

        for column in columns:
            dtype = transformed_df[column].dtype
            if dtype == 'object':
                column_definitions.append(f'"{column}" VARCHAR')
            elif dtype == 'int64':
                column_definitions.append(f'"{column}" INTEGER')
            elif dtype == 'float64':
                column_definitions.append(f'"{column}" FLOAT')
            elif dtype == 'datetime64[ns]':
                column_definitions.append(f'"{column}" DATE')
            else:
                column_definitions.append(f'"{column}" VARCHAR')

        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS transformed_data (
                {', '.join(column_definitions)}
            );
        """
        cur.execute(create_table_query)

        insert_query = sql.SQL("""
            INSERT INTO transformed_data ({})
            VALUES %s;
        """).format(sql.SQL(', ').join(map(sql.Identifier, columns)))

        data_to_insert = [tuple(x) for x in transformed_df.values]

        execute_values(cur, insert_query, data_to_insert)

        conn.commit()

        cur.close()
        conn.close()

        return {"message": "Data successfully saved to PostgreSQL."}
    except Exception as e:
        print(f"Error saat menyimpan data: {str(e)}")
        return {"error": str(e)}
