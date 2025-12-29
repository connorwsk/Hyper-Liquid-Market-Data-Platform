
import os
import psycopg2
from dotenv import load_dotenv

def connect_to_supabase():
    load_dotenv()
    try:
        connection = psycopg2.connect(
            os.getenv("DATABASE_URL"),
            sslmode="require"
            )
        print("Connection successful!")

        return connection
    except Exception as e:
        print("connect_to_supabase error:", e)
        return None

def save_table_to_supabase(df, table_name: str):
    connection = connect_to_supabase()
    cursor = connection.cursor()

    columns = ', '.join(df.columns)
    placeholders = ', '.join(['%s'] * len(df.columns))
    insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

    data_tuples = [tuple(row) for row in df.values]
    cursor.executemany(insert_query, data_tuples)
    connection.commit()

    print(f"✓ Saved {len(df)} rows to {table_name}")
    
    cursor.close()
    connection.close()





