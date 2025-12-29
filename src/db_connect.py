# Pulls data from coinmarketcap API and saves to Supabase. Four functions:
# 1: connect_to_cmc() - connects to CMC and pulls data
# 2: cmc_json_to_dataframe() - converts CMC json data to pandas dataframe
# 3: connect_to_supabase() - connects to Supabase database
# 4: save_raw_data_to_supabase() - saves raw data to Supabase database

import psycopg2
from dotenv import load_dotenv
import os
from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json
import pandas as pd
from datetime import datetime, timezone

load_dotenv()

def connect_to_cmc():
    url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
    parameters = {
    'start':'1',
    'limit':'5000',
    'convert':'USD',
    }
    headers = {
    'Accepts': 'application/json',
    'X-CMC_PRO_API_KEY': os.getenv("API_KEY"),
    }
    session = Session()
    session.headers.update(headers)

    try:
        response = session.get(url, params=parameters)
        data = json.loads(response.text)
        print("CMC data pulled successfully!")
        return data
    
    except (ConnectionError, Timeout, TooManyRedirects) as e:
        print(e) 


def cmc_json_to_dataframe(cmc_json: dict) -> pd.DataFrame:

    if "data" not in cmc_json:
        raise ValueError("Invalid CMC response: 'data' key not found")

    pulled_at = datetime.now(timezone.utc)

    rows = []
    for coin in cmc_json["data"]:
        usd = coin.get("quote", {}).get("USD", {})

        rows.append({
            "pulled_at": pulled_at,
            "id": coin.get("id"),
            "symbol": coin.get("symbol"),
            "name": coin.get("name"),
            "cmc_rank": coin.get("cmc_rank"),
            "price_usd": usd.get("price"),
            "market_cap_usd": usd.get("market_cap"),
            "volume_24h_usd": usd.get("volume_24h"),
            "percent_change_1h": usd.get("percent_change_1h"),
            "percent_change_24h": usd.get("percent_change_24h"),
            "percent_change_7d": usd.get("percent_change_7d"),
            "last_updated": coin.get("last_updated"),
        })
    df = pd.DataFrame(rows)
    return df

def connect_to_supabase():
    try:
        connection = psycopg2.connect(
            os.getenv("DATABASE_URL"),
            sslmode="require"
            )
        print("Connection successful!")

        return connection
    except Exception as e:
        print("Error connecting to database:", e)
        return None

def save_raw_data_to_supabase(connection, df: pd.DataFrame, table_name: str = "crypto_raw_data"):
    """
    Saves a pandas DataFrame to Supabase using psycopg2.
    
    Args:
        connection: psycopg2 connection object
        df: pandas DataFrame to save
        table_name: name of the table to insert data into
    """
    try:
        cursor = connection.cursor()
        
        # Get column names from DataFrame
        columns = ', '.join(df.columns)
        
        # Create placeholders for VALUES clause
        placeholders = ', '.join(['%s'] * len(df.columns))
        
        # Create INSERT query
        insert_query = f"""
            INSERT INTO {table_name} ({columns})
            VALUES ({placeholders})
        """
        
        # Convert DataFrame to list of tuples
        data_tuples = [tuple(row) for row in df.values]
        
        # Execute batch insert
        cursor.executemany(insert_query, data_tuples)
        
        # Commit the transaction
        connection.commit()
        
        print(f"Successfully inserted {len(df)} rows into {table_name}")
        
        # Close cursor
        cursor.close()
        
    except Exception as e:
        print(f"Error saving data to Supabase: {e}")
        connection.rollback()
        raise
    finally:
        # Close connection
        if connection:
            connection.close()
            print("Connection closed.")
            
def main():
    #Pull data from MCM and convert to dataframe
    raw_data = connect_to_cmc()
    raw_df = cmc_json_to_dataframe(raw_data)

    #Save raw data to supabase
    superbase_connection = connect_to_supabase()
    save_raw_data_to_supabase(superbase_connection,raw_df)

if __name__ == "__main__":
    main()

