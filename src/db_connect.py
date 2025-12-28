# This code connects to supabase database using credentials from .env file
import psycopg2
from dotenv import load_dotenv
import os
from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json
import pandas as pd
from datetime import datetime, timezone

load_dotenv()

def cmc_json_to_dataframe(cmc_json: dict) -> pd.DataFrame:
    """
    Convert CoinMarketCap listings/latest JSON into a clean DataFrame.

    Returns one row per coin with commonly used fields.
    """

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


# Connect to Supabase
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

# Connect to CoinMarketCap, puill data
def connect_to_cmc():
    url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
    parameters = {
    'start':'1',
    'limit':'5000',
    'convert':'USD'
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
        return data
    except (ConnectionError, Timeout, TooManyRedirects) as e:
        print(e) 

#Query Supabase using existing connection form connect_to_supabase
def supabase_query(connection):
        # Create a cursor to execute SQL queries
        cursor = connection.cursor()
        
        # Example query
        cursor.execute("SELECT NOW();")
        result = cursor.fetchone()
        print("Current Time:", result)

        # Close the cursor and connection
        cursor.close()
        connection.close()
        print("Connection closed.")


def main():
    data = connect_to_cmc()
    # sb_connection = connect_to_supabase()
    # supabase_query(sb_connection)
    print("connected to cmc")
    df = cmc_json_to_dataframe(data)
    print(df.head())

if __name__ == "__main__":
    main()

