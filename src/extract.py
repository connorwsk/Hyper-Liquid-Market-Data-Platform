#Extract data from sources  
import os
from requests import Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json
import pandas as pd
from datetime import datetime, timezone
from dotenv import load_dotenv

def connect_to_cmc():
    load_dotenv()

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