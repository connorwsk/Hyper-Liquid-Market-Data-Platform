"""Functions to transform and clean data"""
import pandas as pd

def clean_crypto_data(df):
    """Clean raw crypto data (Bronze → Silver)"""
    return (df
        .drop_duplicates(subset=['id'])
        .dropna(subset=['price_usd', 'symbol'])
        .assign(symbol=lambda x: x['symbol'].str.upper()))

def aggregate_daily_stats(df):
    """Create daily aggregated stats (Silver → Gold)"""
    return df.groupby(['symbol', df['pulled_at'].dt.date]).agg({
        'price_usd': ['min', 'max', 'mean'],
        'volume_24h_usd': 'sum',
        'market_cap_usd': 'mean'
    }).reset_index()