# Import libraries
import os
import json
from datetime import datetime, timezone
import requests
import psycopg2
from psycopg2.extras import Json
from dotenv import load_dotenv

load_dotenv()

HL_INFO_URL = "https://api.hyperliquid.xyz/info"

#Connect to supabase
def get_db_conn():
    return psycopg2.connect(
        host=os.environ["SUPABASE_DB_HOST"],
        port=int(os.environ.get("SUPABASE_DB_PORT", "5432")),
        dbname=os.environ.get("SUPABASE_DB_NAME", "postgres"),
        user=os.environ["SUPABASE_DB_USER"],
        password=os.environ["SUPABASE_DB_PASSWORD"],
        sslmode=os.environ.get("SUPABASE_DB_SSLMODE", "require"),
    )

def fetch_meta_and_asset_ctxs():
    # Hyperliquid /info supports multiple request body "type" values. :contentReference[oaicite:2]{index=2}
    body = {"type": "metaAndAssetCtxs"}
    r = requests.post(HL_INFO_URL, json=body, timeout=20)
    r.raise_for_status()
    return body["type"], r.json()


def insert_raw(request_type: str, payload: dict):
    pulled_at = datetime.now(timezone.utc)

    sql = """
        insert into raw.hyperliquid_meta_and_asset_ctxs_raw
            (pulled_at, request_type, payload)
        values
            (%s, %s, %s)
    """

    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (pulled_at, request_type, Json(payload)))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def main():
    req_type, payload = fetch_meta_and_asset_ctxs()
    insert_raw(req_type, payload)
    print(f"Inserted raw payload for {req_type} at {datetime.now(timezone.utc).isoformat()}")


if __name__ == "__main__":
    main()
