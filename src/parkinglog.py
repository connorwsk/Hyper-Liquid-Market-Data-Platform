def main():
    data = connect_to_cmc()
    sb_connection = connect_to_supabase()
    # supabase_query(sb_connection)
    df = cmc_json_to_dataframe(data)
    df.head()
if __name__ == "__main__":
    main()

    
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
        # print(data)
        return data
    except (ConnectionError, Timeout, TooManyRedirects) as e:
        print(e) 
