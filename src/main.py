from extract import connect_to_cmc, cmc_json_to_dataframe
from transform import clean_crypto_data
from load import save_table_to_supabase

def main():
    #Pull data from MCM and convert to dataframe
    raw_data = connect_to_cmc()
    print("Connected and pulled data")
    raw_df = cmc_json_to_dataframe(raw_data)
    print("Converted raw data to dataframe")
    cleaned_df = clean_crypto_data(raw_df)
    print("Cleaned data")
    #Save raw data to supabase
    save_table_to_supabase(raw_df, "crypto_raw_data")
    save_table_to_supabase(cleaned_df, "crypto_cleaned_data")

if __name__ == "__main__":
    main()

