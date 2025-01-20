import pandas as pd

def load_data(file_path):
    return pd.read_csv(file_path)

def get_vendor(card_number, prefix_dict):
    for vendor, prefixes in prefix_dict.items():
        for prefix in prefixes:
            if prefix.endswith("#"):
                if card_number.startswith(prefix[:-2]):
                    return vendor
            elif prefix.endswith("%"):
                if card_number.startswith(prefix[:-1]):
                    return vendor
            elif card_number.startswith(prefix):
                return vendor
    return None

def sanitise_data(file_path, prefix_map):
    df = pd.read_csv(file_path)
    df['valid_vendor'] = df['credit_card_number'].astype(str).apply(
        lambda x: get_vendor(x, prefix_map)
    )
    print(df)
    return df[df['valid_vendor'].notnull()]

def find_fraudulent_transactions(fraud_file, transactions_df):
    fraud_df = pd.read_csv(fraud_file)
    merged_df = pd.merge(fraud_df, transactions_df, on='credit_card_number', how='inner')
    return merged_df.shape[0]