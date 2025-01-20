from utils import load_data, sanitise_data, find_fraudulent_transactions, pd

prefix_vendor = {
    'maestro': ['5018', '5020', '5038', '56##'],
    'mastercard': ['51', '52', '54', '55', '222%'],
    'visa': ['4'],
    'amex': ['34', '37'],
    'discover': ['6011', '65'],
    'diners': ['300', '301', '304', '305', '36', '38'],
    'jcb16': ['35'],
    'jcb15': ['2131', '1800']
}

if __name__ == "__main__":
    fraud_data = load_data('Data/fraud.csv')

    sanitised_001 = sanitise_data('Data/transaction-001.csv', prefix_vendor)
    sanitised_002 = sanitise_data('Data/transaction-002.csv', prefix_vendor)
    sanitised_transactions = pd.concat([sanitised_001, sanitised_002], ignore_index=True)

    total_fraudulent = find_fraudulent_transactions('Data/fraud.csv', sanitised_transactions)
    print(f"Total fraudulent transactions: {total_fraudulent}")
