import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("CreditCardVendorMapping").getOrCreate()

fraud_df = spark.read.csv("C:/Users/ThinkPad/GitProjects/engineering-coding-challenge/Data/fraud.csv", header=True, inferSchema=True)
transaction_1_df = spark.read.csv("C:/Users/ThinkPad/GitProjects/engineering-coding-challenge/Data/transaction-001.csv", header=True, inferSchema=True)
transaction_2_df = spark.read.csv("C:/Users/ThinkPad/GitProjects/engineering-coding-challenge/Data/transaction-002.csv", header=True, inferSchema=True)
transactions_df = transaction_1_df.union(transaction_2_df)

card_prefixes = {
    'maestro': ['5018', '5020', '5038', '56##'],
    'mastercard': ['51', '52', '54', '55', '222%'],
    'visa': ['4'],
    'amex': ['34', '37'],
    'discover': ['6011', '65'],
    'diners': ['300', '301', '304', '305', '36', '38'],
    'jcb16': ['35'],
    'jcb15': ['2131', '1800']
}


def remove_invalid_cards(card_dataframe):
    card_pattern = r"^(5018|5020|5038|56\d{2}|51|52|54|55|222\d*|4|34|37|6011|65|300|301|304|305|36|38|35|2131|1800)"
    card_dataframe = card_dataframe.filter(col("credit_card_number").rlike(card_pattern))
    
    return card_dataframe

def find_fraudulent_transactions(card_dataframe, fraud_dataframe):
    fraudulent_df = fraud_dataframe.join(card_dataframe, on="credit_card_number", how="left_semi")

    return fraudulent_df

def main(fraud_df, transactions_df):
    valid_transactions_df = remove_invalid_cards(transactions_df)
    fraudulent_transactions_df = find_fraudulent_transactions(valid_transactions_df, fraud_df)
    print(fraudulent_transactions_df.count())


if __name__ == "__main__":
    main(fraud_df, transactions_df)
