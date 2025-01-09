import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_csv

spark = SparkSession.builder.appName("CreditCardVendorMapping").getOrCreate()

df = spark.read.csv("C:/Users/ThinkPad/GitProjects/engineering-coding-challenge/Data/fraud.csv", header=True, inferSchema=True)
df2 = spark.read.csv("C:/Users/ThinkPad/GitProjects/engineering-coding-challenge/Data/transaction-001.csv", header=True, inferSchema=True)
df3 = spark.read.csv("C:/Users/ThinkPad/GitProjects/engineering-coding-challenge/Data/transaction-002.csv", header=True, inferSchema=True)

df.show()
df2.show()
df3.show()
