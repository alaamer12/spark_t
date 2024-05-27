"""Huge Spark installation size 320 MB"""
from pyspark.sql import SparkSession

# Initialize a SparkSession
spark = SparkSession.builder \
    .appName("ExampleApp") \
    .getOrCreate()

# Create a DataFrame from a list of tuples
data = [("Alice", 34), ("Bob", 45), ("Catherine", 29)]
columns = ["Name", "Age"]

df = spark.createDataFrame(data, columns)
df.show()

# Select and filter data
df_filtered = df.filter(df.Age > 30)
df_filtered.show()

# Close the SparkSession
spark.stop()
