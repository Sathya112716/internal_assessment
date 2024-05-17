from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("pySpark Q2") .getOrCreate()
data = [ ("John", "Apple", 10),
    ("Alice", "Apple", 20),
    ("John", "Banana", 12),
    ("Alice", "Banana", 14),
    ("Mike", "Apple", 15),
    ("Mike", "Banana", 17)]

schema = ["salesperson", "product", "quantity"]
df = spark.createDataFrame(data, schema=schema)
pivot_df = df.groupBy("product").pivot("salesperson").sum("quantity").sort("product")
pivot_df.show()
spark.stop()