from pyspark.sql import SparkSession
from pyspark.sql.functions import col
spark = SparkSession.builder.appName("SalesProductJoin").getOrCreate()
sales_data = [
    (1, 100, 2008, 10, 5000),
    (2, 100, 2009, 12, 5000),
    (7, 200, 2011, 15, 9000)
]

product_data = [
    (100, 'Nokia'),
    (200, 'Apple'),
    (300, 'Samsung')
]
sales_columns = ["sale_id", "product_id", "year", "quantity", "price"]
product_columns = ["product_id", "product_name"]
sales_df = spark.createDataFrame(sales_data, sales_columns)
product_df = spark.createDataFrame(product_data, product_columns)
result_df = sales_df.join(product_df, on="product_id").select("product_name", "year", "price")

result_df.show()
