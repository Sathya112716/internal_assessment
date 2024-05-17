from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, col
spark = SparkSession.builder .appName("Pyspark Q1") .getOrCreate()

schema = ["id", "first_name", "middle_name", "last_name"]
data = [(1, 'John', None, 'Doe'),
    (2, 'Alice', 'Ann', 'Smith'),
    (3, 'Mike', None, 'Johnson')]

df = spark.createDataFrame(data, schema=schema)
df = df.withColumn("full_name",
    concat_ws(" ", col("first_name"), col("middle_name"), col("last_name")))
df.show()
spark.stop()