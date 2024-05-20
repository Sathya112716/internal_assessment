from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_add, to_date
spark = SparkSession.builder.appName("WeatherAnalysis").getOrCreate()
data = [
    (1, '2015-01-01', 10),
    (2, '2015-01-02', 25),
    (3, '2015-01-03', 20),
    (4, '2015-01-04', 30)
]

columns = ["id", "recordDate", "temperature"]
weather_df = spark.createDataFrame(data, columns)

weather_df = weather_df.withColumn("recordDate", to_date(col("recordDate"), "yyyy-MM-dd"))

joined_df = weather_df.alias("w1").join(
    weather_df.alias("w2"),
    col("w1.recordDate") == date_add(col("w2.recordDate"), 1)
)

result_df = joined_df.filter(col("w1.temperature") > col("w2.temperature"))
result_df = result_df.select(col("w1.id"))
result_df.show()