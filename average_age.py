from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

spark = SparkSession.builder.appName("Dell Academy AI").getOrCreate()
df = spark.read.parquet("gs://dell-4/spark_databricks/*.snappy.parquet", header=True, inferSchema=True)

average_age = df.agg(avg(col("age")).alias("average_age"))

average_age.show()

spark.stop()