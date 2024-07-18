from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

spark = SparkSession.builder.appName("Dell Academy AI").getOrCreate()
df = spark.read.parquet("gs://dell-4/spark_databricks/*.snappy.parquet", header=True, inferSchema=True)

df = df.withColumn("tripduration", col("tripduration").cast("int"))
df = df.filter(col("tripduration") > 0)

avg_duration_per_pair = df.groupBy("start_station_id", "end_station_id").agg(avg("tripduration").alias("average_duration"))
avg_duration_per_pair = avg_duration_per_pair.orderBy(col("average_duration").desc()).limit(10)

avg_duration_per_pair.show()

spark.stop()