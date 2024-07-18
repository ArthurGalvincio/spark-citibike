from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

spark = SparkSession.builder.appName("Dell Academy AI").getOrCreate()
df = spark.read.parquet("gs://dell-4/spark_databricks/*.snappy.parquet", header=True, inferSchema=True)

df = df.withColumn("tripduration", col("tripduration").cast("int"))

avg_duration = df.agg(avg("tripduration").alias("average_duration")).collect()[0]["average_duration"]
print(f"Duração média das viagens: {avg_duration}")

slowest_trips = df.orderBy(col("tripduration"), ascending=False).limit(10)
slowest_trips.show()

spark.stop()