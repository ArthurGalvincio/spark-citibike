from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

spark = SparkSession.builder.appName("Dell Academy AI").getOrCreate()
df = spark.read.parquet("gs://dell-4/spark_databricks/*.snappy.parquet", header=True, inferSchema=True)

total_trips_per_bike = df.groupBy("bikeid").agg(count("*").alias("total_trips"))
top_10_bikes = total_trips_per_bike.orderBy(col("total_trips").desc()).limit(10)

top_10_bikes.show()

spark.stop()