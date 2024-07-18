from pyspark.sql import SparkSession
from pyspark.sql.functions import count

spark = SparkSession.builder.appName("Dell Academy AI").getOrCreate()
df = spark.read.parquet("gs://dell-4/spark_databricks/*.snappy.parquet", header=True, inferSchema=True)

gender_distribution = df.groupBy("gender").agg(count("*").alias("count"))

gender_distribution.show()

spark.stop()