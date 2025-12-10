from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Example1") \
    .getOrCreate()

sc = spark.sparkContext

numb = range(5,10)
spark_data = sc.parallelize(numb)

print("Collected Data: ", spark_data.collect())

spark.stop()
