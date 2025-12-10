from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Example3") \
    .getOrCreate()

sc = spark.sparkContext

list_data = [10, 21, 32 , 43, 54
             , 65, 76, 87, 98, 109 ]

filtered_data = list(filter(lambda x: x % 10 == 0, list_data))

filtered_data2 = sc.parallelize(filtered_data)

print("Filtered Data: ", filtered_data2.collect())

spark.stop()
