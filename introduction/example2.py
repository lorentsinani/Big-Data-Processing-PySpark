from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Example2") \
    .getOrCreate()

sc = spark.sparkContext

my_list = [1, 2 , 3, 4, 5, 6, 7, 8, 9, 10]

print("My List: ", my_list)

squared_list = list(map(lambda x: x ** 2, my_list))

# print or load with parallelize to collect afterwards

# print("Squared List: ", squared_list) # 1st option

squared_rdd = sc.parallelize(squared_list)  # 2nd option
print("Squared List: ", squared_rdd.collect())

spark.stop()
