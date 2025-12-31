from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Example7").getOrCreate()

data = [
    ("HR", "3000"),
    ("IT", "4000"),
    ("Finance", "3500"),
    ("Marketing", "3200"),
    ("IT", "4500"),
]
columns = ["department", "salary"]

df = spark.createDataFrame(data, columns)
df = df.withColumn("salary", df["salary"].cast("int"))

rdd_aggregated = df.rdd.map(lambda row: (row["department"], row["salary"]))
rdd_aggregated = rdd_aggregated.reduceByKey(lambda x, y: x + y)

print(rdd_aggregated.collect())

spark.stop()
