from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Example2").getOrCreate()

data = [
    ("Alice", "HR", 3000),
    ("Bob", "IT", 4000),
    ("Charlie", "IT", 3500),
    ("David", "HR", 3200),
]
columns = ["name", "department", "salary"]
df = spark.createDataFrame(data, columns)

df.createOrReplaceTempView("people")

query = """SELECT name from people"""

result = spark.sql(query)
result.show(2)

spark.stop()
