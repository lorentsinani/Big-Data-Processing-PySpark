from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Example3").getOrCreate()

data = [
    ("Alice", "HR", 3000, "F"),
    ("Bob", "IT", 4000, "M"),
    ("Charlie", "IT", 3500, "M"),
    ("David", "HR", 3200, "F"),
]
columns = ["name", "department", "salary", "sex"]
df = spark.createDataFrame(data, columns)

df.createOrReplaceTempView("people")

female_query = """SELECT * from people WHERE sex == 'F'"""
male_query = """SELECT * from people WHERE sex == 'M'"""

female_df = spark.sql(female_query)
male_df = spark.sql(male_query)

print(
    f"There are {female_df.count()} rows in the female_df and {male_df.count()} rows in the male_df"
)

spark.stop()
