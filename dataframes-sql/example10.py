from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Example10").getOrCreate()
sc = spark.sparkContext

employees_data = [
    (1, "Alice", "Engineering", 90000, 5, "Berlin"),
    (2, "Bob", "Engineering", 80000, 3, "Berlin"),
    (3, "Charlie", "HR", 60000, 4, "Munich"),
    (4, "Diana", "HR", 65000, 6, "Munich"),
    (5, "Eve", "Finance", 70000, 7, "Hamburg"),
    (6, "Frank", "Finance", 72000, 4, "Berlin"),
    (7, "Grace", "Engineering", 95000, 8, "Hamburg"),
    (8, "Heidi", "Marketing", 55000, 2, "Berlin"),
    (9, "Ivan", "Marketing", 58000, 3, "Hamburg"),
    (10, "Judy", "Engineering", 99000, 10, "Munich"),
]

employees_columns = ["id", "name", "dept", "salary", "years_exp", "city"]

employees_df = spark.createDataFrame(employees_data, employees_columns)
employees_df.show()
employees_df.printSchema()

employees_df.createOrReplaceTempView("employees")
