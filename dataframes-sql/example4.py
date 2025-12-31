from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Example4").getOrCreate()

data = [
    ("Software Engineer", 70000, "NY"),
    ("Manager", 80000, "CA"),
    ("Analyst", 60000, "TX"),
    ("Data Engineer", 75000, "WA"),
    ("Intern", 30000, "FL"),
    ("Consultant", 90000, "IL"),
    ("Director", 120000, "MA"),
    ("VP", 150000, "VA"),
    ("CEO", 250000, "DC"),
    ("Software Engineer", 70000, "CA"),
    ("Analyst", 60000, "CA"),
    ("Data Engineer", 75000, "CA"),
    ("Intern", 30000, "CA"),
    ("Consultant", 90000, "CA"),
    ("Director", 120000, "CA"),
    ("VP", 150000, "CA"),
    ("CEO", 250000, "CA"),
]
columns = ["job_title", "salary_in_usd", "company_location"]
df = spark.createDataFrame(data, columns)

df.createOrReplaceTempView("salaries_table")

query = """SELECT job_title, salary_in_usd FROM salaries_table WHERE company_location = 'CA'"""

result_df = spark.sql(query)
result_df.describe().show()

spark.stop()
