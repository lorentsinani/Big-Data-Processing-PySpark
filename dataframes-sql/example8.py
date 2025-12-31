from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Example8").getOrCreate()

data = [
    ("Software Engineer", 70000, "NY", "S"),
    ("Manager", 80000, "CA", "L"),
    ("Analyst", 60000, "TX", "S"),
    ("Data Engineer", 75000, "WA", "L"),
    ("Intern", 30000, "FL", "S"),
    ("Consultant", 90000, "IL", "L"),
    ("Director", 120000, "MA", "L"),
    ("VP", 150000, "VA", "L"),
    ("CEO", 250000, "DC", "L"),
    ("Software Engineer", 70000, "CA", "S"),
    ("Analyst", 60000, "CA", "S"),
    ("Data Engineer", 75000, "CA", "L"),
    ("Intern", 30000, "CA", "S"),
    ("Consultant", 90000, "CA", "L"),
    ("Director", 120000, "CA", "L"),
    ("VP", 150000, "CA", "L"),
    ("CEO", 250000, "CA", "L"),
]
columns = ["job_title", "salary_in_usd", "company_location", "company_size"]
salaries_df = spark.createDataFrame(data, columns)

large_companies = (
    salaries_df.filter(salaries_df.company_size == "L")
    .filter(salaries_df.company_location == "CA")
    .groupBy()
    .avg("salary_in_usd")
    .show()
)

large_companies_total = salaries_df.filter(
    (salaries_df.company_size == "L") & (salaries_df.company_location == "CA")
)
large_companies_total.groupBy().sum("salary_in_usd").show()

spark.stop()
