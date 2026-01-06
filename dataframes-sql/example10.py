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

# --- Example 10: Department average salary (RDD -> DF -> SQL) ---

employees_rdd = employees_df.rdd

# RDD: (dept, (salary, 1))
dept_salary_rdd = employees_rdd.map(lambda row: (row.dept, (row.salary, 1)))

# Sum salary and count per dept
dept_sum_count_rdd = dept_salary_rdd.reduceByKey(
    lambda a, b: (a[0] + b[0], a[1] + b[1])
)

# Compute average salary per dept: (dept, avg_salary)
dept_avg_rdd = dept_sum_count_rdd.map(lambda kv: (kv[0], kv[1][0] / kv[1][1]))

# Convert to DataFrame
dept_avg_df = dept_avg_rdd.toDF(["dept", "avg_salary"])

print("Average salary per dept (from RDD):")
dept_avg_df.show()

dept_avg_df.createOrReplaceTempView("dept_avg")

top3_sql = spark.sql(
    """
    SELECT dept, avg_salary
    FROM dept_avg
    ORDER BY avg_salary DESC
    LIMIT 3
"""
)

print("Top 3 departments by average salary:")
top3_sql.show()


# # --- Example 11: Employees above dept average (DF + SQL + RDD) ---

# DataFrame: average salary per dept
dept_avg_df = (
    employees_df.groupBy("dept")
    .avg("salary")
    .alias("avg_salary")
    .withColumnRenamed("avg(salary)", "avg_salary")
)

print("Dept average salaries (DF):")
dept_avg_df.show()

# Join employees with dept averages
joined_df = employees_df.join(dept_avg_df, on="dept", how="inner")
joined_df.printSchema()

# Filter employees above their dept average
high_earners_df = joined_df.filter(joined_df.salary > joined_df.avg_salary)

print("Employees with salary above their dept average:")
high_earners_df.select("name", "dept", "salary", "avg_salary").show()

# Use SQL just for fun
joined_df.createOrReplaceTempView("employees_with_avg")

high_earners_sql = spark.sql(
    """
    SELECT name, dept, salary, avg_salary
    FROM employees_with_avg
    WHERE salary > avg_salary
    ORDER BY dept, salary DESC
"""
)

print("Same result via SQL:")
high_earners_sql.show()

# Convert high earners to RDD and count per dept
high_earners_rdd = high_earners_df.rdd

dept_high_count_rdd = high_earners_rdd.map(lambda row: (row.dept, 1)).reduceByKey(
    lambda x, y: x + y
)

print("Number of above-average earners per dept (RDD):")
for dept, cnt in dept_high_count_rdd.collect():
    print(dept, cnt)


# # --- Example 12: Seniority categories (RDD -> DF -> SQL) ---

# RDD: (name, dept, years_exp) -> add category
def categorize(row):
    if row.years_exp < 3:
        category = "junior"
    elif row.years_exp <= 6:
        category = "mid"
    else:
        category = "senior"
    return (row.name, row.dept, row.years_exp, category)

seniority_rdd = employees_df.rdd.map(categorize)

seniority_df = seniority_rdd.toDF(["name", "dept", "years_exp", "category"])

print("Employees with seniority category:")
seniority_df.show()

# Use SQL to count per dept and category
seniority_df.createOrReplaceTempView("seniority")

counts_sql = spark.sql("""
    SELECT dept, category, COUNT(*) AS num_employees
    FROM seniority
    GROUP BY dept, category
    ORDER BY dept, category
""")

print("Number of employees per dept and category (SQL):")
counts_sql.show()

# --- Example 13: Budget utilization (RDD -> DF -> SQL) ---

# 1. Hardcoded budgets in Python
budget_data = [
    ("Engineering", 400000),
    ("HR",          150000),
    ("Finance",     160000),
    ("Marketing",   120000),
]

# 2. RDD -> DataFrame
budget_rdd = sc.parallelize(budget_data)
budget_df = budget_rdd.toDF(["dept", "budget"])

print("Department budgets:")
budget_df.show()

# 3. Join employees with budgets
emp_with_budget_df = employees_df.join(budget_df, on="dept", how="inner")

emp_with_budget_df.createOrReplaceTempView("emp_budget")

# 4. SQL: total salary and utilization per department
utilization_sql = spark.sql("""
    SELECT
        dept,
        SUM(salary) AS total_salary,
        MAX(budget) AS budget,   -- same budget per dept
        ROUND(SUM(salary) / MAX(budget), 2) AS utilization_ratio
    FROM emp_budget
    GROUP BY dept
    ORDER BY utilization_ratio DESC
""")

print("Budget utilization per dept:")
utilization_sql.show()

spark.stop()
