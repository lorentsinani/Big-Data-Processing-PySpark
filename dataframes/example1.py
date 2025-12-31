from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Example1").getOrCreate()

data = [("Enginner", 35000), ("Director", 36000), ("Specialist", 30000)]
columns = ["position", "salary"]
df = spark.createDataFrame(data, columns)

df.createOrReplaceTempView("data_view")
result_df = spark.sql(
    """
                      SELECT position, SUM(salary) AS total_salary 
                      FROM data_view 
                      GROUP BY position 
                      ORDER BY total_salary DESC
                      """
)

result_df.show()

spark.stop()
