from pyspark.sql import SparkSession
from matplotlib import pyplot as plt

spark = SparkSession.builder.appName("Example9").getOrCreate()

filePath = "data/fifa.csv"
fifa_df = spark.read.csv(filePath, header=True, inferSchema=True)
fifa_df.printSchema()
fifa_df.show(10)
print("There are {} rows in the fifa_df dataframe.".format(fifa_df.count()))

fifa_df.createOrReplaceTempView("fifa_table")
query = """SELECT Age FROM fifa_table WHERE Nationality = 'Germany'"""
fifa_df_germany_age = spark.sql(query)
fifa_df_germany_age.show()

fifa_df_germany_age_pandas = fifa_df_germany_age.toPandas()
fifa_df_germany_age_pandas.plot(
    kind="density", title="Age of German Players in FIFA Dataset"
)
plt.show()

spark.stop()
