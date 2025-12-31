from pyspark.sql import SparkSession
import matplotlib.pyplot as plt

spark = SparkSession.builder.appName("Example5").getOrCreate()

data = [
    ("Alice", 34, "NY"),
    ("Bob", 45, "CA"),
    ("Cathy", 29, "TX"),
    ("David", 40, "WA"),
]
columns = ["name", "age", "state"]
names_df = spark.createDataFrame(data, columns)

print("The column names of names_df are: ", names_df.columns)

df_pandas = names_df.toPandas()

df_pandas.plot(
    kind="barh",
    x="name",
    y="age",
    title="Age of Individuals by Name",
    colormap="plasma",
)
plt.show()

spark.stop()
