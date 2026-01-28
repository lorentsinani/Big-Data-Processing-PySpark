from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split

spark = SparkSession.builder.appName("HelloRate").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

lines = (spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load())

words = lines.select(explode(split(col("value"), r"\s+")).alias("word")).where(col("word") != "")

counts = words.groupBy("word").count()


q = (counts.writeStream
     .format("console")
     .outputMode("complete")
     .option("truncate", "false")
     .trigger(processingTime="5 seconds")
     .start())

q.awaitTermination()
