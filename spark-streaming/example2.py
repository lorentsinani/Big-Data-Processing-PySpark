from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

spark = SparkSession.builder.appName("TransformFilter").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = (spark.readStream
      .format("rate")
      .option("rowsPerSecond", 10)
      .load())

out = (df
       .withColumn("user_id", (col("value") % 5).cast("int"))
       .withColumn("event_type", expr("CASE WHEN user_id IN (0,1) THEN 'click' ELSE 'view' END"))
       .filter(col("event_type") == "click")
       )

q = (out.writeStream
     .format("console")
     .outputMode("append")
     .option("truncate", "false")
     .start())

q.awaitTermination()
