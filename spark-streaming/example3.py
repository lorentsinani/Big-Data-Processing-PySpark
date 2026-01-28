from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min, max, count

spark = SparkSession.builder.appName("RateCountPerUser").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Read from rate source (includes timestamp automatically)
df = (
    spark.readStream
        .format("rate")
        .option("rowsPerSecond", 10)
        .load()
        .select(
            col("timestamp"),
            (col("value") % 5).cast("int").alias("user_id")
        )
)

# ------------------------------------------------------------------
# 1. Original cumulative count per user (unchanged logic)
# ------------------------------------------------------------------
counts = df.groupBy("user_id").count()

counts_query = (
    counts.writeStream
        .format("console")
        .outputMode("complete")
        .option("truncate", "false")
        .trigger(processingTime="5 seconds")
        .start()
)

# ------------------------------------------------------------------
# 2. Per-batch timestamp proof (this shows what's *really* happening)
# ------------------------------------------------------------------
def inspect_batch(batch_df, batch_id):
    print(f"\n===== Batch {batch_id} timing =====")
    (
        batch_df.agg(
            min("timestamp").alias("min_ts"),
            max("timestamp").alias("max_ts"),
            count("*").alias("rows")
        )
        .show(truncate=False)
    )

debug_query = (
    df.writeStream
        .foreachBatch(inspect_batch)
        .trigger(processingTime="5 seconds")
        .start()
)

spark.streams.awaitAnyTermination()
