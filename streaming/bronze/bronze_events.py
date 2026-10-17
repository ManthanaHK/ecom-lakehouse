from pyspark.sql.functions import current_timestamp, col
from streaming.spark_session import get_spark

spark = get_spark("bronze_events")
spark.sparkContext.setLogLevel("WARN")

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "ecom.events")
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", 5000)
    .option("failOnDataLoss", "false")
    .load()
)

bronze = (
    raw.select(
        col("value").cast("string").alias("raw_json"),
        col("key").cast("string").alias("kafka_key"),
        col("topic"), col("partition"), col("offset"),
        col("timestamp").alias("kafka_ts"),
    )
    .withColumn("ingest_ts", current_timestamp())
)

query = (
    bronze.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", "s3a://checkpoints/bronze_events")
    .trigger(processingTime="10 seconds")
    .start("s3a://lakehouse/bronze/events")
)
query.awaitTermination()