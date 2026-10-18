from delta.tables import DeltaTable
from pyspark.sql import functions as F
from streaming.spark_session import get_spark
from streaming.silver.transforms import (
    parse, is_valid, quarantine_reason, to_silver, merge_new_events)

BRONZE = "s3a://lakehouse/bronze/events"
SILVER = "s3a://lakehouse/silver/events"
QUARANTINE = "s3a://lakehouse/quarantine/events"
CHECKPOINT = "s3a://checkpoints/silver_events"

spark = get_spark("silver_events")
spark.sparkContext.setLogLevel("WARN")

if not DeltaTable.isDeltaTable(spark, SILVER):
    (to_silver(parse(spark.read.format("delta").load(BRONZE))).limit(0)
        .write.format("delta").save(SILVER))

def process_batch(batch_df, batch_id):
    parsed = parse(batch_df).withColumn("is_valid", is_valid()).persist()

    bad = (parsed.filter(~F.col("is_valid"))
        .select("raw_json", "partition", "offset",
                quarantine_reason().alias("reason"),
                F.current_timestamp().alias("quarantined_at")))
    if not bad.isEmpty():
        (bad.write.format("delta").mode("append")
            .option("txnAppId", "silver_events_quarantine")
            .option("txnVersion", batch_id)
            .save(QUARANTINE))

    merge_new_events(spark, SILVER, to_silver(parsed.filter("is_valid")))
    parsed.unpersist()

if __name__ == "__main__":
    query = (spark.readStream.format("delta")
             .option("maxFilesPerTrigger", 20)
             .load(BRONZE)
             .writeStream.foreachBatch(process_batch)
             .option("checkpointLocation", CHECKPOINT)
             .trigger(processingTime="10 seconds")
             .start())
    query.awaitTermination()