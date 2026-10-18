from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

VALID_TYPES = ["page_view", "add_to_cart", "purchase"]

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_time", StringType()),   # cast to timestamp in parse()
    StructField("user_id", IntegerType()),
    StructField("product_id", IntegerType()),
    StructField("price", DoubleType()),
    StructField("qty", IntegerType()),
    StructField("session_id", StringType()),
    StructField("device", StringType()),
])

def parse(df):
    p = df.withColumn("p", F.from_json("raw_json", EVENT_SCHEMA))
    return p.select(
        "raw_json", "partition", "offset", "ingest_ts",
        F.col("p.event_id").alias("event_id"),
        F.col("p.event_type").alias("event_type"),
        F.col("p.event_time").cast("timestamp").alias("event_time"),
        F.col("p.user_id").alias("user_id"),
        F.col("p.product_id").alias("product_id"),
        F.col("p.price").alias("price"),
        F.col("p.qty").alias("qty"),
        F.col("p.session_id").alias("session_id"),
        F.col("p.device").alias("device"),
    )

def is_valid():
    # coalesce: a NULL comparison is neither true nor false, so NULLs would escape BOTH branches
    return F.coalesce(
        F.col("event_id").isNotNull() & F.col("event_time").isNotNull()
        & F.col("user_id").isNotNull() & F.col("product_id").isNotNull()
        & F.col("event_type").isin(*VALID_TYPES)
        & (F.col("price") > 0) & (F.col("qty") > 0),
        F.lit(False),
    )

def quarantine_reason():
    return (F.when(F.col("event_id").isNull(), "unparseable_or_missing_id")
             .otherwise("failed_validation"))

def to_silver(df):
    return df.select(
        "event_id", "event_type", "event_time",
        F.to_date("event_time").alias("event_date"),
        "user_id", "product_id", "price", "qty",
        F.round(F.col("price") * F.col("qty"), 2).alias("line_amount"),
        "session_id", "device",
        F.col("partition").alias("src_partition"),
        F.col("offset").alias("src_offset"),
        "ingest_ts",
        F.current_timestamp().alias("silver_ts"),
    )

def merge_new_events(spark, silver_path, good_df):
    """Idempotent upsert: insert only event_ids the table has never seen."""
    (DeltaTable.forPath(spark, silver_path).alias("t")
        .merge(good_df.dropDuplicates(["event_id"]).alias("s"), "t.event_id = s.event_id")
        .whenNotMatchedInsertAll()
        .execute())