import pytest
from pyspark.sql import functions as F

TOPIC = "ecom.events"
EXPECTED_PARTITIONS = {0, 1, 2}
EXPECTED_COLUMNS = {"raw_json", "kafka_key", "topic", "partition", "offset", "kafka_ts", "ingest_ts"}


# ---------- reusable check logic ----------

def offset_report(df):
    """Per partition: min/max offset, total rows, distinct offsets."""
    rows = (df.groupBy("partition")
              .agg(F.min("offset").alias("min_off"),
                   F.max("offset").alias("max_off"),
                   F.count("*").alias("rows"),
                   F.countDistinct("offset").alias("distinct_off"))
              .collect())
    return {r["partition"]: {"min": r["min_off"], "max": r["max_off"],
                             "rows": r["rows"], "distinct": r["distinct_off"]} for r in rows}

def gaps(stats):
    """Missing offsets between min and max (immune to duplicates)."""
    return {p: s["max"] - s["min"] + 1 - s["distinct"] for p, s in stats.items()}

def dupes(stats):
    """Rows that repeat an already-seen offset."""
    return {p: s["rows"] - s["distinct"] for p, s in stats.items()}


@pytest.fixture(scope="session")
def bronze_stats(bronze):
    return offset_report(bronze)


# ---------- unit tests: prove the checks can actually fail ----------

def test_gap_check_catches_a_missing_offset(spark):
    df = spark.createDataFrame([(0, 0), (0, 1), (0, 3)], ["partition", "offset"])  # offset 2 missing
    assert gaps(offset_report(df)) == {0: 1}

def test_dupe_check_catches_a_repeated_offset(spark):
    df = spark.createDataFrame([(0, 0), (0, 1), (0, 1), (0, 2)], ["partition", "offset"])
    stats = offset_report(df)
    assert dupes(stats) == {0: 1}
    assert gaps(stats) == {0: 0}   # a duplicate must not be mistaken for a gap

def test_clean_data_passes_both_checks(spark):
    df = spark.createDataFrame([(0, 0), (0, 1), (0, 2), (1, 0), (1, 1)], ["partition", "offset"])
    stats = offset_report(df)
    assert set(gaps(stats).values()) == {0} and set(dupes(stats).values()) == {0}


# ---------- integration tests: the real Bronze table ----------

def test_schema_is_exactly_what_we_designed(bronze):
    assert set(bronze.columns) == EXPECTED_COLUMNS

def test_table_is_not_empty(bronze):
    assert bronze.count() > 0

def test_only_expected_partitions_and_topic(bronze):
    assert {r["partition"] for r in bronze.select("partition").distinct().collect()} == EXPECTED_PARTITIONS
    assert [r["topic"] for r in bronze.select("topic").distinct().collect()] == [TOPIC]

def test_no_nulls_in_lineage_columns(bronze):
    cols = ["raw_json", "partition", "offset", "kafka_ts", "ingest_ts"]
    nulls = bronze.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in cols]).first().asDict()
    assert all(v == 0 for v in nulls.values()), nulls

def test_offsets_start_at_zero(bronze_stats):
    assert {p: s["min"] for p, s in bronze_stats.items()} == {p: 0 for p in bronze_stats}

def test_no_offset_gaps(bronze_stats):
    assert all(g == 0 for g in gaps(bronze_stats).values()), gaps(bronze_stats)

def test_no_duplicate_offsets(bronze_stats):
    assert all(d == 0 for d in dupes(bronze_stats).values()), dupes(bronze_stats)

def test_row_count_matches_offset_math(bronze, bronze_stats):
    assert bronze.count() == sum(s["max"] - s["min"] + 1 for s in bronze_stats.values())

def test_ingest_is_never_before_kafka_timestamp(bronze):
    assert bronze.filter(F.col("ingest_ts") < F.col("kafka_ts")).count() == 0

def test_bronze_is_never_ahead_of_kafka(spark, bronze_stats):
    kafka = (spark.read.format("kafka")
             .option("kafka.bootstrap.servers", "localhost:9092")
             .option("subscribe", TOPIC)
             .option("startingOffsets", "earliest")
             .option("endingOffsets", "latest")
             .load())
    kafka_max = {r["partition"]: r["m"]
                 for r in kafka.groupBy("partition").agg(F.max("offset").alias("m")).collect()}
    for p, s in bronze_stats.items():
        lag = kafka_max[p] - s["max"]
        print(f"partition {p}: bronze_max={s['max']} kafka_max={kafka_max[p]} lag={lag}")
        assert lag >= 0, "Bronze holds an offset Kafka doesn't have (topic recreated?)"

def test_bronze_keeps_the_garbage(bronze):
    # Bronze must NOT filter: malformed JSON from the producer should still be here
    total = bronze.count()
    if total < 1000:
        pytest.skip("not enough rows for the producer's ~0.5% garbage to show up")
    bad = bronze.filter(F.get_json_object("raw_json", "$.event_id").isNull()).count()
    print(f"unparseable rows kept in Bronze: {bad} of {total}")
    assert bad > 0