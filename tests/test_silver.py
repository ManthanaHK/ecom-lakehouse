import json
from datetime import datetime, date
import pytest
from pyspark.sql import functions as F
from streaming.silver.transforms import (
    parse, is_valid, quarantine_reason, to_silver, merge_new_events)

# ---------- helpers ----------

BRONZE_SCHEMA = "raw_json string, partition int, offset long, ingest_ts timestamp"
BASE = {"event_id": "e-1", "event_type": "purchase", "event_time": "2026-10-17T10:00:00+00:00",
        "user_id": 7, "product_id": 42, "price": 19.99, "qty": 3, "session_id": "s-1", "device": "web"}

def ev(**override):
    return json.dumps({**BASE, **override})

def bronze_df(spark, *raw_jsons):
    ts = datetime(2026, 10, 17, 12, 0, 0)
    return spark.createDataFrame([(j, 0, i, ts) for i, j in enumerate(raw_jsons)], BRONZE_SCHEMA)

def classify(spark, raw):
    row = (parse(bronze_df(spark, raw))
           .withColumn("ok", is_valid())
           .withColumn("reason", quarantine_reason()).first())
    return row["ok"], row["reason"]


# ---------- unit tests: the rules ----------

def test_a_good_event_is_accepted(spark):
    ok, _ = classify(spark, ev())
    assert ok

INVALID = {
    "price_negative": ev(price=-5.0),
    "price_zero": ev(price=0),
    "qty_zero": ev(qty=0),
    "unknown_event_type": ev(event_type="refund"),
    "missing_event_id": ev(event_id=None),
    "missing_user_id": ev(user_id=None),
    "bad_timestamp": ev(event_time="not-a-time"),
    "price_is_text": ev(price="abc"),
    "truncated_json": '{"event_id": "oops", "price": ',
    "empty_object": "{}",
}

@pytest.mark.parametrize("raw", INVALID.values(), ids=INVALID.keys())
def test_bad_rows_are_rejected(spark, raw):
    ok, _ = classify(spark, raw)
    assert not ok

def test_garbage_gets_the_unparseable_reason(spark):
    assert classify(spark, INVALID["truncated_json"])[1] == "unparseable_or_missing_id"

def test_well_formed_but_invalid_gets_failed_validation(spark):
    assert classify(spark, ev(price=-5.0))[1] == "failed_validation"


# ---------- unit tests: derived columns ----------

def test_line_amount_is_price_times_qty(spark):
    row = to_silver(parse(bronze_df(spark, ev(price=19.99, qty=3)))).first()
    assert row["line_amount"] == pytest.approx(59.97)

def test_event_date_is_computed_in_utc(spark):
    # 23:30 UTC is already the next day in India; if the session timezone drifts, this fails
    row = to_silver(parse(bronze_df(spark, ev(event_time="2026-10-17T23:30:00+00:00")))).first()
    assert row["event_date"] == date(2026, 10, 17)


# ---------- unit tests: idempotent MERGE ----------

def _seed_silver(spark, path):
    to_silver(parse(bronze_df(spark, ev()))).limit(0).write.format("delta").save(path)

def test_replaying_a_batch_does_not_duplicate_rows(spark, tmp_path):
    path = str(tmp_path / "silver")
    _seed_silver(spark, path)
    batch = to_silver(parse(bronze_df(spark, ev(event_id="a"), ev(event_id="b"))))
    merge_new_events(spark, path, batch)
    merge_new_events(spark, path, batch)          # simulate foreachBatch replaying the same batch
    assert spark.read.format("delta").load(path).count() == 2

def test_duplicates_inside_one_batch_collapse_to_one_row(spark, tmp_path):
    path = str(tmp_path / "silver")
    _seed_silver(spark, path)
    merge_new_events(spark, path, to_silver(parse(bronze_df(spark, ev(event_id="a"), ev(event_id="a")))))
    assert spark.read.format("delta").load(path).count() == 1


# ---------- integration tests: the real tables (run with Bronze and Silver STOPPED) ----------

@pytest.fixture(scope="module")
def parsed(bronze):
    return parse(bronze).withColumn("ok", is_valid()).cache()

def test_silver_event_ids_are_unique(silver):
    assert silver.count() == silver.select("event_id").distinct().count()

def test_silver_holds_every_valid_event(parsed, silver):
    expected = parsed.filter("ok").select("event_id").distinct().count()
    assert silver.count() == expected

def test_quarantine_holds_every_invalid_row(parsed, quarantine):
    assert quarantine.count() == parsed.filter(~F.col("ok")).count()

def test_quarantine_has_no_repeated_offsets(quarantine):
    assert quarantine.count() == quarantine.select("partition", "offset").distinct().count()

def test_every_silver_row_passes_the_rules(silver):
    assert silver.filter(~is_valid()).count() == 0

def test_silver_line_amount_matches_price_times_qty(silver):
    off = silver.filter(F.abs(F.col("line_amount") - F.round(F.col("price") * F.col("qty"), 2)) > 0.001)
    assert off.count() == 0

def test_silver_event_date_matches_event_time(silver):
    assert silver.filter(F.col("event_date") != F.to_date("event_time")).count() == 0

def test_the_reconciliation_equation(bronze, parsed, silver, quarantine):
    valid_rows = parsed.filter("ok").count()
    dupes = valid_rows - parsed.filter("ok").select("event_id").distinct().count()
    b, s, q = bronze.count(), silver.count(), quarantine.count()
    print(f"bronze={b} = silver={s} + quarantine={q} + duplicates={dupes}")
    assert b == s + q + dupes