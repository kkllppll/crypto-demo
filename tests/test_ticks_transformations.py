import datetime as dt
import json

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

from lakeflow_pipeline.utilities.ticks_transformations import (
    RULES,
    normalize_symbol,
    parse_and_validate_ticks,
)

NORMALIZE_SCHEMA = StructType([
    StructField("id", StringType(), False),
    StructField("raw_symbol", StringType(), True),
    StructField("target", StringType(), True),
])

NORMALIZE_CASES = [
    ("bitcoin", "BTC"),
    ("BTC", "BTC"),
    ("Bitcoin ", "BTC"),
    (" ETH ", "ETH"),
    ("sol", "SOL"),
    (None, None),
    ("", None),
    ("cardano", None),
]


def test_normalize_symbol(spark):
    rows = [(str(i), raw, target) for i, (raw, target) in enumerate(NORMALIZE_CASES)]
    df = spark.createDataFrame(rows, NORMALIZE_SCHEMA)

    result = df.withColumn("actual", normalize_symbol(df.raw_symbol)).collect()

    for row in result:
        assert row.actual == row.target, f"raw={row.raw_symbol!r}"

INGESTED = dt.datetime(2024, 10, 30, 10, 0, 0, tzinfo=dt.timezone.utc)
INGESTED_EPOCH = int(INGESTED.timestamp())
TS_OK = "1730000000"  # 2024-10-27, in the past

SCHEMA = StructType([
    StructField("value", StringType(), True),
    StructField("ingestion_timestamp", TimestampType(), True),
])


def tick(**overrides):
    """JSON tick; pass field=None to drop the field from the payload."""
    fields = {"symbol": "bitcoin", "price_usd": "65000", "ts": TS_OK, **overrides}
    return json.dumps({k: v for k, v in fields.items() if v is not None})


ALL = set(RULES)

CASES = [
    pytest.param(tick(), set(), id="valid"),
    pytest.param(tick(symbol="BTC"), set(), id="valid_ticker_alias"),
    pytest.param(tick(price_usd="0"), {"positive_finite_price"}, id="price_zero"),
    pytest.param(tick(price_usd="-10"), {"positive_finite_price"}, id="price_negative"),
    pytest.param(tick(price_usd="abc"), {"positive_finite_price"}, id="price_not_number"),
    pytest.param(tick(price_usd="NaN"), {"positive_finite_price"}, id="price_nan"),
    pytest.param(tick(price_usd="Infinity"), {"positive_finite_price"}, id="price_infinity"),
    pytest.param(tick(price_usd=None), {"positive_finite_price"}, id="price_missing"),
    pytest.param(tick(symbol="cardano"), {"known_symbol"}, id="unknown_symbol"),
    pytest.param(tick(ts="1730000000000"), {"valid_event_time", "not_in_future"}, id="ts_in_millis"),
    pytest.param(tick(ts=None), {"valid_event_time", "not_in_future"}, id="ts_missing"),
    pytest.param(tick(ts=str(INGESTED_EPOCH + 600)), {"not_in_future"}, id="ts_future_10min"),
    pytest.param(tick(ts=str(INGESTED_EPOCH + 300)), set(), id="ts_future_exactly_5min_ok"),
    pytest.param(
        tick(symbol="cardano", price_usd="-10"),
        {"known_symbol", "positive_finite_price"},
        id="double_violation",
    ),
    pytest.param("not-a-json", ALL, id="broken_json"),
    pytest.param(None, ALL, id="value_null"),
]


@pytest.mark.parametrize("value, expected_failed", CASES)
def test_parse_and_validate_ticks(spark, value, expected_failed):
    df = spark.createDataFrame([(value, INGESTED)], SCHEMA)

    row = parse_and_validate_ticks(df).select(
        "is_valid", "failed_rules", "quarantine_reason"
    ).collect()[0]

    assert set(row.failed_rules) == expected_failed
    assert row.is_valid == (not expected_failed)
    assert (row.quarantine_reason == "") == (not expected_failed)


def test_parse_valid_tick_values(spark):
    """A valid tick is parsed into the right typed values."""
    df = spark.createDataFrame([(tick(volume_24h="123.5"), INGESTED)], SCHEMA)

    row = parse_and_validate_ticks(df).select(
        "symbol", "price_usd", "volume_24h",
        F.unix_timestamp("event_time").alias("event_epoch"),
    ).collect()[0]

    assert row.symbol == "BTC"
    assert row.price_usd == 65000.0
    assert row.volume_24h == 123.5
    assert row.event_epoch == int(TS_OK)