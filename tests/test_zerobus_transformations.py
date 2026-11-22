import datetime as dt
import json

from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from lakeflow_pipeline.utilities.ticks_transformations import parse_and_validate_ticks
from lakeflow_pipeline.utilities.zerobus_transformations import (
    BRONZE_COLUMNS,
    ZEROBUS_SOURCE,
    zerobus_to_bronze,
)

RAW_SCHEMA = StructType(
    [
        StructField("value", StringType(), True),
        StructField("producer_id", StringType(), False),
        StructField("producer_seq", LongType(), False),
        StructField("producer_sent_at", TimestampType(), False),
    ]
)

SENT_AT = dt.datetime(2024, 10, 27, 12, 0, 0)

TICK = json.dumps({"symbol": "bitcoin", "price_usd": 65000.0, "ts": 1730000000.0})

# types the Kafka (Event Hub) flow writes into ticks_bronze
EVENTHUB_TYPES = {
    "value": "string",
    "eventhub_partition": "int",
    "eventhub_offset": "bigint",
    "eventhub_timestamp": "timestamp",
    "source": "string",
    "ingestion_timestamp": "timestamp",
    "load_date": "date",
}


def test_zerobus_rows_match_bronze_schema(spark):
    raw = spark.createDataFrame([(TICK, "run-1", 1, SENT_AT)], RAW_SCHEMA)

    bronze = zerobus_to_bronze(raw)

    assert bronze.columns == BRONZE_COLUMNS
    assert dict(bronze.dtypes) == EVENTHUB_TYPES


def test_zerobus_rows_are_tagged_and_keep_payload(spark):
    raw = spark.createDataFrame([(TICK, "run-1", 1, SENT_AT)], RAW_SCHEMA)

    row = zerobus_to_bronze(raw).collect()[0]

    assert row.source == ZEROBUS_SOURCE
    assert row.value == TICK
    assert row.eventhub_partition is None
    assert row.eventhub_offset is None
    assert row.eventhub_timestamp is None


def test_zerobus_rows_pass_existing_silver_rules(spark):
    bad_tick = json.dumps({"symbol": "bitcoin", "price_usd": -1.0, "ts": 1730000000.0})
    raw = spark.createDataFrame(
        [(TICK, "run-1", 1, SENT_AT), (bad_tick, "run-1", 2, SENT_AT)], RAW_SCHEMA
    )

    rows = (
        parse_and_validate_ticks(zerobus_to_bronze(raw)).orderBy("price_usd").collect()
    )

    assert [r.is_valid for r in rows] == [False, True]
    assert rows[0].failed_rules == ["positive_finite_price"]
    assert rows[1].symbol == "BTC"
