import datetime as dt
from chispa.dataframe_comparer import assert_df_equality
import pytest
from pyspark.sql.types import (
    BooleanType, DoubleType, IntegerType, LongType,
    StringType, StructField, StructType, TimestampType,
)

from gold_pipeline.utilities.gold_transformations import (
    add_date_key,
    build_dim_asset,
    build_fact_ohlc_hourly,
    build_fact_price_tick,
    current_ohlc,
    distinct_symbols,
    fact_quality_issues,
)

T = dt.datetime(2024, 10, 30, 10, 0, 0)


@pytest.fixture(autouse=True)
def utc_timezone(spark):
    """date_key / hour_of_day depend on the session time zone."""
    spark.conf.set("spark.sql.session.timeZone", "UTC")


OHLC_SCHEMA = StructType([
    StructField("symbol", StringType()),
    StructField("event_time", TimestampType()),
    StructField("open", DoubleType()),
    StructField("high", DoubleType()),
    StructField("low", DoubleType()),
    StructField("close", DoubleType()),
    StructField("vwap", DoubleType()),
    StructField("volume", DoubleType()),
    StructField("trade_count", IntegerType()),
    StructField("is_current", BooleanType()),
])

TICK_SCHEMA = StructType([
    StructField("symbol", StringType()),
    StructField("event_time", TimestampType()),
    StructField("price_usd", DoubleType()),
    StructField("volume_24h", DoubleType()),
    StructField("source", StringType()),
])

FACT_SCHEMA = StructType([
    StructField("asset_key", LongType()),
    StructField("date_key", IntegerType()),
    StructField("source_key", LongType()),
    StructField("event_time", TimestampType()),
])


def dims(spark, source_key=1, source_name="kraken_api"):
    dim_asset = spark.createDataFrame([(1, "BTC")], ["asset_key", "symbol"])
    dim_source = spark.createDataFrame([(source_key, source_name)], ["source_key", "source_name"])
    return dim_asset, dim_source


def ohlc_row(open_=100.0, close=110.0, symbol="BTC", ts=T, is_current=True):
    return (symbol, ts, open_, 120.0, 90.0, close, 102.0, 1000.0, 50, is_current)


def test_current_ohlc_keeps_only_current(spark):
    df = spark.createDataFrame(
        [ohlc_row(close=100.0, is_current=True), ohlc_row(close=95.0, is_current=False)],
        OHLC_SCHEMA,
    )

    rows = current_ohlc(df).collect()

    assert len(rows) == 1
    assert rows[0]["close"] == 100.0


def test_add_date_key_uses_event_date(spark):
    df = spark.createDataFrame([(dt.datetime(2024, 10, 30, 23, 30),)], ["event_time"])

    assert add_date_key(df).collect()[0]["date_key"] == 20241030


def test_build_dim_asset_unknown_symbol(spark):
    symbols = spark.createDataFrame([("BTC",), ("UNKNOWN_COIN",)], ["symbol"])

    names = {r["symbol"]: r["asset_name"] for r in build_dim_asset(symbols).collect()}

    assert names == {"BTC": "Bitcoin", "UNKNOWN_COIN": "unknown"}


def test_distinct_symbols_drops_null_and_duplicates(spark):
    schema = StructType([StructField("symbol", StringType())])
    df1 = spark.createDataFrame([("BTC",), (None,)], schema)
    df2 = spark.createDataFrame([("BTC",), ("ETH",)], schema)

    symbols = [r["symbol"] for r in distinct_symbols(df1, df2).collect()]

    assert sorted(symbols) == ["BTC", "ETH"]

def test_fact_ohlc_hourly_values(spark):
    df = spark.createDataFrame([ohlc_row(open_=100.0, close=110.0)], OHLC_SCHEMA)
    dim_asset, dim_source = dims(spark, source_key=7)

    row = build_fact_ohlc_hourly(df, dim_asset, dim_source).collect()[0]

    assert row["price_change_pct"] == 10.0
    assert row["hour_of_day"] == 10
    assert row["asset_key"] == 1
    assert row["source_key"] == 7
    assert row["date_key"] == 20241030


def test_fact_ohlc_hourly_zero_open_gives_null_change(spark):
    df = spark.createDataFrame([ohlc_row(open_=0.0, close=110.0)], OHLC_SCHEMA)
    dim_asset, dim_source = dims(spark)

    row = build_fact_ohlc_hourly(df, dim_asset, dim_source).collect()[0]

    assert row["price_change_pct"] is None


def test_fact_ohlc_hourly_deduplicates(spark):
    df = spark.createDataFrame([ohlc_row(), ohlc_row()], OHLC_SCHEMA)
    dim_asset, dim_source = dims(spark)

    assert build_fact_ohlc_hourly(df, dim_asset, dim_source).count() == 1


def test_fact_price_tick_deduplicates_and_keys(spark):
    tick = ("BTC", T, 65000.0, 100.0, "streaming")
    df = spark.createDataFrame([tick, tick], TICK_SCHEMA)
    dim_asset, dim_source = dims(spark, source_key=2, source_name="streaming")

    rows = build_fact_price_tick(df, dim_asset, dim_source).collect()

    assert len(rows) == 1
    assert rows[0]["source_key"] == 2
    assert rows[0]["asset_key"] == 1
    assert rows[0]["date_key"] == 20241030


def test_fact_price_tick_unknown_symbol_gets_null_asset_key(spark):
    """Missing dimension row must surface as NULL key (caught by the quality gate)."""
    df = spark.createDataFrame([("DOGE", T, 0.1, 1.0, "streaming")], TICK_SCHEMA)
    dim_asset, dim_source = dims(spark, source_key=2, source_name="streaming")

    row = build_fact_price_tick(df, dim_asset, dim_source).collect()[0]

    assert row["asset_key"] is None


KEYS = ["asset_key", "event_time"]


@pytest.fixture
def calendar(spark):
    return spark.createDataFrame([(20241030,)], ["date_key"])


def make_fact(spark, rows):
    return spark.createDataFrame(rows, FACT_SCHEMA)


def issues_for(fact, calendar, expected_rows=1):
    return fact_quality_issues(
        fact,
        expected_rows=expected_rows,
        key_columns=KEYS,
        dim_columns=["asset_key", "date_key", "source_key"],
        calendar=calendar,
    )


def test_quality_clean_fact_has_no_issues(spark, calendar):
    fact = make_fact(spark, [(1, 20241030, 1, T)])

    assert issues_for(fact, calendar) == []


def test_quality_wrong_row_count(spark, calendar):
    fact = make_fact(spark, [(1, 20241030, 1, T)])

    issues = issues_for(fact, calendar, expected_rows=5)

    assert any("row count" in i for i in issues)


def test_quality_null_dimension_key(spark, calendar):
    fact = make_fact(spark, [(None, 20241030, 1, T)])

    issues = issues_for(fact, calendar)

    assert any("without asset_key" in i for i in issues)


def test_quality_duplicate_keys(spark, calendar):
    fact = make_fact(spark, [(1, 20241030, 1, T), (1, 20241030, 1, T)])

    issues = issues_for(fact, calendar, expected_rows=2)

    assert any("duplicate keys" in i for i in issues)


def test_quality_date_outside_dim_date(spark, calendar):
    fact = make_fact(spark, [(1, 20990101, 1, dt.datetime(2099, 1, 1, 10, 0))])

    issues = issues_for(fact, calendar)

    assert any("outside dim_date" in i for i in issues)


def test_add_date_key_dataframe(spark):
    df = spark.createDataFrame(
        [(dt.datetime(2024, 10, 30, 23, 30),), (dt.datetime(2024, 1, 2, 0, 0),)],
        StructType([StructField("event_time", TimestampType())]),
    )
    expected = spark.createDataFrame(
        [(dt.datetime(2024, 10, 30, 23, 30), 20241030), (dt.datetime(2024, 1, 2, 0, 0), 20240102)],
        StructType([
            StructField("event_time", TimestampType()),
            StructField("date_key", IntegerType()),
        ]),
    )

    assert_df_equality(add_date_key(df), expected, ignore_row_order=True, ignore_nullable=True)


def test_build_fact_price_tick_dataframe(spark):
    ticks = spark.createDataFrame(
        [
            ("BTC", T, 65000.0, 100.0, "streaming"),
            ("BTC", T, 65000.0, 100.0, "streaming"),  # duplicate of the business key
            ("ETH", T, 2500.0, 50.0, "streaming"),
        ],
        TICK_SCHEMA,
    )
    dim_asset = spark.createDataFrame([(1, "BTC"), (2, "ETH")], ["asset_key", "symbol"])
    dim_source = spark.createDataFrame([(9, "streaming")], ["source_key", "source_name"])

    expected = spark.createDataFrame(
        [(1, 20241030, 9, T, 65000.0, 100.0), (2, 20241030, 9, T, 2500.0, 50.0)],
        StructType([
            StructField("asset_key", LongType()),
            StructField("date_key", IntegerType()),
            StructField("source_key", LongType()),
            StructField("event_time", TimestampType()),
            StructField("price_usd", DoubleType()),
            StructField("volume_24h", DoubleType()),
        ]),
    )

    result = build_fact_price_tick(ticks, dim_asset, dim_source)

    assert_df_equality(result, expected, ignore_row_order=True, ignore_nullable=True)