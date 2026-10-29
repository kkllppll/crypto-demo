"""Gold layer transformations: pure DataFrame functions, no table reads or writes.

Notebooks in gold_pipeline/ read silver tables, call these functions and write
the result. Unit tests can import this module directly.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

UNKNOWN = "unknown"

# silver.ohlc_prices has no `source` column: every candle comes from Kraken
OHLC_SOURCE = "kraken_api"

# source_name - (source_type, provider); source_name matches silver values
SOURCE_CATALOG = {
    "kraken_api": ("batch", "Kraken"),
    "streaming": ("streaming", "CoinGecko via Event Hub"),
}

# symbol - human-readable name; unknown symbols get UNKNOWN
ASSET_NAMES = {"BTC": "Bitcoin", "ETH": "Ethereum", "SOL": "Solana"}

FACT_OHLC_COLUMNS = [
    "asset_key",
    "date_key",
    "source_key",
    "event_time",
    "hour_of_day",
    "open",
    "high",
    "low",
    "close",
    "vwap",
    "volume",
    "trade_count",
    "price_change_pct",
]

FACT_TICK_COLUMNS = [
    "asset_key",
    "date_key",
    "source_key",
    "event_time",
    "price_usd",
    "volume_24h",
]


def _lookup(mapping: dict, column: str):
    """Value from a Python dict for every row; UNKNOWN when the key is missing."""
    spark_map = F.create_map(*[F.lit(x) for pair in mapping.items() for x in pair])
    return F.coalesce(spark_map[F.col(column)], F.lit(UNKNOWN))


# ---------------------------------------------------------------- silver


def current_ohlc(ohlc: DataFrame) -> DataFrame:
    """silver.ohlc_prices is SCD2: keep only the current version of each candle."""
    return ohlc.where(F.col("is_current"))


# ------------------------------------------------------------- dimensions


def dim_source_rows() -> list:
    """Static rows for dim_source: (source_name, source_type, provider)."""
    return [(name, kind, provider) for name, (kind, provider) in SOURCE_CATALOG.items()]


def distinct_symbols(*silver_dfs: DataFrame) -> DataFrame:
    """All symbols that appear in any silver table, one row each."""
    symbols = None
    for df in silver_dfs:
        part = df.select("symbol").where(F.col("symbol").isNotNull())
        symbols = part if symbols is None else symbols.unionByName(part)
    return symbols.distinct()


def build_dim_asset(symbols: DataFrame) -> DataFrame:
    """One row per symbol with its name (SCD1: no history)."""
    return symbols.select("symbol", _lookup(ASSET_NAMES, "symbol").alias("asset_name"))


# ------------------------------------------------------------ key lookups


def add_date_key(df: DataFrame, ts_column: str = "event_time") -> DataFrame:
    """date_key = yyyyMMdd as INT, the key of dim_date."""
    return df.withColumn("date_key", F.date_format(ts_column, "yyyyMMdd").cast("int"))


def attach_asset_key(df: DataFrame, dim_asset: DataFrame) -> DataFrame:
    d = dim_asset.select("asset_key", F.col("symbol").alias("_dim_symbol"))
    return df.join(d, df["symbol"] == d["_dim_symbol"], "left").drop("_dim_symbol")


def attach_source_key(df: DataFrame, dim_source: DataFrame) -> DataFrame:
    d = dim_source.select("source_key", F.col("source_name").alias("_source_name"))
    joined = df.join(
        d, F.coalesce(df["source"], F.lit(UNKNOWN)) == d["_source_name"], "left"
    )
    return joined.drop("_source_name")


# ------------------------------------------------------------------ facts


def build_fact_ohlc_hourly(
    ohlc: DataFrame, dim_asset: DataFrame, dim_source: DataFrame
) -> DataFrame:
    """Grain: one asset x one hour. Input: current versions from silver.ohlc_prices."""
    df = ohlc.dropDuplicates(["symbol", "event_time"]).withColumn(
        "source", F.lit(OHLC_SOURCE)
    )
    df = attach_asset_key(df, dim_asset)
    df = attach_source_key(df, dim_source)
    df = add_date_key(df)
    return df.select(
        "asset_key",
        "date_key",
        "source_key",
        "event_time",
        F.hour("event_time").alias("hour_of_day"),
        "open",
        "high",
        "low",
        "close",
        "vwap",
        "volume",
        "trade_count",
        F.when(
            F.col("open") > 0,
            F.round((F.col("close") - F.col("open")) / F.col("open") * 100, 4),
        ).alias("price_change_pct"),
    )


def build_fact_price_tick(
    ticks: DataFrame, dim_asset: DataFrame, dim_source: DataFrame
) -> DataFrame:
    """Grain: one asset x one moment (silver.ticks_silver).

    Lakeflow deduplicates only within its watermark, so duplicates of the
    business key are dropped here once more.
    """
    df = ticks.dropDuplicates(["symbol", "event_time"])
    df = attach_asset_key(df, dim_asset)
    df = attach_source_key(df, dim_source)
    df = add_date_key(df)
    return df.select(*FACT_TICK_COLUMNS)


# ---------------------------------------------------------- quality gate


def fact_quality_issues(
    fact: DataFrame,
    expected_rows: int,
    key_columns: list,
    dim_columns: list,
    calendar: DataFrame,
) -> list:
    """Returns a list of problems; empty list means the fact is OK.

    - row count equals the number of distinct business keys in silver
    - every dimension key was found (no NULL keys)
    - primary key is unique
    - every date_key exists in dim_date
    """
    issues = []

    rows = fact.count()
    if rows != expected_rows:
        issues.append(f"row count {rows} != expected {expected_rows}")

    for col in dim_columns:
        missing = fact.where(F.col(col).isNull()).count()
        if missing:
            issues.append(f"{missing} rows without {col}")

    duplicates = fact.groupBy(*key_columns).count().where("count > 1").count()
    if duplicates:
        issues.append(f"{duplicates} duplicate keys on {key_columns}")

    outside = (
        fact.select("date_key")
        .distinct()
        .join(calendar.select("date_key"), "date_key", "left_anti")
        .count()
    )
    if outside:
        issues.append(f"{outside} date_key values are outside dim_date")

    return issues
