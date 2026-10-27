from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

# how to understand value: expected rows
PAYLOAD_SCHEMA = StructType([
    StructField("symbol", StringType()),
    StructField("price_usd", StringType()),
    StructField("ts", StringType()),
    StructField("volume_24h", StringType()),
    StructField("_corrupt_record", StringType()),
])

# known CoinGecko ids get a ticker; any other non-empty symbol passes through upper-cased
SYMBOL_MAP = {
    "bitcoin": "BTC",
    "ethereum": "ETH",
    "solana": "SOL",
    "btc": "BTC",
    "eth": "ETH",
    "sol": "SOL",
}

# criteria for a good entry
RULES = {
    "valid_json": "value IS NOT NULL AND payload IS NOT NULL AND payload._corrupt_record IS NULL",
    "valid_symbol": "symbol IS NOT NULL",
    "positive_finite_price": "price_usd IS NOT NULL AND NOT isnan(price_usd) AND price_usd > 0 AND price_usd < CAST('Infinity' AS DOUBLE)",
    "valid_event_time": "event_time IS NOT NULL",
    "not_in_future": "event_time IS NOT NULL AND event_time <= ingestion_timestamp + INTERVAL 5 MINUTES",
}


def normalize_symbol(raw_symbol):
    key = F.nullif(F.lower(F.trim(raw_symbol)), F.lit(""))
    symbols = F.create_map(*[F.lit(x) for pair in SYMBOL_MAP.items() for x in pair])
    return F.coalesce(symbols[key], F.upper(key))


# disassemble and check
def parse_and_validate_ticks(bronze: DataFrame) -> DataFrame:
    """Pure DataFrame transformation; also usable by the team's unit tests."""
    parsed = (
        bronze
        .withColumn("payload", F.from_json("value", PAYLOAD_SCHEMA, {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"}))
        .withColumn("raw_symbol", F.col("payload.symbol"))
        .withColumn("symbol", normalize_symbol(F.col("raw_symbol")))
        .withColumn("price_usd", F.expr("try_cast(payload.price_usd AS DOUBLE)"))
        .withColumn("volume_24h", F.expr("try_cast(payload.volume_24h AS DOUBLE)"))
        .withColumn("event_seconds", F.expr("try_cast(payload.ts AS DOUBLE)"))
        # Producer uses time.time(): Unix SECONDS, including fractions
        # Convert only values within the 2009–2099 range (protective restriction)
        # If a value is outside this range or is NULL, event_time will be NULL
        .withColumn("event_time", F.when(
            F.col("event_seconds").between(1230768000.0, 4102444799.0),
            F.timestamp_seconds("event_seconds"),
        ))
    )

    for rule_name, predicate in RULES.items():
        parsed = parsed.withColumn(rule_name, F.coalesce(F.expr(predicate), F.lit(False)))
    failures = F.array(*[F.when(~F.col(name), F.lit(name)) for name in RULES])

    return (
        parsed
        .withColumn("failed_rules", F.filter(failures, lambda item: item.isNotNull()))
        .withColumn("is_valid", F.size("failed_rules") == 0)
        .withColumn("quarantine_reason", F.concat_ws(", ", F.col("failed_rules")))
    )
