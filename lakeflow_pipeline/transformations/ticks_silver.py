from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

catalog = spark.conf.get("catalog")
bronze_schema = spark.conf.get("bronze_schema")
silver_schema = spark.conf.get("silver_schema", "team_crypto_silver")

# how to understand value: expected rows
PAYLOAD_SCHEMA = StructType([
    StructField("symbol", StringType()),
    StructField("price_usd", StringType()),
    StructField("ts", StringType()),
    StructField("_corrupt_record", StringType()),
])

# criteria for a good entry
RULES = {
    "valid_json": "value IS NOT NULL AND payload IS NOT NULL AND payload._corrupt_record IS NULL",
    "known_symbol": "symbol IS NOT NULL",
    "positive_finite_price": "price_usd IS NOT NULL AND NOT isnan(price_usd) AND price_usd > 0 AND price_usd < CAST('Infinity' AS DOUBLE)",
    "valid_event_time": "event_time IS NOT NULL",
    "not_in_future": "event_time IS NOT NULL AND event_time <= ingestion_timestamp + INTERVAL 5 MINUTES", 
}

# disassemble and check
def parse_and_validate_ticks(bronze):
    """Pure DataFrame transformation; also usable by the team's unit tests."""
    symbols = F.create_map(
        F.lit("bitcoin"), F.lit("BTC"),
        F.lit("ethereum"), F.lit("ETH"),
        F.lit("solana"), F.lit("SOL"),
        F.lit("btc"), F.lit("BTC"),
        F.lit("eth"), F.lit("ETH"),
        F.lit("sol"), F.lit("SOL"),
    )
    parsed = (
        bronze
        .withColumn("payload", F.from_json("value", PAYLOAD_SCHEMA, {"mode": "PERMISSIVE", "columnNameOfCorruptRecord":"_corrupt_record"},))
        
        .withColumn("raw_symbol", F.col("payload.symbol"))
        .withColumn("symbol", symbols[F.lower(F.trim(F.col("raw_symbol")))])
        .withColumn("price_usd", F.expr("try_cast(payload.price_usd AS DOUBLE)"))
        .withColumn("event_seconds", F.expr("try_cast(payload.ts AS DOUBLE)"))
        # Producer uses time.time(): Unix SECONDS, including fractions
        # Convert only values ​​within the 2009–2099 range (protective restriction)
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


@dp.table(
    name="_ticks_validated",
    private=True,
    comment="Parsed ticks, including rejected records; quality metrics before routing",
)
@dp.expect_all({name: name for name in RULES})
def ticks_validated():
    return parse_and_validate_ticks(spark.readStream.table(f"{catalog}.{bronze_schema}.ticks_bronze"))


@dp.table(
    name=f"{catalog}.{silver_schema}.ticks_silver",
    comment="Valid crypto ticks; unique by symbol, event_time and source",
    table_properties={"quality": "silver"},
)
@dp.expect("positive_price", "price_usd > 0")
@dp.expect("required_symbol", "symbol IS NOT NULL")
@dp.expect("required_event_time", "event_time IS NOT NULL")
def ticks_silver():
    return (
        spark.readStream.table("_ticks_validated")
        .filter(F.col("is_valid"))
        .withColumnRenamed("source", "transport_source")
        .withColumn("source", F.lit("streaming"))
        .withColumn("quote_currency", F.lit("USD"))
        .select(
            "symbol", "event_time", "price_usd", "quote_currency", "source",
            "transport_source", "ingestion_timestamp", "load_date",
            "eventhub_partition", "eventhub_offset", "eventhub_timestamp",
        )
        .dropDuplicates(["symbol", "event_time", "source"])
    )
