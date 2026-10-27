from pyspark import pipelines as dp
from pyspark.sql import functions as F

from utilities.ticks_transformations import RULES, parse_and_validate_ticks

catalog = spark.conf.get("catalog")
bronze_schema = spark.conf.get("bronze_schema")
silver_schema = spark.conf.get("silver_schema", "team_crypto_silver")


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
            "symbol", "event_time", "price_usd", "volume_24h", "quote_currency", "source",
            "transport_source", "ingestion_timestamp", "load_date",
            "eventhub_partition", "eventhub_offset", "eventhub_timestamp",
        )
        # bounded state: keys older than the watermark are forgotten
        .withWatermark("event_time", "10 minutes")
        .dropDuplicatesWithinWatermark(["symbol", "event_time", "source"])
    )
