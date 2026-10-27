from pyspark import pipelines as dp
from pyspark.sql import functions as F

catalog = spark.conf.get("catalog")
silver_schema = spark.conf.get("silver_schema", "team_crypto_silver")

@dp.table(
    name=f"{catalog}.{silver_schema}.ticks_quarantine",
    comment="Invalid ticks with original JSON and rejection reasons",
    table_properties={"quality": "quarantine"},
)
@dp.expect("has_rejection_reason", "size(failed_rules) > 0")
def ticks_quarantine():
    return (
        spark.readStream.table("_ticks_validated")
        .filter(~F.col("is_valid"))
        .select(
            "value",
            "raw_symbol",
            "symbol",
            "price_usd",
            "event_time",
            "failed_rules",
            "quarantine_reason",
            "source",
            "ingestion_timestamp",
            "load_date",
            "eventhub_partition",
            "eventhub_offset",
            "eventhub_timestamp",
        )
    )