from pathlib import Path
import os
import yaml

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from utilities.ticks_transformations import RULES, parse_and_validate_ticks

SCRIPT_PATH = Path(__file__).resolve()

PROJECT_ROOT = SCRIPT_PATH.parents[2]
CONFIG_PATH = PROJECT_ROOT / "config" / "config.yml"

with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

catalog = spark.conf.get("catalog")
bronze_schema = spark.conf.get("bronze_schema")
bronze_table = config["bronze"]["eventhub_ticks"]
silver_schema = spark.conf.get("silver_schema")
silver_table = config["silver"]["ticks_table"]
validated_view = config["silver"]["validated_view"]
streaming = config["streaming"]

@dp.table(
    name=validated_view,
    private=True,
    comment="Parsed ticks, including rejected records; quality metrics before routing",
)
@dp.expect_all({name: name for name in RULES})
def ticks_validated():
    return parse_and_validate_ticks(spark.readStream.table(f"{catalog}.{bronze_schema}.{bronze_table}"))


@dp.table(
    name=f"{catalog}.{silver_schema}.{silver_table}",
    comment="Valid crypto ticks; unique by symbol, event_time and source",
    table_properties={"quality": "silver"},
)
@dp.expect("positive_price", "price_usd > 0")
@dp.expect("required_symbol", "symbol IS NOT NULL")
@dp.expect("required_event_time", "event_time IS NOT NULL")
def ticks_silver():
    return (
        spark.readStream.table(validated_view)
        .filter(F.col("is_valid"))
        .withColumnRenamed("source", "transport_source")
        .withColumn("source", F.lit(streaming["source_label"]))
        .withColumn("quote_currency", F.lit(streaming["quote_currency"]))
        .select(
            "symbol", "event_time", "price_usd", "volume_24h", "quote_currency", "source",
            "transport_source", "ingestion_timestamp", "load_date",
            "eventhub_partition", "eventhub_offset", "eventhub_timestamp",
        )
        # bounded state: keys older than the watermark are forgotten
        .withWatermark("event_time", streaming["watermark"])
        .dropDuplicatesWithinWatermark(["symbol", "event_time", "source"])
    )