from pathlib import Path
import os
import yaml

from pyspark import pipelines as dp
from pyspark.sql import functions as F

SCRIPT_PATH = Path(__file__).resolve()

PROJECT_ROOT = SCRIPT_PATH.parents[2]
CONFIG_PATH = PROJECT_ROOT / "config" / "config.yml"

with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

catalog = config["catalog"]
silver_schema = config["silver"]["schema"]
quarantine_table = config["silver"]["quarantine_table"]
validated_view = config["silver"]["validated_view"]

@dp.table(
    name=f"{catalog}.{silver_schema}.{quarantine_table}",
    comment="Invalid ticks with original JSON and rejection reasons",
    table_properties={"quality": "quarantine"},
)
@dp.expect("has_rejection_reason", "size(failed_rules) > 0")
def ticks_quarantine():
    return (
        spark.readStream.table(validated_view)
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