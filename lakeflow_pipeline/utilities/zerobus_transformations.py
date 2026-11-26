from pathlib import Path
import os
import yaml

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SCRIPT_PATH = Path(__file__).resolve()

PROJECT_ROOT = SCRIPT_PATH.parents[2]
CONFIG_PATH = PROJECT_ROOT / "config" / "config.yml"

with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

# Zerobus landing table: the producer writes one row per tick
#   value            STRING    - raw tick JSON, same payload as the Event Hub producer
#   producer_id      STRING    - producer run that sent the record
#   producer_seq     BIGINT    - counter inside one run, used to find lost records
#   producer_sent_at TIMESTAMP - when the producer sent the record
# Only `value` goes on to bronze; the producer columns stay in the landing table for checks.
ZEROBUS_SOURCE = config["streaming"]["zerobus"]["source_label"]

# Column order and types of ticks_bronze (as produced by the Event Hub flow)
BRONZE_COLUMNS = [
    "value",
    "eventhub_partition",
    "eventhub_offset",
    "eventhub_timestamp",
    "source",
    "ingestion_timestamp",
    "load_date",
]


def zerobus_to_bronze(raw: DataFrame) -> DataFrame:
    """Shape Zerobus landing rows like ticks_bronze rows.

    Event Hub metadata does not exist for Zerobus, so those columns are typed NULLs;
    the payload stays a raw JSON string, so silver parsing and DQ rules are reused as is.
    """
    return raw.select(
        F.col("value").cast("string").alias("value"),
        F.lit(None).cast("int").alias("eventhub_partition"),
        F.lit(None).cast("long").alias("eventhub_offset"),
        F.lit(None).cast("timestamp").alias("eventhub_timestamp"),
        F.lit(ZEROBUS_SOURCE).alias("source"),
        F.current_timestamp().alias("ingestion_timestamp"),
        F.current_date().alias("load_date"),
    )
