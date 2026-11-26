from pathlib import Path
import os
import yaml

from pyspark import pipelines as dp

from utilities.zerobus_transformations import zerobus_to_bronze

SCRIPT_PATH = Path(__file__).resolve()

PROJECT_ROOT = SCRIPT_PATH.parents[2]
CONFIG_PATH = PROJECT_ROOT / "config" / "config.yml"

with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

catalog = config["catalog"]
bronze_schema = config["bronze"]["schema"]
bronze_table = config["bronze"]["eventhub_ticks"]
zerobus_table = config["bronze"]["zerobus_ticks"]

# second source for the existing ticks_bronze table (defined in ticks_bronze.py)
# event hub keeps its own default flow; this flow only appends Zerobus rows
# so silver, quarantine and gold receive both transports without code changes
@dp.append_flow(
    target=f"{catalog}.{bronze_schema}.{bronze_table}",
    name="ticks_bronze_from_zerobus",
    comment="Price ticks pushed directly to Delta via Zerobus Ingest",
)
def ticks_bronze_from_zerobus():
    return zerobus_to_bronze(
        spark.readStream.table(f"{catalog}.{bronze_schema}.{zerobus_table}")
    )