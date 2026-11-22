from pyspark import pipelines as dp

from utilities.zerobus_transformations import zerobus_to_bronze

catalog = spark.conf.get("catalog")
bronze_schema = spark.conf.get("bronze_schema")
zerobus_table = spark.conf.get("zerobus_table", "ticks_zerobus_raw")


# second source for the existing ticks_bronze table (defined in ticks_bronze.py)
# event hub keeps its own default flow; this flow only appends Zerobus rows
# so silver, quarantine and gold receive both transports without code changes
@dp.append_flow(
    target=f"{catalog}.{bronze_schema}.ticks_bronze",
    name="ticks_bronze_from_zerobus",
    comment="Price ticks pushed directly to Delta via Zerobus Ingest",
)
def ticks_bronze_from_zerobus():
    return zerobus_to_bronze(
        spark.readStream.table(f"{catalog}.{bronze_schema}.{zerobus_table}")
    )
