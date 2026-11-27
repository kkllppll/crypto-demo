from pathlib import Path
import os
import yaml

from pyspark import pipelines as dp
from pyspark.sql import functions as F

CONFIG_PATH = Path(spark.conf.get("config_path"))

with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

catalog = spark.conf.get("catalog")
bronze_schema = spark.conf.get("bronze_schema")
bronze_table = config["bronze"]["eventhub_ticks"]
eventhub = config["streaming"]["eventhub"]
eventhub_namespace = eventhub["namespace"]
eventhub_name = eventhub["name"]
secret_scope = config["streaming"]["secret_scope"]

connection_str = dbutils.secrets.get(scope=secret_scope, key=eventhub["connection_secret_key"])

jaas_config = (
    "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
    f'username="$ConnectionString" password="{connection_str}";'
)

kafka_options = {
    "kafka.bootstrap.servers": f"{eventhub_namespace}.servicebus.windows.net:{eventhub['port']}",
    "subscribe": eventhub_name,
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.jaas.config": jaas_config,
    "startingOffsets": eventhub["starting_offsets"],
    # Event Hub drops events after the retention period; don't fail the stream on that gap
    "failOnDataLoss": str(eventhub["fail_on_data_loss"]).lower(),
}


@dp.table(
    name=f"{catalog}.{bronze_schema}.{bronze_table}",
    comment="Raw price ticks from Event Hub, payload kept as JSON string",
    table_properties={"quality": "bronze"},
)
@dp.expect("value_not_null", "value IS NOT NULL")
def ticks_bronze():
    return (
        spark.readStream.format("kafka")
        .options(**kafka_options)
        .load()
        .select(
            F.col("value").cast("string").alias("value"),
            F.col("partition").alias("eventhub_partition"),
            F.col("offset").alias("eventhub_offset"),
            F.col("timestamp").alias("eventhub_timestamp"),
            F.lit(eventhub["source_label"]).alias("source"),
            F.current_timestamp().alias("ingestion_timestamp"),
            F.current_date().alias("load_date"),
        )
    )
