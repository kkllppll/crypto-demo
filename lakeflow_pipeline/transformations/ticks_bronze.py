from pyspark import pipelines as dp
from pyspark.sql import functions as F

catalog = spark.conf.get("catalog")
bronze_schema = spark.conf.get("bronze_schema")
eventhub_namespace = spark.conf.get("eventhub_namespace")
eventhub_name = spark.conf.get("eventhub_name")
secret_scope = spark.conf.get("secret_scope")

connection_str = dbutils.secrets.get(scope=secret_scope, key="eventhub-connection-string")

jaas_config = (
    "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
    f'username="$ConnectionString" password="{connection_str}";'
)

kafka_options = {
    "kafka.bootstrap.servers": f"{eventhub_namespace}.servicebus.windows.net:9093",
    "subscribe": eventhub_name,
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.jaas.config": jaas_config,
    "startingOffsets": "earliest",
    # Event Hub drops events after the retention period; don't fail the stream on that gap
    "failOnDataLoss": "false",
}


@dp.table(
    name=f"{catalog}.{bronze_schema}.ticks_bronze",
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
            F.lit("eventhub").alias("source"),
            F.current_timestamp().alias("ingestion_timestamp"),
            F.current_date().alias("load_date"),
        )
    )
