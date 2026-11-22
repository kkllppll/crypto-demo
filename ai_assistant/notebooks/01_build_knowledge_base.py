# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Knowledge base for the AI assistant
# MAGIC
# MAGIC Builds `<ai_schema>.kb_documents`, the source table of the vector index:
# MAGIC
# MAGIC | source_type | from | symbol |
# MAGIC |---|---|---|
# MAGIC | `market_daily` | gold `fact_ohlc_hourly` + `dim_asset`: one narrative per asset per day | BTC / ETH / ... |
# MAGIC | `project_doc` | `README.md` and `ai_assistant/README.md`, split by headings | `*` |
# MAGIC | `data_dictionary` | `information_schema` comments of gold and silver tables | `*` |
# MAGIC
# MAGIC **Idempotent**: `doc_id` is derived from what identifies a document (asset + day, file + section),
# MAGIC not from its content. The MERGE updates changed documents, inserts new ones and deletes documents
# MAGIC that no longer exist, so a rerun without new data changes nothing and the index sync is a no-op.
# MAGIC Change Data Feed is on, which a Delta Sync vector index requires.
# MAGIC
# MAGIC **Governance**: narratives are built from row-filtered gold facts, so the identity running this job
# MAGIC must see all assets (see `03_create_uc_tools`). Per-user filtering happens at retrieval time:
# MAGIC the agent only asks the index for symbols the user can see in gold.

# COMMAND ----------

import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), "..", ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from ai_assistant.utilities.kb_builder import (
    KB_COLUMNS,
    daily_market_stats,
    data_dictionary_documents,
    market_daily_documents,
    markdown_documents,
)
from ai_assistant.utilities.settings import load_settings

S = load_settings(dbutils, REPO_ROOT)
spark.conf.set("spark.sql.session.timeZone", "UTC")
print(f"catalog={S.catalog} gold={S.gold} kb_table={S.kb_table}")

# COMMAND ----------

# MAGIC %md ### Schema and table

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {S.ai} COMMENT 'AI assistant: knowledge base, vector index, agent tools and model'")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {S.kb_table} (
    doc_id      STRING    NOT NULL COMMENT 'Stable id: hash of what identifies the document',
    source_type STRING    NOT NULL COMMENT 'market_daily / project_doc / data_dictionary',
    symbol      STRING    NOT NULL COMMENT 'Asset of a market narrative; * for project-wide documents',
    title       STRING,
    content     STRING    NOT NULL COMMENT 'Text that is embedded',
    source_ref  STRING    COMMENT 'Where the text came from (file, table, fact/symbol/date)',
    updated_at  TIMESTAMP,
    CONSTRAINT pk_kb_documents PRIMARY KEY (doc_id)
)
COMMENT 'Documents for the crypto assistant vector index'
TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

# COMMAND ----------

# MAGIC %md ### Market narratives from gold

# COMMAND ----------

fact_ohlc = spark.table(f"{S.gold}.fact_ohlc_hourly")
dim_asset = spark.table(f"{S.gold}.dim_asset")
market_docs = market_daily_documents(daily_market_stats(fact_ohlc, dim_asset))

# COMMAND ----------

# MAGIC %md ### Project docs and data dictionary

# COMMAND ----------

doc_rows = []
for rel_path, title in [("README.md", "Project README"), ("ai_assistant/README.md", "AI assistant README")]:
    path = os.path.join(REPO_ROOT, rel_path)
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            doc_rows += markdown_documents(f.read(), rel_path, title)
    else:
        print(f"skip missing {rel_path}")

schemas = [S.gold_schema, S.silver_schema]
in_list = ", ".join(f"'{s}'" for s in schemas)
tables = [r.asDict() for r in spark.sql(f"""
    SELECT table_schema, table_name, comment
    FROM {S.catalog}.information_schema.tables
    WHERE table_schema IN ({in_list}) AND table_name NOT LIKE '\\_%' AND table_name <> 'access_rules'
""").collect()]
columns = [r.asDict() for r in spark.sql(f"""
    SELECT table_schema, table_name, column_name, data_type, comment, ordinal_position
    FROM {S.catalog}.information_schema.columns
    WHERE table_schema IN ({in_list})
""").collect()]
doc_rows += data_dictionary_documents(tables, columns)

static_docs = spark.createDataFrame(doc_rows, schema=", ".join(f"{c} string" for c in KB_COLUMNS))

# COMMAND ----------

# MAGIC %md ### Quality checks, then MERGE

# COMMAND ----------

from pyspark.sql import functions as F

source = market_docs.unionByName(static_docs).withColumn("updated_at", F.current_timestamp())
source = source.cache()

total = source.count()
duplicates = total - source.select("doc_id").distinct().count()
empty = source.where(F.col("content").isNull() | (F.length(F.trim("content")) == 0)).count()
n_market = source.where("source_type = 'market_daily'").count()

problems = []
if duplicates:
    problems.append(f"{duplicates} duplicate doc_id")
if empty:
    problems.append(f"{empty} empty documents")
if total == 0:
    problems.append("no documents at all")
if problems:
    raise ValueError("knowledge base quality gate failed: " + "; ".join(problems))
if n_market == 0:
    print("WARNING: no market narratives - gold fact_ohlc_hourly is empty or not visible to this identity")

source.createOrReplaceTempView("kb_src")
result = spark.sql(f"""
MERGE INTO {S.kb_table} t
USING kb_src s
ON t.doc_id = s.doc_id
WHEN MATCHED AND (t.content <> s.content OR t.title <> s.title OR t.symbol <> s.symbol) THEN UPDATE SET
    source_type = s.source_type, symbol = s.symbol, title = s.title,
    content = s.content, source_ref = s.source_ref, updated_at = s.updated_at
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE
""")
display(result)

# COMMAND ----------

display(spark.sql(f"SELECT source_type, count(*) AS docs FROM {S.kb_table} GROUP BY source_type ORDER BY source_type"))
