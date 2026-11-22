# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Unity Catalog functions used as agent tools
# MAGIC
# MAGIC The agent never writes SQL. It can only call these read-only SQL table functions with typed
# MAGIC parameters, so there is no prompt-to-SQL injection surface and every tool is governed,
# MAGIC versioned and discoverable in Unity Catalog like any other asset.
# MAGIC
# MAGIC | function | answers |
# MAGIC |---|---|
# MAGIC | `get_latest_prices()` | latest streaming price per asset |
# MAGIC | `get_daily_summary(symbol, days_back)` | daily open / close / range / change / volatility |
# MAGIC | `get_largest_hourly_moves(symbol, top_n)` | biggest hourly candles by abs % change |
# MAGIC | `get_data_quality_summary(days_back)` | valid vs quarantined ticks per day and reason |
# MAGIC | `get_visible_symbols()` | symbols the caller can see (used to filter retrieval) |
# MAGIC
# MAGIC **RLS / CLS still apply**: the functions read the gold facts that carry `rls_asset_filter` and
# MAGIC `cls_mask_volume`; filters and masks are evaluated for `current_user()`, i.e. the end user when the
# MAGIC agent runs with on-behalf-of-user auth.
# MAGIC
# MAGIC "Last N days" means the N most recent days **that have data**, not calendar days back from today,
# MAGIC so answers stay meaningful when the batch load is a few days old.

# COMMAND ----------

import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), "..", ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from ai_assistant.utilities.settings import load_settings

S = load_settings(dbutils, REPO_ROOT)
GOLD, SILVER, AI = S.gold, S.silver, S.ai
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {AI} COMMENT 'AI assistant: knowledge base, vector index, agent tools and model'")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {AI}.get_latest_prices()
RETURNS TABLE (symbol STRING, asset_name STRING, price_usd DOUBLE, event_time TIMESTAMP)
COMMENT 'Latest streaming price in USD per asset visible to the caller (gold fact_price_tick, row filter applies).'
RETURN
    SELECT a.symbol, a.asset_name, f.price_usd, f.event_time
    FROM {GOLD}.fact_price_tick f
    JOIN {GOLD}.dim_asset a ON f.asset_key = a.asset_key
    QUALIFY ROW_NUMBER() OVER (PARTITION BY a.symbol ORDER BY f.event_time DESC) = 1
""")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {AI}.get_daily_summary(
    asset_symbol STRING COMMENT 'Asset symbol, e.g. BTC',
    days_back    INT    DEFAULT 7 COMMENT 'Number of most recent days with data'
)
RETURNS TABLE (
    symbol STRING, full_date DATE, day_name STRING,
    day_open DOUBLE, day_close DOUBLE, day_low DOUBLE, day_high DOUBLE,
    daily_change_pct DOUBLE, volatility_pct DOUBLE, hourly_candles BIGINT
)
COMMENT 'Daily OHLC summary of one asset from hourly Kraken candles (gold fact_ohlc_hourly), most recent day first.'
RETURN
    WITH daily AS (
        SELECT
            a.symbol,
            d.full_date,
            d.day_name,
            min_by(f.open, f.event_time)  AS day_open,
            max_by(f.close, f.event_time) AS day_close,
            MIN(f.low)                    AS day_low,
            MAX(f.high)                   AS day_high,
            ROUND(STDDEV(f.price_change_pct), 3) AS volatility_pct,
            COUNT(*)                      AS hourly_candles
        FROM {GOLD}.fact_ohlc_hourly f
        JOIN {GOLD}.dim_asset a ON f.asset_key = a.asset_key
        JOIN {GOLD}.dim_date  d ON f.date_key  = d.date_key
        WHERE a.symbol = upper(asset_symbol)
        GROUP BY a.symbol, d.full_date, d.day_name
    )
    SELECT symbol, full_date, day_name, day_open, day_close, day_low, day_high,
           ROUND((day_close - day_open) / NULLIF(day_open, 0) * 100, 2) AS daily_change_pct,
           volatility_pct, hourly_candles
    FROM daily
    QUALIFY DENSE_RANK() OVER (ORDER BY full_date DESC) <= days_back
    ORDER BY full_date DESC
""")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {AI}.get_largest_hourly_moves(
    asset_symbol STRING COMMENT 'Asset symbol, e.g. ETH',
    top_n        INT    DEFAULT 5 COMMENT 'How many candles to return'
)
RETURNS TABLE (symbol STRING, event_time TIMESTAMP, open DOUBLE, close DOUBLE, price_change_pct DOUBLE)
COMMENT 'Hourly candles of one asset with the largest absolute % change (gold fact_ohlc_hourly).'
RETURN
    SELECT a.symbol, f.event_time, f.open, f.close, f.price_change_pct
    FROM {GOLD}.fact_ohlc_hourly f
    JOIN {GOLD}.dim_asset a ON f.asset_key = a.asset_key
    WHERE a.symbol = upper(asset_symbol) AND f.price_change_pct IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (ORDER BY abs(f.price_change_pct) DESC, f.event_time) <= top_n
    ORDER BY abs(f.price_change_pct) DESC
""")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {AI}.get_data_quality_summary(
    days_back INT DEFAULT 7 COMMENT 'Number of most recent load dates'
)
RETURNS TABLE (load_date DATE, status STRING, reason STRING, ticks BIGINT)
COMMENT 'Streaming data quality per load date: valid ticks in silver vs quarantined ticks by rejection reason.'
RETURN
    WITH dq AS (
        SELECT load_date, 'valid' AS status, CAST(NULL AS STRING) AS reason, COUNT(*) AS ticks
        FROM {SILVER}.ticks_silver
        GROUP BY load_date
        UNION ALL
        SELECT load_date, 'quarantined', quarantine_reason, COUNT(*)
        FROM {SILVER}.ticks_quarantine
        GROUP BY load_date, quarantine_reason
    )
    SELECT load_date, status, reason, ticks
    FROM dq
    QUALIFY DENSE_RANK() OVER (ORDER BY load_date DESC) <= days_back
    ORDER BY load_date DESC, status DESC, ticks DESC
""")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {AI}.get_visible_symbols()
RETURNS TABLE (symbol STRING)
COMMENT 'Symbols visible to the caller in gold facts after the row filter; the agent restricts retrieval to these.'
RETURN
    SELECT DISTINCT a.symbol
    FROM (
        SELECT asset_key FROM {GOLD}.fact_ohlc_hourly
        UNION
        SELECT asset_key FROM {GOLD}.fact_price_tick
    ) f
    JOIN {GOLD}.dim_asset a ON f.asset_key = a.asset_key
""")

# COMMAND ----------

# MAGIC %md ### Grants
# MAGIC Readers may execute the tools and query the index; they get no access to the raw `kb_documents`
# MAGIC table or to `access_rules`.

# COMMAND ----------

spark.sql(f"GRANT USE SCHEMA ON SCHEMA {AI} TO `{S.reader_group}`")
for fn in ["get_latest_prices", "get_daily_summary", "get_largest_hourly_moves",
           "get_data_quality_summary", "get_visible_symbols"]:
    spark.sql(f"GRANT EXECUTE ON FUNCTION {AI}.{fn} TO `{S.reader_group}`")
# SELECT on the vector index is granted in 02, once the index exists.

# COMMAND ----------

# MAGIC %md ### Job identity must see all assets
# MAGIC `01_build_knowledge_base` reads row-filtered gold facts. The identity that runs the job (a service
# MAGIC principal in PROD) is usually not in the reader group, so without a personal rule it would see no rows
# MAGIC and the index would have no market narratives. This adds an explicit, auditable rule for it.

# COMMAND ----------

me = spark.sql("SELECT current_user()").first()[0]
access_rules = f"{GOLD}.access_rules"
if spark.catalog.tableExists(access_rules):
    spark.sql(f"DELETE FROM {access_rules} WHERE user_email = '{me}'")
    spark.sql(f"INSERT INTO {access_rules} (user_email, symbol, can_see_volume) VALUES ('{me}', NULL, TRUE)")
    print(f"access rule set for job identity {me}")
else:
    print(f"{access_rules} not found - run gold_pipeline/05_governance first")

# COMMAND ----------

# MAGIC %md ### Smoke test (as the current identity)

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {AI}.get_latest_prices()"))
display(spark.sql(f"SELECT * FROM {AI}.get_daily_summary('BTC', 3)"))
display(spark.sql(f"SELECT * FROM {AI}.get_data_quality_summary(3)"))
