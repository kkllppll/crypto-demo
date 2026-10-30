dbutils.widgets.text("catalog", "dbr_dev_ua5816bd")
dbutils.widgets.text("bronze_schema", "team_crypto_bronze")
dbutils.widgets.text("silver_schema", "team_crypto_silver")
dbutils.widgets.text("gold_schema", "team_crypto_gold")

CATALOG = dbutils.widgets.get("catalog")
BRONZE = f"{CATALOG}.{dbutils.widgets.get('bronze_schema')}"
SILVER = f"{CATALOG}.{dbutils.widgets.get('silver_schema')}"
GOLD = f"{CATALOG}.{dbutils.widgets.get('gold_schema')}"

CHECKS = [
    (
        "bronze_not_empty", "error",
        f"SELECT COUNT(*) FROM {BRONZE}.ticks_bronze",
        lambda v: v > 0,
        "bronze.ticks_bronze has rows",
    ),
    (
        "silver_plus_quarantine_not_above_bronze", "error",
        f"""SELECT (SELECT COUNT(*) FROM {SILVER}.ticks_silver)
                 + (SELECT COUNT(*) FROM {SILVER}.ticks_quarantine)
                 - (SELECT COUNT(*) FROM {BRONZE}.ticks_bronze)""",
        lambda v: v <= 0,
        "silver + quarantine <= bronze (difference = dropped duplicates / rows still in flight)",
    ),
    (
        "silver_has_no_invalid_rows", "error",
        f"""SELECT COUNT(*) FROM {SILVER}.ticks_silver
            WHERE symbol IS NULL OR event_time IS NULL OR price_usd IS NULL
               OR isnan(price_usd) OR price_usd <= 0
               OR price_usd >= CAST('Infinity' AS DOUBLE)
               OR event_time > ingestion_timestamp + INTERVAL 5 MINUTES""",
        lambda v: v == 0,
        "no bad ticks leaked into silver",
    ),
    (
        "quarantine_always_has_reason", "error",
        f"""SELECT COUNT(*) FROM {SILVER}.ticks_quarantine
            WHERE failed_rules IS NULL OR size(failed_rules) = 0
               OR quarantine_reason IS NULL OR trim(quarantine_reason) = ''""",
        lambda v: v == 0,
        "every quarantined row explains why",
    ),
    (
        "silver_ticks_unique_key", "warn",
        f"""SELECT COUNT(*) FROM (
                SELECT 1 FROM {SILVER}.ticks_silver
                GROUP BY symbol, event_time, source HAVING COUNT(*) > 1)""",
        lambda v: v == 0,
        "no duplicate (symbol, event_time, source); dedup is only within the watermark, gold removes the rest",
    ),
    
    (
        "gold_ticks_match_silver", "error",
        f"""SELECT (SELECT COUNT(*) FROM {GOLD}.fact_price_tick)
                 - (SELECT COUNT(*) FROM (SELECT DISTINCT symbol, event_time FROM {SILVER}.ticks_silver))""",
        lambda v: v == 0,
        "fact_price_tick rows = distinct (symbol, event_time) in silver (non-zero = gold notebook not re-run)",
    ),
    (
        "gold_ohlc_match_silver", "error",
        f"""SELECT (SELECT COUNT(*) FROM {GOLD}.fact_ohlc_hourly)
                 - (SELECT COUNT(*) FROM (SELECT DISTINCT symbol, event_time
                                          FROM {SILVER}.ohlc_prices WHERE is_current))""",
        lambda v: v == 0,
        "fact_ohlc_hourly rows = distinct current candles in silver",
    ),
    
    (
        "ohlc_one_current_version_per_candle", "error",
        f"""SELECT COUNT(*) FROM (
                SELECT symbol, event_time FROM {SILVER}.ohlc_prices
                GROUP BY symbol, event_time
                HAVING SUM(CASE WHEN is_current THEN 1 ELSE 0 END) <> 1)""",
        lambda v: v == 0,
        "SCD2: exactly one is_current = true per (symbol, event_time)",
    ),
    (
        "bronze_fresh_within_60_min", "warn",
        f"""SELECT COALESCE(TIMESTAMPDIFF(MINUTE, MAX(ingestion_timestamp), current_timestamp()), 999999)
            FROM {BRONZE}.ticks_bronze""",
        lambda v: v <= 60,
        "minutes since the last row landed in bronze (producer alive?)",
    ),
]

results = []
for name, severity, sql, ok_fn, meaning in CHECKS:
    try:
        value = spark.sql(sql).collect()[0][0]
        passed = bool(ok_fn(value))
    except Exception as exc:  # a missing table or column is a failed check, not a crash
        value, passed = f"ERROR: {str(exc)[:120]}", False
    results.append((name, severity, "PASS" if passed else "FAIL", str(value), meaning))

report = spark.createDataFrame(results, ["check", "severity", "status", "value", "meaning"])
display(report)

failed_errors = [r for r in results if r[2] == "FAIL" and r[1] == "error"]
failed_warns = [r for r in results if r[2] == "FAIL" and r[1] == "warn"]

print(f"{len(results) - len(failed_errors) - len(failed_warns)} passed, "
      f"{len(failed_warns)} warnings, {len(failed_errors)} errors")

if failed_errors:
    raise AssertionError(
        "DQ reconciliation failed: " + "; ".join(f"{r[0]}={r[3]}" for r in failed_errors)
    )