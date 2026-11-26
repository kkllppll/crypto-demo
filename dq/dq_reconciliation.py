from pathlib import Path
import argparse
import os
import yaml
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

parser = argparse.ArgumentParser()
parser.add_argument("--catalog", required=True)
parser.add_argument("--bronze_schema", required=True)
parser.add_argument("--silver_schema", required=True)
parser.add_argument("--gold_schema", required=True)
args, _ = parser.parse_known_args()

SCRIPT_PATH = Path(__file__).resolve()

PROJECT_ROOT = SCRIPT_PATH.parents[1]
CONFIG_PATH = PROJECT_ROOT / "config" / "config.yml"

with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

BRONZE = f"{args.catalog}.{args.bronze_schema}"
SILVER = f"{args.catalog}.{args.silver_schema}"
GOLD = f"{args.catalog}.{args.gold_schema}"

TICKS_BRONZE = f"{BRONZE}.{config['bronze']['eventhub_ticks']}"
TICKS_SILVER = f"{SILVER}.{config['silver']['ticks_table']}"
TICKS_QUARANTINE = f"{SILVER}.{config['silver']['quarantine_table']}"
OHLC_SILVER = f"{SILVER}.{config['silver']['table']}"
FACT_PRICE_TICK = f"{GOLD}.{config['gold']['tables']['fact_price_tick']}"
FACT_OHLC_HOURLY = f"{GOLD}.{config['gold']['tables']['fact_ohlc_hourly']}"

CHECKS = [
    (
        "bronze_not_empty", "error",
        f"SELECT COUNT(*) FROM {TICKS_BRONZE}",
        lambda v: v > 0,
        "bronze.ticks_bronze has rows",
    ),
    (
        "silver_plus_quarantine_not_above_bronze", "error",
        f"""SELECT (SELECT COUNT(*) FROM {TICKS_SILVER})
                 + (SELECT COUNT(*) FROM {TICKS_QUARANTINE})
                 - (SELECT COUNT(*) FROM {TICKS_BRONZE})""",
        lambda v: v <= 0,
        "silver + quarantine <= bronze (difference = dropped duplicates / rows still in flight)",
    ),
    (
        "silver_has_no_invalid_rows", "error",
        f"""SELECT COUNT(*) FROM {TICKS_SILVER}
            WHERE symbol IS NULL OR event_time IS NULL OR price_usd IS NULL
               OR isnan(price_usd) OR price_usd <= 0
               OR price_usd >= CAST('Infinity' AS DOUBLE)
               OR event_time > ingestion_timestamp + INTERVAL 5 MINUTES""",
        lambda v: v == 0,
        "no bad ticks leaked into silver",
    ),
    (
        "quarantine_always_has_reason", "error",
        f"""SELECT COUNT(*) FROM {TICKS_QUARANTINE}
            WHERE failed_rules IS NULL OR size(failed_rules) = 0
               OR quarantine_reason IS NULL OR trim(quarantine_reason) = ''""",
        lambda v: v == 0,
        "every quarantined row explains why",
    ),
    (
        "silver_ticks_unique_key", "warn",
        f"""SELECT COUNT(*) FROM (
                SELECT 1 FROM {TICKS_SILVER}
                GROUP BY symbol, event_time, source HAVING COUNT(*) > 1)""",
        lambda v: v == 0,
        "no duplicate (symbol, event_time, source); dedup is only within the watermark, gold removes the rest",
    ),
    
    (
        "gold_ticks_match_silver", "error",
        f"""SELECT (SELECT COUNT(*) FROM {FACT_PRICE_TICK})
                 - (SELECT COUNT(*) FROM (SELECT DISTINCT symbol, event_time FROM {TICKS_SILVER}))""",
        lambda v: v == 0,
        "fact_price_tick rows = distinct (symbol, event_time) in silver (non-zero = gold notebook not re-run)",
    ),
    (
        "gold_ohlc_match_silver", "error",
        f"""SELECT (SELECT COUNT(*) FROM {FACT_OHLC_HOURLY})
                 - (SELECT COUNT(*) FROM (SELECT DISTINCT symbol, event_time
                                          FROM {OHLC_SILVER} WHERE is_current))""",
        lambda v: v == 0,
        "fact_ohlc_hourly rows = distinct current candles in silver",
    ),
    
    (
        "ohlc_one_current_version_per_candle", "error",
        f"""SELECT COUNT(*) FROM (
                SELECT symbol, event_time FROM {OHLC_SILVER}
                GROUP BY symbol, event_time
                HAVING SUM(CASE WHEN is_current THEN 1 ELSE 0 END) <> 1)""",
        lambda v: v == 0,
        "SCD2: exactly one is_current = true per (symbol, event_time)",
    ),
    (
        "bronze_fresh_within_60_min", "warn",
        f"""SELECT COALESCE(TIMESTAMPDIFF(MINUTE, MAX(ingestion_timestamp), current_timestamp()), 999999)
            FROM {TICKS_BRONZE}""",
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
report.show(truncate=False)

failed_errors = [r for r in results if r[2] == "FAIL" and r[1] == "error"]
failed_warns = [r for r in results if r[2] == "FAIL" and r[1] == "warn"]

print(f"{len(results) - len(failed_errors) - len(failed_warns)} passed, "
      f"{len(failed_warns)} warnings, {len(failed_errors)} errors")

if failed_errors:
    raise AssertionError(
        "DQ reconciliation failed: " + "; ".join(f"{r[0]}={r[3]}" for r in failed_errors)
    )