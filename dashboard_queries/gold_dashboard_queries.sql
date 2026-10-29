-- ============================================================
-- Crypto Prices - Gold: dashboard datasets
-- Each dataset joins a gold fact with dimensions; global filters
-- come from dimensions (symbol, full_date, is_weekend).
-- Prices are non-additive (avg/min/max/last, never SUM).
-- volume (coins per hour) and volume_24h (USD, rolling) are different units.
-- ============================================================

-- Dataset: latest_prices  -> table "Latest price"
SELECT
  a.symbol,
  f.price_usd  AS latest_price_usd,
  f.event_time AS last_update
FROM dbr_dev_ua5816bd.team_crypto_gold.fact_price_tick f
JOIN dbr_dev_ua5816bd.team_crypto_gold.dim_asset a ON f.asset_key = a.asset_key
QUALIFY ROW_NUMBER() OVER (PARTITION BY a.symbol ORDER BY f.event_time DESC) = 1
ORDER BY a.symbol;

-- Dataset: hourly_candles -> "BTC/ETH/SOL - hourly close", "Volume by hour of day"
SELECT
  a.symbol,
  a.asset_name,
  d.full_date,
  d.day_name,
  d.is_weekend,
  s.provider,
  f.event_time,
  f.hour_of_day,
  f.open,
  f.high,
  f.low,
  f.close,
  f.volume,
  f.price_change_pct
FROM dbr_dev_ua5816bd.team_crypto_gold.fact_ohlc_hourly f
JOIN dbr_dev_ua5816bd.team_crypto_gold.dim_asset  a ON f.asset_key  = a.asset_key
JOIN dbr_dev_ua5816bd.team_crypto_gold.dim_date   d ON f.date_key   = d.date_key
JOIN dbr_dev_ua5816bd.team_crypto_gold.dim_source s ON f.source_key = s.source_key

-- Dataset: daily_summary -> "Daily change, %", "Average daily volatility, %"
SELECT
  a.symbol,
  d.full_date,
  d.day_name,
  d.is_weekend,
  min_by(f.open, f.event_time)                                        AS day_open,
  max_by(f.close, f.event_time)                                       AS day_close,
  MIN(f.low)                                                          AS day_low,
  MAX(f.high)                                                         AS day_high,
  ROUND((max_by(f.close, f.event_time) - min_by(f.open, f.event_time))
        / min_by(f.open, f.event_time) * 100, 2)                      AS daily_change_pct,
  ROUND(STDDEV(f.price_change_pct), 3)                                AS volatility_pct,
  SUM(f.volume)                                                       AS total_volume_coins
FROM dbr_dev_ua5816bd.team_crypto_gold.fact_ohlc_hourly f
JOIN dbr_dev_ua5816bd.team_crypto_gold.dim_asset a ON f.asset_key = a.asset_key
JOIN dbr_dev_ua5816bd.team_crypto_gold.dim_date  d ON f.date_key  = d.date_key
GROUP BY a.symbol, d.full_date, d.day_name, d.is_weekend

-- Dataset: live_ticks -> "BTC - live price (streaming)"
SELECT
  a.symbol,
  d.full_date,
  f.event_time,
  f.price_usd
FROM dbr_dev_ua5816bd.team_crypto_gold.fact_price_tick f
JOIN dbr_dev_ua5816bd.team_crypto_gold.dim_asset a ON f.asset_key = a.asset_key
JOIN dbr_dev_ua5816bd.team_crypto_gold.dim_date  d ON f.date_key  = d.date_key