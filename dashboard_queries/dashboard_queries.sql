-- ============================================
-- Crypto Prices Dashboard — SQL queries
-- ============================================

-- Latest Prices (Batch)
SELECT
  symbol,
  close AS latest_batch_price,
  event_time AS last_batch_update
FROM dbr_dev_ua5816bd.team_crypto_silver.unified_prices
WHERE record_type = 'ohlc_batch'
  AND close IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY event_time DESC) = 1
ORDER BY symbol

-- Latest Prices (Streaming)
SELECT
  symbol,
  price_usd AS latest_streaming_price,
  event_time AS last_streaming_update
FROM dbr_dev_ua5816bd.team_crypto_silver.unified_prices
WHERE record_type = 'streaming_tick'
  AND price_usd IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY event_time DESC) = 1
ORDER BY symbol

-- Price Movement (Batch)
SELECT
  symbol,
  event_time,
  close AS batch_price
FROM dbr_dev_ua5816bd.team_crypto_silver.unified_prices
WHERE record_type = 'ohlc_batch'
  AND close IS NOT NULL
ORDER BY event_time

-- Price Movement (Streaming)
SELECT
  symbol,
  event_time,
  price_usd AS streaming_price
FROM dbr_dev_ua5816bd.team_crypto_silver.unified_prices
WHERE record_type = 'streaming_tick'
  AND price_usd IS NOT NULL
ORDER BY event_time
