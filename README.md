# Bybit BTCUSDT perpetual OHLCV collector

Continuously stores **closed 1-minute candles** for Bybit's `linear` BTCUSDT perpetual contract and displays them in a browser with a candlestick and volume chart. Uses the public V5 REST Kline endpoint; no exchange API key or trading permission is required. On restart, fetches all missing minutes before resuming. A primary key prevents duplicates. OHLC and volume values remain exact decimal strings in local SQLite and `NUMERIC` in PostgreSQL.

## Railway deployment

1. Push these files to a GitHub repository (at the repository root).
2. Create a Railway project, add a **PostgreSQL** database, and create a service from that repository.
3. Add a service variable `DATABASE_URL=${{Postgres.DATABASE_URL}}` (adjust `Postgres` to the database service's actual name). Do this **before its first deploy** or redeploy after setting it. Never put passwords in the repository.
4. Use **one replica**, with no cron schedule. Railway builds the Dockerfile and runs the worker and web server continuously. Disable sleep mode if enabled. The web server listens on Railway's `PORT`.
5. In Railway logs, look for `saved=...` and `caught up with last closed candle`. The first run defaults to the last 1,440 minutes. `INITIAL_LOOKBACK_MINUTES` changes only the first run for an empty table.
6. Generate a Railway service domain for the **collector service**, then open it in your browser. The dashboard offers 1, 6 and 24 hour views, EMA 20/50 overlays, a candle crosshair, drag-to-pan and wheel zoom. It refreshes every 30 seconds. `/api/candles?limit=360` returns read-only JSON for use by other tools. `/health` responds to the Railway health check.

If a database service is added later, note that any initial local SQLite data is ephemeral; the worker will re-fetch the configured initial range into PostgreSQL. The service intentionally refuses to run with local SQLite when `RAILWAY_ENVIRONMENT` is set.

## Local run

```bash
python3 dashboard.py
```

Open http://localhost:8080. The default local file is `ohlcv.sqlite3`. For PostgreSQL set `DATABASE_URL` to its connection URL and install `requirements.txt`. For a large historical first run use `INITIAL_LOOKBACK_MINUTES=10080` (one week). Backfill occurs in requests of up to 1,000 closed candles with a short pause between pages.

Example SQL:

```sql
SELECT start_ms, to_timestamp(start_ms / 1000.0) AS candle_utc,
       open, high, low, close, volume, turnover
FROM ohlcv WHERE symbol = 'BTCUSDT' AND interval = '1'
ORDER BY start_ms DESC LIMIT 10;
```

`volume` is BTC base-asset volume and `turnover` is USDT quote-asset turnover for a USDT linear contract. Times are UTC milliseconds. Bybit's latest forming candle is excluded; a three-second boundary buffer reduces race conditions. Network/API failures trigger exponential retry, and missing API rows are rejected so a partial page cannot silently advance the saved position.

The public dashboard displays public market data only; it never exposes database credentials or places trades. Since this same service serves the chart, its domain need not be attached to the PostgreSQL service.

Documentation: https://bybit-exchange.github.io/docs/v5/market/kline
