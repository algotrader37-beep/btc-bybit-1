"""Durable Bybit BTCUSDT linear perpetual OHLCV collector."""

import json
import logging
import os
import signal
import sqlite3
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

LOG = logging.getLogger("ohlcv")
STOP = False
SYMBOL = "BTCUSDT"
CATEGORY = "linear"
INTERVAL = "1"
INTERVAL_MS = 60_000
PAGE_SIZE = 1000
API = "https://api.bybit.com/v5/market/kline"


def utc(ms):
    return datetime.fromtimestamp(ms / 1000, timezone.utc).isoformat()


def request_candles(start, end):
    query = urllib.parse.urlencode({
        "category": CATEGORY, "symbol": SYMBOL, "interval": INTERVAL,
        "start": start, "end": end, "limit": PAGE_SIZE,
    })
    req = urllib.request.Request(f"{API}?{query}", headers={"User-Agent": "bybit-ohlcv-collector/1.0"})
    with urllib.request.urlopen(req, timeout=20) as response:
        payload = json.load(response)
    if payload.get("retCode") != 0:
        raise RuntimeError(f"Bybit API error: {payload.get('retCode')} {payload.get('retMsg')}")
    result = payload.get("result", {})
    if result.get("symbol") != SYMBOL or result.get("category") != CATEGORY:
        raise ValueError("Unexpected Bybit symbol/category in response")
    return result.get("list", [])


class Store:
    def __init__(self, database_url):
        self.postgres = database_url.startswith(("postgres://", "postgresql://"))
        if self.postgres:
            import psycopg
            self.db = psycopg.connect(database_url, autocommit=False)
        else:
            if database_url != "sqlite:///ohlcv.sqlite3":
                raise ValueError("DATABASE_URL must be a PostgreSQL URL or sqlite:///ohlcv.sqlite3")
            self.db = sqlite3.connect("ohlcv.sqlite3")
        numeric = "NUMERIC" if self.postgres else "TEXT"
        self.db.execute(f"""CREATE TABLE IF NOT EXISTS ohlcv (
            symbol TEXT NOT NULL, interval TEXT NOT NULL, start_ms BIGINT NOT NULL,
            open {numeric} NOT NULL, high {numeric} NOT NULL, low {numeric} NOT NULL,
            close {numeric} NOT NULL, volume {numeric} NOT NULL, turnover {numeric} NOT NULL,
            PRIMARY KEY(symbol, interval, start_ms))""")
        self.db.commit()

    def latest(self):
        placeholder = "%s" if self.postgres else "?"
        row = self.db.execute(
            f"SELECT MAX(start_ms) FROM ohlcv WHERE symbol={placeholder} AND interval={placeholder}",
            (SYMBOL, INTERVAL),
        ).fetchone()
        return row[0]

    def insert(self, rows):
        placeholder = "%s" if self.postgres else "?"
        sql = ("INSERT INTO ohlcv (symbol, interval, start_ms, open, high, low, close, volume, turnover) "
               f"VALUES ({', '.join([placeholder] * 9)}) ON CONFLICT (symbol, interval, start_ms) DO NOTHING")
        try:
            if self.postgres:
                with self.db.cursor() as cursor:
                    cursor.executemany(sql, rows)
            else:
                self.db.executemany(sql, rows)
            self.db.commit()
        except Exception:
            self.db.rollback()
            raise

    def close(self):
        self.db.close()


def normalize(raw, start, end):
    candles = {}
    for entry in raw:
        if len(entry) != 7:
            raise ValueError("Malformed candle")
        timestamp = int(entry[0])
        if start <= timestamp <= end and timestamp % INTERVAL_MS == 0:
            candles[timestamp] = (SYMBOL, INTERVAL, timestamp, *entry[1:])
    expected = list(range(start, end + 1, INTERVAL_MS))
    if set(candles) != set(expected):
        missing = sorted(set(expected) - set(candles))
        raise ValueError(f"Missing {len(missing)} candle(s), first={utc(missing[0]) if missing else 'none'}")
    return [candles[timestamp] for timestamp in expected]


def collect_once(store, now_ms, lookback_minutes):
    # Delay slightly after the minute boundary; do not store a forming candle.
    last_closed = ((now_ms - 3_000) // INTERVAL_MS - 1) * INTERVAL_MS
    latest = store.latest()
    start = latest + INTERVAL_MS if latest is not None else last_closed - (lookback_minutes - 1) * INTERVAL_MS
    total = 0
    while start <= last_closed:
        end = min(start + (PAGE_SIZE - 1) * INTERVAL_MS, last_closed)
        rows = normalize(request_candles(start, end), start, end)
        store.insert(rows)
        total += len(rows)
        LOG.info("saved=%d range=%s..%s", len(rows), utc(start), utc(end))
        start = end + INTERVAL_MS
        if start <= last_closed:
            time.sleep(0.25)
    return total


def handle_stop(signum, frame):
    global STOP
    STOP = True


def main():
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
    signal.signal(signal.SIGTERM, handle_stop)
    signal.signal(signal.SIGINT, handle_stop)
    lookback = int(os.getenv("INITIAL_LOOKBACK_MINUTES", "1440"))
    if lookback < 1:
        raise ValueError("INITIAL_LOOKBACK_MINUTES must be positive")
    url = os.getenv("DATABASE_URL", "sqlite:///ohlcv.sqlite3")
    if os.getenv("RAILWAY_ENVIRONMENT") and not url.startswith(("postgres://", "postgresql://")):
        raise ValueError("Railway requires DATABASE_URL pointing to persistent PostgreSQL")
    delay = 5
    while not STOP:
        store = None
        try:
            store = Store(url)
            count = collect_once(store, int(time.time() * 1000), lookback)
            delay = 5
            if count:
                LOG.info("caught up with last closed candle")
            # Wake shortly after the next UTC minute boundary.
            wait = max(1, 63 - time.time() % 60)
        except Exception as error:
            # Bybit advises waiting at least 10 minutes after an IP-rate-limit 403.
            if isinstance(error, urllib.error.HTTPError) and error.code == 403:
                wait = 600
                LOG.exception("Bybit HTTP 403; pausing for 10 minutes")
            else:
                LOG.exception("collection failed, retrying in %ds: %s", delay, error)
                wait = delay
                delay = min(delay * 2, 300)
        finally:
            if store:
                store.close()
        until = time.monotonic() + wait
        while not STOP and time.monotonic() < until:
            time.sleep(min(1, until - time.monotonic()))


if __name__ == "__main__":
    main()
