"""Public Bybit ticker and forming 1m candle, cached for the dashboard."""

import json
import logging
import threading
import time

import websocket

LOG = logging.getLogger("livefeed")
URL = "wss://stream.bybit.com/v5/public/linear"
TOPICS = ["tickers.BTCUSDT", "kline.1.BTCUSDT"]


class LiveFeed:
    def __init__(self):
        self.lock = threading.Lock()
        self.price = None
        self.price_ts = None
        self.forming = None
        self.confirmed_start = None
        self.last_message_at = 0.0

    def ingest(self, message):
        topic = message.get("topic")
        data = message.get("data")
        if topic not in TOPICS or data is None:
            return
        ts = int(message.get("ts") or time.time() * 1000)
        with self.lock:
            self.last_message_at = time.monotonic()
            if topic == TOPICS[0]:
                if isinstance(data, dict) and data.get("symbol") in (None, "BTCUSDT") and data.get("lastPrice"):
                    self.price = str(data["lastPrice"])
                    self.price_ts = ts
            elif isinstance(data, list):
                for candle in data:
                    if str(candle.get("interval")) != "1":
                        continue
                    start = int(candle["start"])
                    if candle.get("confirm") is True:
                        self.confirmed_start = start
                        if self.forming and self.forming["time"] <= start:
                            self.forming = None
                    elif not self.forming or start >= self.forming["time"]:
                        self.forming = {
                            "time": start, "open": str(candle["open"]),
                            "high": str(candle["high"]), "low": str(candle["low"]),
                            "close": str(candle["close"]), "volume": str(candle["volume"]),
                            "turnover": str(candle["turnover"]), "provisional": True,
                        }

    def snapshot(self):
        with self.lock:
            connected = bool(self.last_message_at and time.monotonic() - self.last_message_at < 5)
            # Never present cached values as live after a disconnected stream.
            return {
                "connected": connected,
                "price": self.price if connected else None,
                "priceTs": self.price_ts if connected else None,
                "forming": dict(self.forming) if connected and self.forming else None,
                "confirmedStart": self.confirmed_start,
            }

    def run(self):
        delay = 2
        while True:
            connection = None
            try:
                connection = websocket.create_connection(URL, timeout=12)
                connection.settimeout(25)
                connection.send(json.dumps({"op": "subscribe", "args": TOPICS}))
                LOG.info("Bybit public stream connected")
                delay = 2
                last_ping = time.monotonic()
                while True:
                    if time.monotonic() - last_ping > 20:
                        connection.send(json.dumps({"op": "ping"}))
                        last_ping = time.monotonic()
                    try:
                        payload = connection.recv()
                    except websocket.WebSocketTimeoutException:
                        continue
                    if not payload:
                        raise ConnectionError("Bybit stream closed")
                    self.ingest(json.loads(payload))
            except Exception as error:
                LOG.warning("Bybit stream interrupted: %s; retry in %ds", error, delay)
                with self.lock:
                    self.last_message_at = 0.0
                    self.forming = None
                time.sleep(delay)
                delay = min(delay * 2, 60)
            finally:
                if connection:
                    connection.close()
