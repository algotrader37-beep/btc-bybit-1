"""Small read-only web UI backed by the collector's PostgreSQL table."""

import json
import logging
import os
import sqlite3
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import collector
from livefeed import LiveFeed

PAGE = Path(__file__).with_name("dashboard.html").read_bytes()
LOG = logging.getLogger("dashboard")
FEED = LiveFeed()


def recent_candles(limit):
    url = os.getenv("DATABASE_URL", "sqlite:///ohlcv.sqlite3")
    if url.startswith(("postgres://", "postgresql://")):
        import psycopg
        connection = psycopg.connect(url, connect_timeout=5)
        placeholder = "%s"
    else:
        if url != "sqlite:///ohlcv.sqlite3":
            raise ValueError("Invalid database URL")
        connection = sqlite3.connect("ohlcv.sqlite3", timeout=5)
        placeholder = "?"
    try:
        rows = connection.execute(
            "SELECT start_ms, open, high, low, close, volume, turnover "
            f"FROM ohlcv WHERE symbol={placeholder} AND interval={placeholder} "
            f"ORDER BY start_ms DESC LIMIT {placeholder}",
            (collector.SYMBOL, collector.INTERVAL, limit),
        ).fetchall()
        return [dict(zip(("time", "open", "high", "low", "close", "volume", "turnover"),
                         (int(row[0]), *(str(value) for value in row[1:])))) for row in reversed(rows)]
    finally:
        connection.close()


class Handler(BaseHTTPRequestHandler):
    def reply(self, status, body, content_type):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        route = urlsplit(self.path)
        if route.path == "/":
            return self.reply(200, PAGE, "text/html; charset=utf-8")
        if route.path == "/health":
            return self.reply(200, b"ok", "text/plain; charset=utf-8")
        if route.path == "/api/live":
            return self.reply(200, json.dumps(FEED.snapshot()).encode(), "application/json; charset=utf-8")
        if route.path != "/api/candles":
            return self.reply(404, b"Not found", "text/plain; charset=utf-8")
        try:
            limit = int(parse_qs(route.query).get("limit", ["360"])[0])
            if not 1 <= limit <= 1440:
                raise ValueError("limit out of range")
        except ValueError:
            return self.reply(400, b'{"error":"limit must be 1..1440"}', "application/json")
        try:
            body = json.dumps({"symbol": collector.SYMBOL, "interval": "1m",
                               "candles": recent_candles(limit)}).encode()
        except Exception:
            LOG.exception("Failed to query OHLCV")
            return self.reply(503, b'{"error":"Data temporarily unavailable"}', "application/json")
        return self.reply(200, body, "application/json; charset=utf-8")

    def log_message(self, format, *args):
        if not (args and str(args[0]).startswith("GET /health")):
            LOG.info(format, *args)


def main():
    port = int(os.getenv("PORT", "8080"))
    threading.Thread(target=FEED.run, daemon=True).start()
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    LOG.info("dashboard listening on port %d", port)
    try:
        collector.main()
    finally:
        server.shutdown()
        server.server_close()


if __name__ == "__main__":
    main()
