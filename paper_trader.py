"""Forward-only, durable EMA crossover simulation; never sends exchange orders."""

import json
import logging
import time
import urllib.parse
import urllib.request
from decimal import Decimal

INITIAL_BALANCE = Decimal("1000")
NOTIONAL = Decimal("100")
FEE_RATE = Decimal("0.0006")  # Simulation assumption per side, not an exchange quote.
SLIPPAGE_RATE = Decimal("0.0002")  # 2 bps adverse fill per side; not measured order-book slippage.
FAST = Decimal(2) / Decimal(21)
SLOW = Decimal(2) / Decimal(51)
FUNDING_API = "https://api.bybit.com/v5/market/funding/history"
MARK_API = "https://api.bybit.com/v5/market/mark-price-kline"
LOG = logging.getLogger("paper")
LAST_FUNDING_POLL = 0.0
LAST_FUNDING_SUCCESS_MS = None
FUNDING_ERROR = False


def initialize(db, postgres):
    key = "BIGSERIAL PRIMARY KEY" if postgres else "INTEGER PRIMARY KEY AUTOINCREMENT"
    db.execute("""CREATE TABLE IF NOT EXISTS paper_state (
        id INTEGER PRIMARY KEY, last_ms BIGINT NOT NULL,
        fast TEXT NOT NULL, slow TEXT NOT NULL,
        balance TEXT NOT NULL, side TEXT, entry_price TEXT,
        entry_ms BIGINT, qty TEXT, entry_fee TEXT,
        started_at BIGINT, entry_reference TEXT, entry_slippage TEXT, open_funding TEXT)""")
    db.execute(f"""CREATE TABLE IF NOT EXISTS paper_trades (
        id {key}, side TEXT NOT NULL, entry_ms BIGINT NOT NULL,
        exit_ms BIGINT NOT NULL, entry_price TEXT NOT NULL,
        exit_price TEXT NOT NULL, qty TEXT NOT NULL, gross TEXT NOT NULL,
        fees TEXT NOT NULL, net TEXT NOT NULL,
        slippage TEXT, funding TEXT)""")
    db.execute("""CREATE TABLE IF NOT EXISTS paper_funding (
        event_ms BIGINT PRIMARY KEY, rate TEXT NOT NULL, reference_price TEXT NOT NULL,
        side TEXT, cashflow TEXT NOT NULL)""")
    # Existing forward paper accounts may have been initialized before costs were added.
    columns = {row[0] for row in db.execute("SELECT column_name FROM information_schema.columns "
                                              "WHERE table_name='paper_state'").fetchall()} if postgres else {
        row[1] for row in db.execute("PRAGMA table_info(paper_state)").fetchall()}
    for name, kind in (("started_at", "BIGINT"), ("entry_reference", "TEXT"),
                       ("entry_slippage", "TEXT"), ("open_funding", "TEXT")):
        if name not in columns:
            db.execute(f"ALTER TABLE paper_state ADD COLUMN {name} {kind}")
    columns = {row[0] for row in db.execute("SELECT column_name FROM information_schema.columns "
                                              "WHERE table_name='paper_trades'").fetchall()} if postgres else {
        row[1] for row in db.execute("PRAGMA table_info(paper_trades)").fetchall()}
    for name in ("slippage", "funding"):
        if name not in columns:
            db.execute(f"ALTER TABLE paper_trades ADD COLUMN {name} TEXT")
    db.execute("UPDATE paper_state SET started_at=last_ms+60000 WHERE started_at IS NULL")
    db.execute("UPDATE paper_state SET entry_reference=entry_price WHERE side IS NOT NULL AND entry_reference IS NULL")
    db.execute("UPDATE paper_state SET entry_slippage='0' WHERE entry_slippage IS NULL")
    db.execute("UPDATE paper_state SET open_funding='0' WHERE open_funding IS NULL")
    db.execute("UPDATE paper_trades SET slippage='0' WHERE slippage IS NULL")
    db.execute("UPDATE paper_trades SET funding='0' WHERE funding IS NULL")
    db.commit()


def funding_history(since_ms):
    """Fetch all settled funding events since the most recent accounted event."""
    events = []
    end = int(time.time() * 1000)
    for _ in range(20):
        query = urllib.parse.urlencode({"category": "linear", "symbol": "BTCUSDT", "endTime": end, "limit": 200})
        req = urllib.request.Request(f"{FUNDING_API}?{query}", headers={"User-Agent": "bybit-paper/1.0"})
        with urllib.request.urlopen(req, timeout=20) as response:
            payload = json.load(response)
        if payload.get("retCode") != 0:
            raise RuntimeError(f"Funding API error: {payload.get('retCode')} {payload.get('retMsg')}")
        result = payload.get("result", {})
        if result.get("category") != "linear":
            raise ValueError("Unexpected funding category")
        page = result.get("list", [])
        if not page:
            return events
        for row in page:
            if row.get("symbol") != "BTCUSDT":
                raise ValueError("Unexpected funding symbol")
            stamp = int(row["fundingRateTimestamp"])
            if stamp > since_ms:
                events.append((stamp, Decimal(row["fundingRate"])))
        oldest = min(int(row["fundingRateTimestamp"]) for row in page)
        if oldest <= since_ms or len(page) < 200:
            return sorted(set(events))
        end = oldest - 1
    raise RuntimeError("Funding history requires more than 20 pages; refusing incomplete accrual")


def mark_at(settlement_ms):
    """Last 1m mark-price close immediately before a funding settlement."""
    start = settlement_ms - 60000
    query = urllib.parse.urlencode({"category": "linear", "symbol": "BTCUSDT", "interval": "1",
                                    "start": start, "end": settlement_ms - 1, "limit": 1})
    req = urllib.request.Request(f"{MARK_API}?{query}", headers={"User-Agent": "bybit-paper/1.0"})
    with urllib.request.urlopen(req, timeout=20) as response:
        payload = json.load(response)
    result = payload.get("result", {})
    if payload.get("retCode") != 0 or result.get("category") != "linear" or result.get("symbol") != "BTCUSDT":
        raise ValueError("Invalid mark-price response")
    rows = result.get("list", [])
    if len(rows) != 1 or int(rows[0][0]) != start:
        raise ValueError("Missing pre-settlement mark candle")
    return Decimal(rows[0][4])


def update(store):
    """Run after the collector commits; restart resumes from last processed candle."""
    db = store.db
    postgres = store.postgres
    initialize(db, postgres)
    ph = "%s" if postgres else "?"
    state = db.execute("SELECT last_ms,fast,slow,balance,side,entry_price,entry_ms,qty,entry_fee,"
                       "started_at,entry_reference,entry_slippage,open_funding "
                       "FROM paper_state WHERE id=1").fetchone()
    if state is None:
        # Seed EMA from existing data. Do not mislabel past candles as forward paper trades.
        history = db.execute("SELECT start_ms,close FROM ohlcv WHERE symbol='BTCUSDT' AND interval='1' "
                             "ORDER BY start_ms DESC LIMIT 1440").fetchall()
        if not history:
            return
        fast = slow = None
        for _, close in reversed(history):
            price = Decimal(str(close))
            fast = price if fast is None else fast + FAST * (price - fast)
            slow = price if slow is None else slow + SLOW * (price - slow)
        try:
            db.execute(f"INSERT INTO paper_state (id,last_ms,fast,slow,balance,started_at,entry_slippage,open_funding) "
                       f"VALUES (1,{','.join([ph]*7)})",
                       (history[0][0], str(fast), str(slow), str(INITIAL_BALANCE),
                        history[0][0] + 60000, "0", "0"))
            db.commit()
        except Exception:
            db.rollback()
            raise
        return
    (last_ms, fast, slow, balance, side, entry_price, entry_ms, qty, entry_fee,
     started_at, entry_reference, entry_slippage, open_funding) = state
    fast, slow, balance = Decimal(fast), Decimal(slow), Decimal(balance)
    entry_price = Decimal(entry_price) if entry_price is not None else None
    qty = Decimal(qty) if qty is not None else None
    entry_fee = Decimal(entry_fee) if entry_fee is not None else None
    entry_reference = Decimal(entry_reference) if entry_reference is not None else None
    entry_slippage, open_funding = Decimal(entry_slippage), Decimal(open_funding)
    rows = db.execute(f"SELECT start_ms,close FROM ohlcv WHERE symbol='BTCUSDT' AND interval='1' "
                      f"AND start_ms>{ph} ORDER BY start_ms", (last_ms,)).fetchall()
    if rows:
      try:
        for ms, close in rows:
            price = Decimal(str(close))
            previous = fast - slow
            fast += FAST * (price - fast)
            slow += SLOW * (price - slow)
            new_side = "LONG" if previous <= 0 < fast - slow else "SHORT" if previous >= 0 > fast - slow else None
            if new_side and new_side != side:
                if side:
                    exit_price = price * (1 - SLIPPAGE_RATE if side == "LONG" else 1 + SLIPPAGE_RATE)
                    gross = (exit_price - entry_price) * qty * (1 if side == "LONG" else -1)
                    exit_fee = exit_price * qty * FEE_RATE
                    slippage = entry_slippage + abs(exit_price - price) * qty
                    net = gross - entry_fee - exit_fee + open_funding
                    balance += gross - exit_fee  # Entry fee was deducted on entry.
                    db.execute(f"""INSERT INTO paper_trades
                        (side,entry_ms,exit_ms,entry_price,exit_price,qty,gross,fees,net,slippage,funding)
                        VALUES ({','.join([ph]*11)})""",
                        (side, entry_ms, ms, str(entry_price), str(exit_price), str(qty),
                         str(gross), str(entry_fee + exit_fee), str(net),
                         str(slippage), str(open_funding)))
                side = new_side
                entry_reference, entry_ms = price, ms
                entry_price = price * (1 + SLIPPAGE_RATE if side == "LONG" else 1 - SLIPPAGE_RATE)
                qty = NOTIONAL / entry_price
                entry_fee = NOTIONAL * FEE_RATE
                entry_slippage = abs(entry_price - price) * qty
                open_funding = Decimal(0)
                balance -= entry_fee
            last_ms = ms
        db.execute(f"""UPDATE paper_state SET last_ms={ph},fast={ph},slow={ph},balance={ph},
                   side={ph},entry_price={ph},entry_ms={ph},qty={ph},entry_fee={ph},
                   entry_reference={ph},entry_slippage={ph},open_funding={ph} WHERE id=1""",
                   (last_ms, str(fast), str(slow), str(balance), side,
                    str(entry_price) if entry_price is not None else None, entry_ms,
                    str(qty) if qty is not None else None,
                    str(entry_fee) if entry_fee is not None else None,
                    str(entry_reference) if entry_reference is not None else None,
                    str(entry_slippage), str(open_funding)))
        db.commit()
      except Exception:
        db.rollback()
        raise
    poll_funding(store, started_at)


def poll_funding(store, started_at):
    global LAST_FUNDING_POLL, LAST_FUNDING_SUCCESS_MS, FUNDING_ERROR
    if LAST_FUNDING_POLL and time.monotonic() - LAST_FUNDING_POLL < 600:
        return
    db = store.db
    latest = db.execute("SELECT MAX(event_ms) FROM paper_funding").fetchone()[0]
    try:
        events = funding_history(max(started_at, latest or started_at))
        settle_funding(store, events)
        LAST_FUNDING_POLL = time.monotonic()
        LAST_FUNDING_SUCCESS_MS = int(time.time() * 1000)
        FUNDING_ERROR = False
    except Exception:
        FUNDING_ERROR = True
        # Keep OHLCV collection and paper signals running; retry settled rates later.
        LOG.exception("Funding history unavailable; will retry")


def settle_funding(store, events):
    """Reconcile late published settlements against the position held at each timestamp."""
    db = store.db
    ph = "%s" if store.postgres else "?"
    last_ms, started_at, balance, current_side, entry_ms, qty, open_funding = db.execute(
        "SELECT last_ms,started_at,balance,side,entry_ms,qty,open_funding "
        "FROM paper_state WHERE id=1").fetchone()
    balance = Decimal(balance)
    open_funding = Decimal(open_funding)
    try:
        for stamp, rate in sorted(events):
            if stamp <= started_at or stamp > last_ms + 60000:
                continue
            if db.execute(f"SELECT 1 FROM paper_funding WHERE event_ms={ph}", (stamp,)).fetchone():
                continue
            trade = db.execute(f"SELECT id,side,qty,net,funding FROM paper_trades "
                               f"WHERE entry_ms+60000<{ph} AND exit_ms+60000>={ph} "
                               "ORDER BY id DESC LIMIT 1", (stamp, stamp)).fetchone()
            side = trade[1] if trade else (current_side if current_side and entry_ms+60000 < stamp else None)
            position_qty = Decimal(trade[2]) if trade else Decimal(qty) if side else Decimal(0)
            # A closed mark candle is an approximation of the settlement's exact mark price.
            price = mark_at(stamp) if side else Decimal(0)
            flow = (-(price * position_qty * rate) * (1 if side == "LONG" else -1)
                    if side else Decimal(0))
            if trade:
                db.execute(f"UPDATE paper_trades SET net={ph},funding={ph} WHERE id={ph}",
                           (str(Decimal(trade[3]) + flow), str(Decimal(trade[4]) + flow), trade[0]))
            elif side:
                open_funding += flow
            balance += flow
            db.execute(f"INSERT INTO paper_funding (event_ms,rate,reference_price,side,cashflow) "
                       f"VALUES ({','.join([ph]*5)})",
                       (stamp, str(rate), str(price), side, str(flow)))
        db.execute(f"UPDATE paper_state SET balance={ph},open_funding={ph} WHERE id=1",
                   (str(balance), str(open_funding)))
        db.commit()
    except Exception:
        db.rollback()
        raise


def snapshot(db):
    state = db.execute("SELECT last_ms,balance,side,entry_price,entry_ms,qty,open_funding,entry_slippage "
                       "FROM paper_state WHERE id=1").fetchone()
    if not state:
        return {"ready": False, "trades": []}
    rows = db.execute("SELECT side,entry_ms,exit_ms,entry_price,exit_price,gross,fees,net,slippage,funding "
                      "FROM paper_trades ORDER BY id DESC LIMIT 30").fetchall()
    total = db.execute("SELECT COUNT(*) FROM paper_trades").fetchone()[0]
    funding_total = db.execute("SELECT COALESCE(SUM(CAST(cashflow AS NUMERIC)),0) FROM paper_funding").fetchone()[0]
    funding_count = db.execute("SELECT COUNT(*) FROM paper_funding WHERE side IS NOT NULL").fetchone()[0]
    return {
        "ready": True, "mode": "paper", "strategy": "EMA 20/50 crossing on closed 1m candles",
        "initialBalance": str(INITIAL_BALANCE), "notional": str(NOTIONAL),
        "feeRate": str(FEE_RATE), "slippageRate": str(SLIPPAGE_RATE),
        "fundingCashflow": str(funding_total), "fundingCount": funding_count,
        "fundingLastCheck": LAST_FUNDING_SUCCESS_MS, "fundingError": FUNDING_ERROR,
        "lastProcessed": int(state[0]),
        "balance": str(state[1]),
        "closedTradeCount": total,
        "position": ({"side": state[2], "entryPrice": state[3], "entryTime": int(state[4]),
                      "qty": state[5], "funding": state[6], "entrySlippage": state[7]}
                     if state[2] else None),
        "trades": [{"side": r[0], "entryTime": int(r[1]), "exitTime": int(r[2]),
                    "entryPrice": r[3], "exitPrice": r[4], "gross": r[5],
                    "fees": r[6], "net": r[7], "slippage": r[8], "funding": r[9]} for r in rows],
    }
