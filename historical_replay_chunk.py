"""Checkpointable read-only historical replay chunks.

Runs one symbol over a bounded historical window and carries per-symbol position
state across adjacent chunks. Uses the SAME main.analyze rules and conservative
backtest_execution.simulate execution model. No orders or account access.
"""
import asyncio, bisect, time
from datetime import datetime, timezone
import httpx
import main
from historical_coverage import series
from backtest_execution import simulate

BAR = 900_000
DAY = 86_400_000
WARMUP_DAYS = 40
LOOKAHEAD_DAYS = 1


def at_or_before(stamps, timestamp):
    return bisect.bisect_right(stamps, timestamp) - 1


def iso(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


async def replay_chunk(symbol, days=30, end_days_ago=1, initial_busy_until_ms=-1):
    if symbol not in ("BTCUSDT", "ETHUSDT"):
        raise ValueError("unsupported symbol")
    if not 1 <= int(days) <= 30:
        raise ValueError("days must be 1..30")
    if not 1 <= int(end_days_ago) <= 120:
        raise ValueError("end_days_ago must be 1..120")

    now_end = (int(time.time() * 1000) // BAR) * BAR
    signal_end = now_end - int(end_days_ago) * DAY
    start = signal_end - int(days) * DAY
    warmup = start - WARMUP_DAYS * DAY
    data_end = min(signal_end + LOOKAHEAD_DAYS * DAY, now_end)

    async with httpx.AsyncClient(timeout=30) as client:
        data = {}
        for kind in ("15", "60", "240", "oi", "funding"):
            fetch_start = warmup if kind in ("15", "60", "240") else start
            data[kind] = await series(client, symbol, kind, fetch_start, data_end)

    stamps = {kind: sorted(rows) for kind, rows in data.items()}
    checks = {}
    for kind, step in (("15", BAR), ("60", 3_600_000), ("240", 14_400_000), ("oi", BAR)):
        expected = range(((start + step - 1) // step) * step, signal_end, step)
        missing = sum(t not in data[kind] for t in expected)
        checks[kind] = {"missing": missing, "available": len(data[kind])}

    if any(v["missing"] for v in checks.values()):
        return {
            "symbol": symbol, "status": "BLOCKED_INCOMPLETE_DATA", "coverage": checks,
            "start_ms": start, "end_ms": signal_end, "start_iso": iso(start), "end_iso": iso(signal_end),
            "days": int(days), "end_days_ago": int(end_days_ago), "backtest_validated": False,
        }
    if not stamps["funding"]:
        return {
            "symbol": symbol, "status": "BLOCKED_NO_FUNDING", "coverage": checks,
            "start_ms": start, "end_ms": signal_end, "start_iso": iso(start), "end_iso": iso(signal_end),
            "days": int(days), "end_days_ago": int(end_days_ago), "backtest_validated": False,
        }

    candles = [
        {"start_ms": t, "open": float(v[1]), "high": float(v[2]), "low": float(v[3]), "close": float(v[4])}
        for t, v in sorted(data["15"].items()) if t >= start
    ]
    signals = []
    skipped_no_funding = 0
    skipped_warmup = 0

    original_get = main.get
    original_ind = main.ind
    ind_cache = {}

    def cached_ind(rows):
        # Exact memoization only: identical candle windows reuse main.ind output.
        key = (len(rows), str(rows[0][0]), str(rows[-1][0]))
        if key not in ind_cache:
            ind_cache[key] = original_ind(rows)
        return ind_cache[key]

    main.ind = cached_ind
    try:
        # signal_end is included because one full day of forward candles is loaded.
        for close_time in range(start + BAR, signal_end + 1, BAR):
            await asyncio.sleep(0)
            last_open = close_time - BAR
            candle_sets = {}
            for kind, step in (("15", BAR), ("60", 3_600_000), ("240", 14_400_000)):
                eligible = close_time - step
                idx = at_or_before(stamps[kind], eligible)
                if idx < 199:
                    break
                selected = stamps[kind][max(0, idx - 219):idx + 1]
                candle_sets[kind] = [data[kind][t] for t in reversed(selected)]
            if len(candle_sets) != 3:
                skipped_warmup += 1
                continue

            oi_idx = at_or_before(stamps["oi"], last_open)
            if oi_idx < 4:
                continue
            oi_rows = [data["oi"][t] for t in reversed(stamps["oi"][oi_idx - 4:oi_idx + 1])]
            fund_idx = at_or_before(stamps["funding"], close_time)
            if fund_idx < 0:
                skipped_no_funding += 1
                continue
            funding = float(data["funding"][stamps["funding"][fund_idx]]["fundingRate"])

            async def historical_get(_client, path, params=None):
                if path.endswith("/kline"):
                    return {"list": candle_sets[str(params["interval"])]}
                if path.endswith("/open-interest"):
                    return {"list": oi_rows}
                raise ValueError("Unexpected historical API request")

            main.get = historical_get
            try:
                result = await main.analyze(None, symbol, {symbol: funding})
            finally:
                main.get = original_get

            if result["side"] in ("LONG", "SHORT"):
                signals.append({
                    "symbol": symbol, "signal_time_ms": close_time - BAR,
                    "side": result["side"], "entry": result["entry"],
                    "stop": result["stop"], "tp1": result["tp1"], "score": result["score"],
                })
    finally:
        main.get = original_get
        main.ind = original_ind

    trades = []
    busy_until = int(initial_busy_until_ms)
    skipped_overlapping = 0
    for sig in signals:
        if sig["signal_time_ms"] < busy_until:
            skipped_overlapping += 1
            continue
        trade = simulate(sig, candles)
        trades.append(trade)
        if trade["status"] == "closed":
            busy_until = trade["exit_time_ms"] + BAR

    funding_events = [(t, float(data["funding"][t]["fundingRate"])) for t in stamps["funding"]]
    for trade in trades:
        if trade["status"] != "closed":
            continue
        opened = trade["signal_time_ms"] + BAR
        ended = trade["exit_time_ms"] + BAR
        direction = 1 if trade["side"] == "LONG" else -1
        funding_cost = direction * sum(rate for t, rate in funding_events if opened < t <= ended)
        trade["funding_return_cost"] = funding_cost
        trade["net_return_including_funding"] = trade["net_return_ex_funding"] - funding_cost
        trade["funding_included"] = True

    closed = [x for x in trades if x["status"] == "closed"]
    return {
        "symbol": symbol, "status": "REPLAYED", "coverage": checks,
        "start_ms": start, "end_ms": signal_end, "data_end_ms": data_end,
        "start_iso": iso(start), "end_iso": iso(signal_end),
        "days": int(days), "end_days_ago": int(end_days_ago),
        "strategy_rules": "main.analyze", "read_only": True,
        "funding_signal_used": True, "funding_cashflows_included": True,
        "funding_assumption": "constant_notional_approximation",
        "execution": "next_15m_open; 0.06% fee/side; 0.02% slippage/side; stop-first; 24h timeout",
        "backtest_validated": False, "signals": len(signals),
        "closed_trades": len(closed), "rejected": len(trades) - len(closed),
        "skipped_overlapping_signals": skipped_overlapping,
        "skipped_no_funding": skipped_no_funding, "skipped_warmup": skipped_warmup,
        "initial_busy_until_ms": int(initial_busy_until_ms), "final_busy_until_ms": busy_until,
        "mean_net_return_ex_funding": sum(x["net_return_ex_funding"] for x in closed) / len(closed) if closed else None,
        "win_rate_ex_funding": sum(x["net_return_ex_funding"] > 0 for x in closed) / len(closed) if closed else None,
        "win_rate_with_funding": sum(x["net_return_including_funding"] > 0 for x in closed) / len(closed) if closed else None,
        "mean_net_return_with_funding": sum(x["net_return_including_funding"] for x in closed) / len(closed) if closed else None,
        "trades": trades,
    }
