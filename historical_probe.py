"""Historical-data feasibility probe for full-fidelity Bybit backtests.

No orders. Do not treat passing this probe as a backtest or proof of edge.
Run: python historical_probe.py
"""
import asyncio
import json
from datetime import datetime, timedelta, timezone

import httpx

BASE = "https://api.bybit.com"
SYMBOLS = ("BTCUSDT", "ETHUSDT")


async def check(client, path, params):
    try:
        r = await client.get(BASE + path, params=params)
        r.raise_for_status()
        obj = r.json()
        if obj.get("retCode") != 0:
            return {"ok": False, "reason": obj.get("retMsg", "exchange_error")}
        data = obj.get("result", {})
        rows = data.get("list", [])
        first = rows[0] if rows else None
        timestamp = (first.get("timestamp") or first.get("fundingRateTimestamp")
                     or first.get("startTime")) if isinstance(first, dict) else (first[0] if first else None)
        return {"ok": bool(rows), "count": len(rows), "first_timestamp": timestamp}
    except Exception as exc:
        return {"ok": False, "reason": type(exc).__name__}


async def main():
    now = datetime.now(timezone.utc)
    start = int((now - timedelta(days=90)).timestamp() * 1000)
    results = {}
    async with httpx.AsyncClient(timeout=20) as client:
        for symbol in SYMBOLS:
            results[symbol] = {
                "kline_15m": await check(client, "/v5/market/kline",
                    {"category": "linear", "symbol": symbol, "interval": "15",
                     "start": start, "limit": 2}),
                "funding_history": await check(client, "/v5/market/funding/history",
                    {"category": "linear", "symbol": symbol, "startTime": start, "limit": 2}),
                "open_interest_history": await check(client, "/v5/market/open-interest",
                    {"category": "linear", "symbol": symbol, "intervalTime": "15min",
                     "startTime": start, "limit": 2}),
            }
    print(json.dumps({"probe_only": True, "validated_edge": False, "results": results}, indent=2))
    if not all(check["ok"] for symbol_results in results.values() for check in symbol_results.values()):
        raise SystemExit("Historical data probe failed: one or more required data sources unavailable")


if __name__ == "__main__":
    asyncio.run(main())
