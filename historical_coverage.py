"""Read-only 90-day market-history coverage audit. Run where Bybit API is reachable.
No trading, no signal optimization, no claims of profitability.
"""
import asyncio, json, sys, time
from datetime import datetime, timezone
import httpx

BASE = "https://api.bybit.com"
INTERVALS = {"15":900000, "60":3600000, "240":14400000}
DAYS = 90

async def fetch(c, path, params):
    for attempt in range(4):
        try:
            r = await c.get(BASE + path, params=params, timeout=25)
            r.raise_for_status()
            data = r.json()
            if data.get("retCode") != 0:
                raise ValueError(str(data.get("retMsg")))
            return data["result"].get("list", [])
        except (httpx.HTTPError, ValueError):
            if attempt == 3: raise
            await asyncio.sleep(1.5 * (attempt + 1))

async def series(c, symbol, kind, start, end):
    step = INTERVALS.get(kind, 900000)
    output = {}
    cursor = start
    while cursor < end:
        upper = min(end - 1, cursor + step * 190)
        if kind == "oi":
            path = "/v5/market/open-interest"
            params = dict(category="linear", symbol=symbol, intervalTime="15min", startTime=cursor, endTime=upper, limit=200)
        elif kind == "funding":
            path = "/v5/market/funding/history"
            params = dict(category="linear", symbol=symbol, startTime=cursor, endTime=upper, limit=200)
        else:
            path = "/v5/market/kline"
            params = dict(category="linear", symbol=symbol, interval=kind, start=cursor, end=upper, limit=200)
        rows = await fetch(c, path, params)
        for row in rows:
            stamp = int((row.get("timestamp") or row.get("fundingRateTimestamp")) if isinstance(row, dict) else row[0])
            if start <= stamp < end:
                output[stamp] = row
        cursor = upper + 1
        await asyncio.sleep(.08)
    return output

async def main():
    now = int(time.time()*1000)
    end = (now//900000)*900000
    start = end - DAYS*86400000
    report = {"start":datetime.fromtimestamp(start/1000,timezone.utc).isoformat(),
              "end":datetime.fromtimestamp(end/1000,timezone.utc).isoformat(),"days":DAYS,
              "read_only":True,"backtest_completed":False,"validated_edge":False,"symbols":{}}
    async with httpx.AsyncClient(headers={"User-Agent":"market-history-audit/1.0"}) as c:
        for symbol in ("BTCUSDT","ETHUSDT"):
            report["symbols"][symbol] = {}
            for kind in ("15","60","240","oi","funding"):
                try:
                    rows = await series(c,symbol,kind,start,end)
                    timestamps = sorted(rows)
                    interval = INTERVALS.get(kind)
                    expected = (end-start)//interval if interval else None
                    missing = (expected-len(rows)) if expected is not None else None
                    report["symbols"][symbol][kind] = {
                        "count":len(rows),"expected":expected,"missing":missing,
                        "complete":missing==0 if missing is not None else None,
                        "first_ms":timestamps[0] if timestamps else None,
                        "last_ms":timestamps[-1] if timestamps else None}
                except Exception as exc:
                    report["symbols"][symbol][kind]={"error":type(exc).__name__,"detail":str(exc)[:180]}
    print(json.dumps(report,indent=2))
    with open("historical_coverage_report.json","w") as f:json.dump(report,f,indent=2)
    return 0 if all(v.get("complete") is True for x in report["symbols"].values() for k,v in x.items() if k!="funding") else 2

if __name__=="__main__":
    sys.exit(asyncio.run(main()))
