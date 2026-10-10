"""Archive a scanner snapshot with subsequent closed Bybit candles.

Usage: python collect_evaluation.py scan-result.json --output evaluation-input.json
Only signals older than the requested horizon can be evaluated.
This is an offline research data collector; it never places orders.
"""
import argparse
import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path

import httpx
from evaluation import timestamp_ms

INTERVAL_MS = 900000


async def collect(snapshot, horizon=24):
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    data = {"updated_at": snapshot.get("updated_at"), "signals": [], "candles": {}}
    async with httpx.AsyncClient(timeout=20) as client:
        for signal in snapshot.get("signals", []):
            if signal.get("side") not in ("LONG", "SHORT"):
                continue
            row = dict(signal)
            row["updated_at"] = snapshot["updated_at"]
            stamp = timestamp_ms(row["updated_at"])
            first = (stamp // INTERVAL_MS + 1) * INTERVAL_MS
            if now_ms < first + horizon * INTERVAL_MS:
                continue
            response = await client.get(
                "https://api.bybit.com/v5/market/kline",
                params={"category": "linear", "symbol": row["symbol"],
                        "interval": "15", "start": first,
                        "end": first + (horizon - 1) * INTERVAL_MS,
                        "limit": min(1000, horizon)})
            response.raise_for_status()
            body = response.json()
            if body.get("retCode") != 0:
                raise RuntimeError("Bybit candle request failed for " + row["symbol"])
            candles = [
                {"start_ms": int(x[0]), "open": float(x[1]), "high": float(x[2]),
                 "low": float(x[3]), "close": float(x[4])}
                for x in body["result"]["list"]
                if int(x[0]) + INTERVAL_MS <= now_ms
            ]
            data["signals"].append(row)
            data["candles"][row["symbol"]] = sorted(candles, key=lambda x: x["start_ms"])
    return data


def main():
    p = argparse.ArgumentParser()
    p.add_argument("snapshot")
    p.add_argument("--output", default="evaluation-input.json")
    p.add_argument("--horizon-bars", type=int, default=24)
    args = p.parse_args()
    if not 1 <= args.horizon_bars <= 1000:
        p.error("horizon-bars must be between 1 and 1000")
    snapshot = json.loads(Path(args.snapshot).read_text())
    result = asyncio.run(collect(snapshot, args.horizon_bars))
    Path(args.output).write_text(json.dumps(result, indent=2))
    print("Matured actionable signals:", len(result["signals"]))


if __name__ == "__main__":
    main()
