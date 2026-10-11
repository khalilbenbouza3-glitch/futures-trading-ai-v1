"""Conservative, read-only evaluation of previously recorded trading signals.

Input: JSON object {"signals":[...],"candles":{"BTCUSDT":[{"start_ms":...,
"open":...,"high":...,"low":...,"close":...},...]}}.
Candles must be closed, chronological, and start strictly after signal timestamp.
No exchange orders, no estimated win probabilities.
"""
import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path


def timestamp_ms(value):
    if isinstance(value, (int, float)):
        return int(value)
    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)


def evaluate_signal(signal, candles, *, fee_bps=6.0, slippage_bps=3.0,
                    horizon_bars=24, interval_ms=900000):
    """Evaluate one signal using a next-bar-open entry to avoid lookahead.

    An OHLC bar touching TP and SL is assigned SL (conservative).
    Returns status UNRESOLVED if fewer than horizon_bars *closed* bars exist.
    """
    if signal.get("side") not in ("LONG", "SHORT"):
        return {"status": "SKIPPED", "reason": "NOT_ACTIONABLE"}
    side = signal["side"]
    direction = 1 if side == "LONG" else -1
    stamp = timestamp_ms(signal["updated_at"])
    stop, target = float(signal["stop"]), float(signal["tp1"])
    ordered = sorted(candles, key=lambda x: int(x["start_ms"]))
    future = [x for x in ordered if int(x["start_ms"]) >= ((stamp // interval_ms) + 1) * interval_ms]
    if len(future) < horizon_bars:
        return {"status": "UNRESOLVED", "reason": "INSUFFICIENT_FUTURE_BARS"}
    future = future[:horizon_bars]
    if int(future[0]["start_ms"]) != ((stamp // interval_ms) + 1) * interval_ms:
        return {"status": "UNRESOLVED", "reason": "MISSING_FIRST_ENTRY_BAR"}
    for i, bar in enumerate(future):
        start = int(bar["start_ms"])
        if i and start != int(future[i-1]["start_ms"]) + interval_ms:
            return {"status": "UNRESOLVED", "reason": "MISSING_CANDLES"}
    entry = float(future[0]["open"])
    if not all(math.isfinite(x) and x > 0 for x in (entry, stop, target)):
        return {"status": "INVALID", "reason": "INVALID_PRICES"}
    if direction * (target - entry) <= 0 or direction * (entry - stop) <= 0:
        return {"status": "INVALID", "reason": "INVALID_RISK_REWARD"}
    # A gap through the intended stop at entry invalidates the proposed setup.
    if direction * (entry - stop) <= 0:
        return {"status": "INVALID", "reason": "STOP_CROSSED_BEFORE_ENTRY"}
    entry_exec = entry * (1 + direction * slippage_bps / 10000)
    outcome = "TIMEOUT"
    exit_price = float(future[-1]["close"])
    exit_bar_start_ms = int(future[-1]["start_ms"])
    mfe = mae = 0.0
    for bar in future:
        high, low = float(bar["high"]), float(bar["low"])
        if low <= 0 or high < low:
            return {"status": "INVALID", "reason": "INVALID_CANDLE"}
        mfe = max(mfe, direction * ((high if direction == 1 else low) / entry - 1))
        mae = min(mae, direction * ((low if direction == 1 else high) / entry - 1))
        # Gaps through a stop execute at the worse of stop and bar open.
        opening = float(bar["open"])
        if (direction == 1 and opening <= stop) or (direction == -1 and opening >= stop):
            outcome, exit_price = "STOP", opening
            exit_bar_start_ms = int(bar["start_ms"])
            break
        stop_hit = low <= stop if direction == 1 else high >= stop
        target_hit = high >= target if direction == 1 else low <= target
        if stop_hit:
            outcome, exit_price = "STOP", stop
            exit_bar_start_ms = int(bar["start_ms"])
            break
        if target_hit:
            outcome, exit_price = "TARGET", target
            exit_bar_start_ms = int(bar["start_ms"])
            break
    exit_exec = exit_price * (1 - direction * slippage_bps / 10000)
    gross = direction * (exit_exec / entry_exec - 1)
    net = gross - 2 * fee_bps / 10000
    risk = abs(entry_exec - stop) / entry_exec
    if risk <= 0:
        return {"status": "INVALID", "reason": "ZERO_RISK"}
    return {"status": "EVALUATED", "outcome": outcome,
            "signal_at_ms": stamp, "entry_bar_start_ms": int(future[0]["start_ms"]),
            "exit_bar_start_ms": exit_bar_start_ms, "side": side,
            "entry": entry, "exit": exit_price, "net_return_pct": round(net * 100, 5),
            "net_r": round(net / risk, 5), "mfe_pct": round(mfe * 100, 5),
            "mae_pct": round(mae * 100, 5), "bars_available": len(future)}


def summarize(results):
    evaluated = [r for r in results if r.get("status") == "EVALUATED"]
    wins = [r for r in evaluated if r["net_return_pct"] > 0]
    return {"evaluated": len(evaluated), "unresolved": sum(r.get("status") == "UNRESOLVED" for r in results),
            "win_rate": round(len(wins) / len(evaluated), 4) if evaluated else None,
            "mean_net_r": round(sum(r["net_r"] for r in evaluated) / len(evaluated), 5) if evaluated else None,
            "validated_edge": False}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input", help="Recorded signals and subsequent completed candles as JSON")
    parser.add_argument("--output", default="evaluation-result.json")
    parser.add_argument("--fee-bps", type=float, default=6)
    parser.add_argument("--slippage-bps", type=float, default=3)
    parser.add_argument("--horizon-bars", type=int, default=24)
    args = parser.parse_args()
    if args.horizon_bars < 1 or args.fee_bps < 0 or args.slippage_bps < 0:
        parser.error("Invalid evaluation parameters")
    data = json.loads(Path(args.input).read_text())
    results = []
    for signal in data["signals"]:
        row = dict(signal)
        row.setdefault("updated_at", data.get("updated_at"))
        result = evaluate_signal(row, data["candles"].get(row["symbol"], []),
                                 fee_bps=args.fee_bps, slippage_bps=args.slippage_bps,
                                 horizon_bars=args.horizon_bars)
        results.append({"symbol": row["symbol"], "score": row.get("score"), **result})
    report = {"method": "next_bar_open_conservative_ohlc_v1", "summary": summarize(results),
              "results": results}
    Path(args.output).write_text(json.dumps(report, indent=2))
    print(json.dumps(report["summary"]))


if __name__ == "__main__":
    main()
