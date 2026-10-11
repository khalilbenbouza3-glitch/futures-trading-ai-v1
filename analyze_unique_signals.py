"""Descriptive analysis of timed signals; no portfolio P&L inference."""
import json
import math
from pathlib import Path

def analyze(scans):
    unique = {}
    untimed = 0
    timed_rows = 0
    for scan_id, rows in sorted(scans.items()):
        for row in rows:
            start = row.get("entry_bar_start_ms")
            end = row.get("exit_bar_start_ms")
            side = row.get("side")
            if start is None or end is None or side not in ("LONG", "SHORT"):
                untimed += 1
                continue
            start, end = int(start), int(end)
            if end < start:
                raise ValueError("Exit bar before entry bar")
            timed_rows += 1
            key = (row["symbol"], side, start)
            if key in unique:
                old = unique[key]
                if any(old[k] != row[k] for k in ("entry", "exit", "net_r")):
                    raise ValueError("Conflicting results for same signal identity")
            else:
                unique[key] = row
    trades = sorted(unique.values(), key=lambda x: int(x["entry_bar_start_ms"]))
    overlap_count = 0
    for i, row in enumerate(trades):
        start = int(row["entry_bar_start_ms"])
        if any(int(previous["exit_bar_start_ms"]) >= start for previous in trades[:i]):
            overlap_count += 1
    returns = [float(x["net_r"]) for x in trades]
    if not all(math.isfinite(x) for x in returns):
        raise ValueError("Invalid return")
    gains = sum(x for x in returns if x > 0)
    losses = -sum(x for x in returns if x < 0)
    return {
        "validated_edge": False,
        "timed_unique_signals": len(trades),
        "timed_duplicates_removed": timed_rows - len(trades),
        "legacy_untimed_rows_excluded": untimed,
        "overlapping_signals": overlap_count,
        "win_rate": sum(x > 0 for x in returns) / len(returns) if returns else None,
        "mean_net_r": sum(returns) / len(returns) if returns else None,
        "profit_factor": gains / losses if losses else None,
        "max_drawdown": None,
        "warning": "Signal-level descriptive only; overlaps, execution order, capital allocation and exact intrabar exits are not resolved."
    }

if __name__ == "__main__":
    data = json.loads(Path("research-history/evaluations.json").read_text())
    result = analyze(data["scans"])
    Path("unique-signal-summary.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))
