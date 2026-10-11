"""Aggregate distinct archived scan evaluation reports, research only."""
import argparse
import json
import math
from pathlib import Path

def aggregate(paths):
    records = []
    seen = set()
    for path in sorted(paths):
        doc = json.loads(Path(path).read_text())
        if doc.get("summary", {}).get("validated_edge") is not False:
            raise ValueError(f"Untrusted validation flag: {path}")
        scan_id = Path(path).parent.name
        for row in doc.get("results", []):
            if row.get("status") != "EVALUATED":
                continue
            key = (scan_id, row.get("symbol"), row.get("entry"))
            if key in seen:
                continue
            seen.add(key)
            r = float(row["net_r"])
            if not math.isfinite(r):
                raise ValueError(f"Non-finite R: {path}")
            records.append((scan_id, row.get("symbol"), r))
    vals = [r for _, _, r in records]
    gains = sum(x for x in vals if x > 0)
    losses = -sum(x for x in vals if x < 0)
    return {
        "method": "archived_signal_descriptive_only",
        "validated_edge": False,
        "n": len(vals),
        "distinct_scans": len(set(x[0] for x in records)),
        "wins": sum(x > 0 for x in vals),
        "win_rate": sum(x > 0 for x in vals) / len(vals) if vals else None,
        "mean_net_r": sum(vals) / len(vals) if vals else None,
        "profit_factor": gains / losses if losses else None,
        "warning": "Signals may overlap; no chronological portfolio equity or drawdown can be inferred from these reports. Sample is retention-limited and not out-of-sample validated."
    }

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("reports", nargs="+")
    parser.add_argument("--output", default="aggregate-summary.json")
    args = parser.parse_args()
    result = aggregate(args.reports)
    Path(args.output).write_text(json.dumps(result, indent=2))
    print(json.dumps(result, indent=2))
