"""Append validated, deduplicated evaluation results to a persistent research ledger."""
import argparse
import json
import math
from pathlib import Path

def update_ledger(paths, ledger_path):
    ledger = json.loads(ledger_path.read_text()) if ledger_path.exists() else {"schema_version": 1, "scans": {}}
    if ledger.get("schema_version") != 1 or not isinstance(ledger.get("scans"), dict):
        raise ValueError("Unsupported ledger schema")
    added = 0
    for path in sorted(paths):
        doc = json.loads(Path(path).read_text())
        if doc.get("summary", {}).get("validated_edge") is not False:
            raise ValueError(f"Untrusted evaluation report: {path}")
        scan_id = Path(path).parent.name
        if not scan_id.isdecimal():
            raise ValueError(f"Invalid scan ID: {scan_id}")
        rows = []
        for row in doc.get("results", []):
            if row.get("status") != "EVALUATED":
                continue
            net_r = float(row["net_r"])
            if not math.isfinite(net_r):
                raise ValueError(f"Invalid net_r: {path}")
            rows.append({"symbol": row["symbol"], "score": row.get("score"),
                         "outcome": row.get("outcome"), "net_r": net_r,
                         "net_return_pct": row.get("net_return_pct"),
                         "entry": row.get("entry"), "exit": row.get("exit")})
        if scan_id in ledger["scans"]:
            if ledger["scans"][scan_id] != rows:
                raise ValueError(f"Conflicting immutable scan record: {scan_id}")
            continue
        ledger["scans"][scan_id] = rows
        added += 1
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    ledger_path.write_text(json.dumps(ledger, indent=2, sort_keys=True) + "\n")
    return added, len(ledger["scans"])

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("reports", nargs="*")
    parser.add_argument("--ledger", default="research-history/evaluations.json")
    args = parser.parse_args()
    added, total = update_ledger(args.reports, Path(args.ledger))
    print(f"New scans: {added}; archived scans: {total}")
