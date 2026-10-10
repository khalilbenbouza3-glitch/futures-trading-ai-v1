"""Descriptive, read-only research on archived evaluation reports."""
import argparse
import json
import math
from collections import defaultdict
from pathlib import Path


def summarize(paths):
    records, seen = [], set()
    for path in paths:
        doc = json.loads(Path(path).read_text())
        if doc.get("summary", {}).get("validated_edge") is not False:
            raise ValueError(f"Unexpected validated_edge in {path}")
        scan_id = str(doc.get("scan_id") or Path(path).stem)
        for row in doc.get("results", []):
            if row.get("status") != "EVALUATED":
                continue
            key = (scan_id, row.get("symbol"))
            if key in seen:
                continue
            seen.add(key)
            records.append(row)

    def stats(rows):
        values = [float(r["net_r"]) for r in rows if isinstance(r.get("net_r"), (float, int))
                  and math.isfinite(float(r["net_r"]))]
        wins = sum(v > 0 for v in values)
        losses = sum(-v for v in values if v < 0)
        gains = sum(v for v in values if v > 0)
        return {"n": len(values),
                "win_rate": round(wins / len(values), 4) if values else None,
                "mean_net_r": round(sum(values) / len(values), 5) if values else None,
                "profit_factor": round(gains / losses, 4) if losses else None}

    groups = defaultdict(list)
    for row in records:
        groups["all"].append(row)
        if row.get("score") is not None:
            score = float(row["score"])
            if math.isfinite(score):
                groups["score_ge_80" if score >= 80 else "score_lt_80"].append(row)
    return {"method": "descriptive_in_sample_only", "validated_edge": False,
            "warning": "Filters require an independent chronological holdout.",
            "groups": {k: stats(v) for k, v in sorted(groups.items())}}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("reports", nargs="+")
    parser.add_argument("--output", default="research-summary.json")
    args = parser.parse_args()
    result = summarize(args.reports)
    Path(args.output).write_text(json.dumps(result, indent=2))
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
