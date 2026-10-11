"""Read-only experiment fixing the internal SHORT/OI gate inconsistency.

Current V2.10 derivative score treats rising OI as trend-confirming for both
long and short trends (oi_sig = sign(OI change) * sign(trend)). But the entry
gate requires falling OI for SHORT, which conflicts with that score logic.

This experiment changes ONLY the SHORT OI gate semantics:
- LONG remains exactly unchanged.
- SHORT uses the same participation rule as LONG: rising OI passes; changes
  smaller than 1% in magnitude remain neutral/pass; OI <= -1% is rejected.
- A SHORT previously rejected only by its old OI gate may be restored using
  the candidate stop/targets already produced by main.analyze.
Production main.py is not modified.
"""
import main
import historical_replay_chunk as hrc

async def replay_chunk_oi_consistent(symbol, days=30, end_days_ago=1,
                                     initial_busy_until_ms=-1):
    original_analyze = main.analyze
    restored_short = 0
    rejected_short = 0

    async def consistent_analyze(client, symbol_name, funding):
        nonlocal restored_short, rejected_short
        result = await original_analyze(client, symbol_name, funding)
        if result.get("candidate_side") != "SHORT":
            return result

        oi_pct = float(result.get("oi_change_1h_pct", 0.0))
        new_oi_pass = (oi_pct >= 0.0) or (abs(oi_pct) < 1.0)
        checks = result.get("entry_gate_checks", {})
        other_pass = all(v for k, v in checks.items() if k != "oi")

        # Restore only signals for which OI was the conflicting gate.
        if new_oi_pass and other_pass and result.get("score", 0) >= 70:
            if result.get("side") != "SHORT":
                result = dict(result)
                result["side"] = "SHORT"
                result["stop"] = result.get("candidate_stop")
                result["tp1"] = result.get("candidate_tp1")
                result["tp2"] = result.get("candidate_tp2")
                result["rr_tp1"] = 2.0
                result["rr_tp2"] = 3.0
                result["experimental_oi_consistency_action"] = "RESTORED_SHORT"
                restored_short += 1
        elif not new_oi_pass and result.get("side") == "SHORT":
            result = dict(result)
            result["side"] = "NEUTRAL"
            result["stop"] = result["tp1"] = result["tp2"] = None
            result["rr_tp1"] = result["rr_tp2"] = None
            result["experimental_oi_consistency_action"] = "REJECTED_SHORT_FALLING_OI"
            rejected_short += 1
        return result

    main.analyze = consistent_analyze
    try:
        report = await hrc.replay_chunk(symbol, days, end_days_ago, initial_busy_until_ms)
    finally:
        main.analyze = original_analyze

    report["experimental_oi_consistency"] = {
        "enabled": True,
        "long_gate_changed": False,
        "short_rule": "pass if oi_change >= 0 or abs(oi_change) < 1%; reject <= -1%",
        "restored_short_signal_calls": restored_short,
        "rejected_short_signal_calls": rejected_short,
        "production_main_modified": False,
    }
    return report
