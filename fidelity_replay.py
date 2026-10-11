"""Read-only replay wrapper that restores V2.10 production price validation.

Production scan_once validates a signal against current live price after analyze:
- reject if price already crossed the stop
- reject if |live-entry| / initial_risk > 0.35

Historical replay generates a signal at a completed 15m boundary, so the earliest
observable contemporaneous price is the next 15m bar open. That same open is also
the execution simulator's entry source. This wrapper applies the production gate
there before execution, without modifying main.py or signal rules.
"""
import historical_replay_chunk as hrc

BAR = 900_000

async def replay_chunk_with_price_validation(symbol, days=30, end_days_ago=1,
                                             initial_busy_until_ms=-1,
                                             max_entry_drift_r=0.35):
    original_simulate = hrc.simulate
    rejected_drift = 0
    rejected_crossed_stop = 0
    passed_validation = 0

    def validated_simulate(signal, bars):
        nonlocal rejected_drift, rejected_crossed_stop, passed_validation
        side = signal.get("side")
        if side not in ("LONG", "SHORT"):
            return original_simulate(signal, bars)
        direction = 1 if side == "LONG" else -1
        entry = float(signal["entry"])
        stop = float(signal["stop"])
        risk = abs(entry - stop)
        target_time = int(signal["signal_time_ms"]) + BAR
        next_bar = next((b for b in bars if int(b["start_ms"]) == target_time), None)
        if next_bar is None or risk <= 0:
            return original_simulate(signal, bars)
        live_price = float(next_bar["open"])
        crossed_stop = (side == "LONG" and live_price <= stop) or (side == "SHORT" and live_price >= stop)
        drift_r = abs(live_price - entry) / risk
        if crossed_stop:
            rejected_crossed_stop += 1
            return {"status":"rejected","symbol":signal["symbol"],"side":side,
                    "signal_time_ms":int(signal["signal_time_ms"]),
                    "reason":"PRODUCTION_STOP_ALREADY_CROSSED",
                    "live_price":live_price,"entry_drift_r":drift_r}
        if drift_r > float(max_entry_drift_r):
            rejected_drift += 1
            return {"status":"rejected","symbol":signal["symbol"],"side":side,
                    "signal_time_ms":int(signal["signal_time_ms"]),
                    "reason":"PRODUCTION_PRICE_TOO_FAR_FROM_ENTRY",
                    "live_price":live_price,"entry_drift_r":drift_r}
        passed_validation += 1
        trade = original_simulate(signal, bars)
        if isinstance(trade, dict):
            trade["production_price_validation_passed"] = True
            trade["entry_drift_r"] = drift_r
            trade["validation_live_price"] = live_price
        return trade

    hrc.simulate = validated_simulate
    try:
        report = await hrc.replay_chunk(symbol, days, end_days_ago, initial_busy_until_ms)
    finally:
        hrc.simulate = original_simulate

    report["production_price_validation"] = {
        "enabled": True,
        "historical_live_price_proxy": "next_15m_open_at_signal_boundary",
        "max_entry_drift_r": float(max_entry_drift_r),
        "rejected_price_too_far": rejected_drift,
        "rejected_stop_already_crossed": rejected_crossed_stop,
        "passed_validation_calls": passed_validation,
        "production_main_modified": False
    }
    return report
