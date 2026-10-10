"""Read-only, conservative candle execution simulator.
Consumes historical signals produced without look-ahead; it does NOT manufacture
signals or assert that scanner indicators have been historically reproduced.
Input JSON: {"signals":[{"symbol":"BTCUSDT","signal_time_ms":...,
"side":"LONG","entry":...,"stop":...,"tp1":...}],
"candles":{"BTCUSDT":[{"start_ms":...,"open":...,"high":...,"low":...,"close":...}]}}
Usage: python backtest_execution.py input.json
"""
import json, sys

BAR_MS = 900000
FEE_PER_SIDE = 0.0006
SLIPPAGE_PER_SIDE = 0.0002

def simulate(signal, bars):
    side = signal["side"]
    if side not in ("LONG", "SHORT"):
        return {"status":"rejected","reason":"INVALID_SIDE"}
    direction = 1 if side == "LONG" else -1
    entry, stop, tp = (float(signal[k]) for k in ("entry","stop","tp1"))
    risk = direction*(entry-stop)
    reward = direction*(tp-entry)
    if risk <= 0 or reward <= 0:
        return {"status":"rejected","reason":"INVALID_RISK"}
    # Enter at NEXT candle open, not the signal candle's close.
    future = sorted((b for b in bars if int(b["start_ms"]) >= int(signal["signal_time_ms"])+BAR_MS),
                    key=lambda b:int(b["start_ms"]))
    if not future or int(future[0]["start_ms"]) != int(signal["signal_time_ms"])+BAR_MS:
        return {"status":"rejected","reason":"NEXT_BAR_MISSING"}
    actual_entry = float(future[0]["open"])*(1+direction*SLIPPAGE_PER_SIDE)
    if direction*(actual_entry-stop) <= 0:
        return {"status":"rejected","reason":"STOP_CROSSED_AT_ENTRY"}
    exit_price, exit_time, reason = None,None,None
    for b in future[:96]:
        lo,hi = float(b["low"]),float(b["high"])
        hit_stop = lo<=stop if direction==1 else hi>=stop
        hit_tp = hi>=tp if direction==1 else lo<=tp
        # Ambiguous same-bar stop/TP -> conservative stop-first.
        if hit_stop:
            exit_price=stop*(1-direction*SLIPPAGE_PER_SIDE)
            reason="STOP" if not hit_tp else "STOP_FIRST_AMBIGUOUS"
        elif hit_tp:
            exit_price=tp*(1-direction*SLIPPAGE_PER_SIDE)
            reason="TP1"
        if reason:
            exit_time=int(b["start_ms"])
            break
    if reason is None:
        if len(future)<96:
            return {"status":"rejected","reason":"INSUFFICIENT_EXIT_HORIZON"}
        b=future[95]
        exit_price=float(b["close"])*(1-direction*SLIPPAGE_PER_SIDE)
        exit_time=int(b["start_ms"])
        reason="TIMEOUT_24H"
    gross=direction*(exit_price/actual_entry-1)
    net=gross-FEE_PER_SIDE*(1+exit_price/actual_entry)
    return {"status":"closed","symbol":signal["symbol"],"side":side,
            "signal_time_ms":int(signal["signal_time_ms"]),"exit_time_ms":exit_time,
            "entry":actual_entry,"exit":exit_price,"exit_reason":reason,
            "gross_return":gross,"net_return_ex_funding":net,
            "funding_included":False}

def main(path):
    with open(path) as f: data=json.load(f)
    results=[]
    for sig in data["signals"]:
        results.append(simulate(sig,data["candles"].get(sig["symbol"],[])))
    closed=[r for r in results if r["status"]=="closed"]
    report={"simulation_only":True,"strategy_backtest_validated":False,
            "assumptions":{"fee_per_side":FEE_PER_SIDE,"slippage_per_side":SLIPPAGE_PER_SIDE,
                           "same_bar_collision":"stop_first","entry":"next_15m_open",
                           "exit":"TP1_or_stop_or_24h","funding":"not_included"},
            "submitted_signals":len(results),"closed":len(closed),
            "rejected":len(results)-len(closed),
            "win_rate_ex_funding":sum(r["net_return_ex_funding"]>0 for r in closed)/len(closed) if closed else None,
            "mean_net_return_ex_funding":sum(r["net_return_ex_funding"] for r in closed)/len(closed) if closed else None,
            "trades":results}
    print(json.dumps(report,indent=2))
    return 0 if closed else 2

if __name__=="__main__":
    sys.exit(main(sys.argv[1]))
