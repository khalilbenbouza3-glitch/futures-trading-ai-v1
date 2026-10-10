"""Read-only historical signal replay using the SAME main.analyze scoring rules.
Downloads a 125-day warmup for 4h EMA200, replays 90 days at closed 15m bars,
and passes signals to the conservative execution simulator.
No orders, no account access. Funding cashflows are not yet modeled.
"""
import asyncio, bisect, json, time, os
from datetime import datetime, timezone
import httpx
import main
from historical_coverage import series
from backtest_execution import simulate

BAR=900000
WARMUP_DAYS=40
TEST_DAYS=int(os.getenv("BACKTEST_DAYS","90"))

def at_or_before(stamps, timestamp):
    return bisect.bisect_right(stamps,timestamp)-1

async def replay(symbol):
    end=(int(time.time()*1000)//BAR)*BAR
    start=end-TEST_DAYS*86400000
    warmup=start-WARMUP_DAYS*86400000
    async with httpx.AsyncClient(timeout=30) as client:
        data={}
        for kind in ("15","60","240","oi","funding"):
            data[kind]=await series(client,symbol,kind,warmup if kind in ("15","60","240") else start,end)
    stamps={kind:sorted(rows) for kind,rows in data.items()}
    checks={}
    for kind,step in (("15",BAR),("60",3600000),("240",14400000),("oi",BAR)):
        expected=range(((start+step-1)//step)*step,end,step)
        missing=sum(t not in data[kind] for t in expected)
        checks[kind]={"missing":missing,"available":len(data[kind])}
    if any(v["missing"] for v in checks.values()):
        return {"symbol":symbol,"status":"BLOCKED_INCOMPLETE_DATA","coverage":checks,
                "backtest_validated":False}
    if len(stamps["funding"])==0:
        return {"symbol":symbol,"status":"BLOCKED_NO_FUNDING","coverage":checks,
                "backtest_validated":False}
    signals=[]
    candles=[{"start_ms":t,"open":float(v[1]),"high":float(v[2]),
              "low":float(v[3]),"close":float(v[4])} for t,v in sorted(data["15"].items()) if t>=start]
    for close_time in range(start+BAR,end-BAR+1,BAR):
        # A signal becomes known only when the 15m bar closes.
        # The execution simulator enters at the NEXT bar's open.
        last_open=close_time-BAR
        candle_sets={}
        for kind,step in (("15",BAR),("60",3600000),("240",14400000)):
            eligible=close_time-step
            idx=at_or_before(stamps[kind],eligible)
            if idx<199:
                break
            selected=stamps[kind][max(0,idx-219):idx+1]
            candle_sets[kind]=[data[kind][t] for t in reversed(selected)]
        if len(candle_sets)!=3:
            continue
        oi_idx=at_or_before(stamps["oi"],last_open)
        if oi_idx<4:
            continue
        oi_rows=[data["oi"][t] for t in reversed(stamps["oi"][oi_idx-4:oi_idx+1])]
        fund_idx=at_or_before(stamps["funding"],close_time)
        if fund_idx<0:
            continue
        funding=float(data["funding"][stamps["funding"][fund_idx]]["fundingRate"])
        original_get=main.get
        async def historical_get(_client,path,params=None):
            if path.endswith("/kline"):
                return {"list":candle_sets[str(params["interval"])]}
            if path.endswith("/open-interest"):
                return {"list":oi_rows}
            raise ValueError("Unexpected historical API request")
        main.get=historical_get
        try:
            result=await main.analyze(None,symbol,{symbol:funding})
        finally:
            main.get=original_get
        if result["side"] in ("LONG","SHORT"):
            signals.append({"symbol":symbol,"signal_time_ms":close_time-BAR,
                            "side":result["side"],"entry":result["entry"],
                            "stop":result["stop"],"tp1":result["tp1"],"score":result["score"]})
    trades=[simulate(sig,candles) for sig in signals]
    closed=[x for x in trades if x["status"]=="closed"]
    return {"symbol":symbol,"status":"REPLAYED","coverage":checks,
            "strategy_rules":"main.analyze","read_only":True,
            "funding_signal_used":True,"funding_cashflows_included":False,
            "backtest_validated":False,"signals":len(signals),
            "closed_trades":len(closed),"rejected":len(trades)-len(closed),
            "mean_net_return_ex_funding":sum(x["net_return_ex_funding"] for x in closed)/len(closed) if closed else None,
            "win_rate_ex_funding":sum(x["net_return_ex_funding"]>0 for x in closed)/len(closed) if closed else None,
            "trades":trades}

async def run():
    reports=[]
    for symbol in ("BTCUSDT","ETHUSDT"):
        try: reports.append(await replay(symbol))
        except Exception as e: reports.append({"symbol":symbol,"status":"ERROR","error":str(e)[:300]})
    with open("historical_replay_report.json","w") as f:json.dump(reports,f,indent=2)
    print(json.dumps([{k:v for k,v in r.items() if k!="trades"} for r in reports],indent=2))

if __name__=="__main__":
    asyncio.run(run())
