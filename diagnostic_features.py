"""Read-only reconstruction of main.analyze features at exact executed signal times.
No orders, no account access, and no execution changes.
"""
import asyncio, bisect, json
from pathlib import Path
import httpx
import main
from historical_coverage import series

BAR=900_000
DAY=86_400_000
WARMUP_DAYS=40
TIMES_PATH=Path("backtest_results/diagnostic_trade_times_1791680400000.json")


def at_or_before(stamps,timestamp):
    return bisect.bisect_right(stamps,timestamp)-1


async def diagnose(symbol):
    payload=json.loads(TIMES_PATH.read_text())
    times=[int(x) for x in payload["signals"][symbol]]
    start=min(times)
    end=max(times)+BAR
    warmup=start-WARMUP_DAYS*DAY
    derivative_start=start-DAY

    async with httpx.AsyncClient(timeout=30) as client:
        data={}
        for kind in ("15","60","240","oi","funding"):
            fetch_start=warmup if kind in ("15","60","240") else derivative_start
            data[kind]=await series(client,symbol,kind,fetch_start,end)

    stamps={kind:sorted(rows) for kind,rows in data.items()}
    original_get=main.get
    original_ind=main.ind
    ind_cache={}

    def cached_ind(rows):
        key=(len(rows),str(rows[0][0]),str(rows[-1][0]))
        if key not in ind_cache:
            ind_cache[key]=original_ind(rows)
        return ind_cache[key]

    out=[]
    main.ind=cached_ind
    try:
        for signal_time in times:
            await asyncio.sleep(0)
            close_time=signal_time+BAR
            candle_sets={}
            for kind,step in (("15",BAR),("60",3_600_000),("240",14_400_000)):
                eligible=close_time-step
                idx=at_or_before(stamps[kind],eligible)
                if idx<199:
                    raise RuntimeError(f"insufficient {kind} history at {signal_time}")
                selected=stamps[kind][max(0,idx-219):idx+1]
                candle_sets[kind]=[data[kind][t] for t in reversed(selected)]

            oi_idx=at_or_before(stamps["oi"],signal_time)
            if oi_idx<4:
                raise RuntimeError(f"insufficient OI history at {signal_time}")
            oi_rows=[data["oi"][t] for t in reversed(stamps["oi"][oi_idx-4:oi_idx+1])]
            fund_idx=at_or_before(stamps["funding"],close_time)
            if fund_idx<0:
                raise RuntimeError(f"missing funding at {signal_time}")
            funding=float(data["funding"][stamps["funding"][fund_idx]]["fundingRate"])

            async def historical_get(_client,path,params=None):
                if path.endswith("/kline"):
                    return {"list":candle_sets[str(params["interval"])]}
                if path.endswith("/open-interest"):
                    return {"list":oi_rows}
                raise ValueError("Unexpected historical API request")

            main.get=historical_get
            try:
                r=await main.analyze(None,symbol,{symbol:funding})
            finally:
                main.get=original_get

            out.append({
                "symbol":symbol,
                "signal_time_ms":signal_time,
                "side":r["side"],
                "candidate_side":r["candidate_side"],
                "score":r["score"],
                "rsi_15m":r["rsi_15m"],
                "adx_15m":r["adx_15m"],
                "volume_ratio":r["volume_ratio"],
                "oi_change_1h_pct":r["oi_change_1h_pct"],
                "funding":r["funding"],
                "anti_chase_penalty":r["anti_chase_penalty"],
                "anti_chase_reason":r["anti_chase_reason"],
                "entry_gate_passed":r["entry_gate_passed"],
                "entry_gate_checks":r["entry_gate_checks"],
                "components":r["components"],
                "atr_15m":r["atr_15m"],
            })
    finally:
        main.get=original_get
        main.ind=original_ind

    return {"symbol":symbol,"anchor_ms":payload["anchor_ms"],"count":len(out),"features":out,"read_only":True}
