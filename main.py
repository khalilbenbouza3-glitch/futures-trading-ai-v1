import asyncio, os
from datetime import datetime, timezone
import httpx, numpy as np, pandas as pd
from fastapi import FastAPI

BASES=["https://api.bybit.com","https://api.bytick.com"]
app=FastAPI(title="Futures Trading AI V2",version="2.10.0")
cache={"status":"starting","version":"2.10.0","updated_at":None,"pairs_scanned":0,"signals":[]}

async def get(c,path,params=None):
    errors=[]
    for base in BASES:
        try:
            r=await c.get(base+path,params=params,timeout=12)
            r.raise_for_status()
            j=r.json()
            if j.get("retCode",0)!=0: raise RuntimeError(f'Bybit {j.get("retCode")}: {j.get("retMsg")}')
            return j["result"]
        except Exception as e:
            errors.append(f"{base}: {type(e).__name__}: {e}")
    raise RuntimeError(" | ".join(errors))

def ind(rows):
    d=pd.DataFrame(rows,columns=["t","o","h","l","c","v","ct","q","n","tb","tq","x"])
    for x in ["h","l","c","v","q","tb","tq"]: d[x]=pd.to_numeric(d[x])
    c,h,l,v=d.c,d.h,d.l,d.v
    e20=c.ewm(span=20,adjust=False).mean(); e50=c.ewm(span=50,adjust=False).mean(); e200=c.ewm(span=200,adjust=False).mean()
    delta=c.diff(); gain=delta.clip(lower=0).ewm(alpha=1/14,adjust=False).mean(); loss=(-delta.clip(upper=0)).ewm(alpha=1/14,adjust=False).mean()
    rsi=100-(100/(1+gain/loss.replace(0,np.nan)))
    tr=pd.concat([h-l,(h-c.shift()).abs(),(l-c.shift()).abs()],axis=1).max(axis=1); atr=tr.ewm(alpha=1/14,adjust=False).mean()
    macd=c.ewm(span=12,adjust=False).mean()-c.ewm(span=26,adjust=False).mean(); macds=macd.ewm(span=9,adjust=False).mean()
    mid=c.rolling(20).mean(); sd=c.rolling(20).std(); bbpos=(c-(mid-2*sd))/((mid+2*sd)-(mid-2*sd))
    tp=(h+l+c)/3; vwap=(tp*v).rolling(48).sum()/v.rolling(48).sum().replace(0,np.nan)
    up=h.diff(); down=-l.diff(); plus=np.where((up>down)&(up>0),up,0.0); minus=np.where((down>up)&(down>0),down,0.0)
    atr14=tr.ewm(alpha=1/14,adjust=False).mean(); pdi=100*pd.Series(plus,index=d.index).ewm(alpha=1/14,adjust=False).mean()/atr14
    mdi=100*pd.Series(minus,index=d.index).ewm(alpha=1/14,adjust=False).mean()/atr14
    dx=100*(pdi-mdi).abs()/(pdi+mdi).replace(0,np.nan); adx=dx.ewm(alpha=1/14,adjust=False).mean()
    hi20=h.shift(1).rolling(20).max(); lo20=l.shift(1).rolling(20).min()
    structure=1 if c.iloc[-1]>hi20.iloc[-1] else (-1 if c.iloc[-1]<lo20.iloc[-1] else (0.5 if c.iloc[-1]>c.iloc[-20] else -0.5))
    trend=1 if c.iloc[-1]>e20.iloc[-1]>e50.iloc[-1] else (-1 if c.iloc[-1]<e20.iloc[-1]<e50.iloc[-1] else 0)
    macro=1 if c.iloc[-1]>e200.iloc[-1] else -1
    momentum=np.mean([1 if rsi.iloc[-1]>=55 else (-1 if rsi.iloc[-1]<=45 else 0),1 if macd.iloc[-1]>macds.iloc[-1] else -1])
    volratio=float(v.iloc[-1]/max(v.rolling(20).mean().iloc[-1],1e-12))
    volume=(1 if c.iloc[-1]>=c.iloc[-2] else -1)*min(volratio/1.5,1)
    vwap_sig=1 if c.iloc[-1]>vwap.iloc[-1] else -1
    trend_strength=min(float(adx.iloc[-1])/30,1) if np.isfinite(adx.iloc[-1]) else 0
    return {"price":float(c.iloc[-1]),"atr":float(atr.iloc[-1]),"rsi":float(rsi.iloc[-1]),"trend":trend,"macro":macro,
      "momentum":float(momentum),"structure":float(structure),"volume":float(volume),"volratio":volratio,"vwap":vwap_sig,
      "adx":float(adx.iloc[-1]) if np.isfinite(adx.iloc[-1]) else 0,"trend_strength":trend_strength,
      "bbpos":float(bbpos.iloc[-1]) if np.isfinite(bbpos.iloc[-1]) else .5}

async def oi_change(c,symbol):
    try:
        x=await get(c,"/v5/market/open-interest",{"category":"linear","symbol":symbol,"intervalTime":"15min","limit":5})
        vals=[float(a["openInterest"]) for a in reversed(x["list"])]
        return ((vals[-1]/vals[0]-1) if len(vals)>1 and vals[0] else 0), vals[-1] if vals else 0
    except Exception:return 0,0

async def analyze(c,symbol,funding):
    f={}
    for tf,iv in (("15m","15"),("1h","60"),("4h","240")):
        k=await get(c,"/v5/market/kline",{"category":"linear","symbol":symbol,"interval":iv,"limit":220})
        # Bybit returns newest-first candles. The newest candle is often still
        # forming; its partial volume must not be compared with full candles.
        # Use only completed candles for indicators and volume gates.
        interval_ms=int(iv)*60_000
        now_ms=int(datetime.now(timezone.utc).timestamp()*1000)
        closed=[a for a in k["list"] if int(a[0])+interval_ms<=now_ms]
        if len(closed)<200:
            raise ValueError(f"Insufficient closed {tf} candles for {symbol}: {len(closed)}")
        rows=[]
        for a in reversed(closed):
            rows.append([a[0],a[1],a[2],a[3],a[4],a[5],0,a[6],0,0,0,0])
        f[tf]=ind(rows)
    oid,oi_now=await oi_change(c,symbol)
    w={"15m":.25,"1h":.35,"4h":.40}
    trend=sum(w[t]*(0.65*f[t]["trend"]+0.35*f[t]["macro"])*(.5+.5*f[t]["trend_strength"]) for t in w)
    momentum=sum(w[t]*f[t]["momentum"] for t in w)
    structure=sum(w[t]*f[t]["structure"] for t in w)
    volume=sum(w[t]*f[t]["volume"] for t in w)
    vwap=sum(w[t]*f[t]["vwap"] for t in w)
    fr=float(funding.get(symbol,0))
    funding_sig=-1 if fr>.0005 else (1 if fr<-.0005 else 0)
    oi_sig=(1 if oid>0.01 else (-1 if oid<-0.01 else 0))*np.sign(trend if trend else momentum)
    derivatives=.55*funding_sig+.45*oi_sig
    raw=.30*trend+.20*momentum+.20*structure+.10*volume+.10*vwap+.10*derivatives
    raw=float(np.clip(raw,-1,1))
    preliminary_side="LONG" if raw>0 else "SHORT"
    rsi15=f["15m"]["rsi"]
    chase_penalty=0.0
    chase_reason=None
    if preliminary_side=="SHORT" and rsi15<25:
        chase_penalty=min((25-rsi15)*1.5,20.0)
        chase_reason="SHORT_AFTER_OVERSOLD"
    elif preliminary_side=="LONG" and rsi15>75:
        chase_penalty=min((rsi15-75)*1.5,20.0)
        chase_reason="LONG_AFTER_OVERBOUGHT"
    score=round(max(abs(raw)*100-chase_penalty,0),1)
    side="NEUTRAL" if score<55 else preliminary_side

    # Candidate entry-quality gate. These thresholds are explicit heuristics
    # and must be validated by backtesting before any live execution.
    vol15=f["15m"]["volratio"]
    adx15=f["15m"]["adx"]
    oi_aligned=(oid>=0 if preliminary_side=="LONG" else oid<=0)
    gate_checks={
        "score": score>=70,
        "volume": vol15>=0.75,
        "adx": adx15>=25,
        "rsi": (30<=rsi15<=72) if preliminary_side=="LONG" else (28<=rsi15<=70),
        "oi": oi_aligned or abs(oid)<0.01,
    }
    gate_passed=all(gate_checks.values()) and side!="NEUTRAL"
    if not gate_passed:
        side="NEUTRAL"

    p=f["15m"]["price"]; atr15=f["15m"]["atr"]; risk=1.5*atr15
    candidate_stop=p-risk if preliminary_side=="LONG" else p+risk
    candidate_tp1=p+2*risk if preliminary_side=="LONG" else p-2*risk
    candidate_tp2=p+3*risk if preliminary_side=="LONG" else p-3*risk
    stop=candidate_stop if side!="NEUTRAL" else None
    tp1=candidate_tp1 if side!="NEUTRAL" else None
    tp2=candidate_tp2 if side!="NEUTRAL" else None
    return {"symbol":symbol,"side":side,"score":score,"entry":p,"stop":stop,"tp1":tp1,"tp2":tp2,
      "atr_15m":round(atr15,8),"candidate_entry":p,"candidate_stop":candidate_stop,
      "candidate_tp1":candidate_tp1,"candidate_tp2":candidate_tp2,
      "components":{"trend":round(trend,3),"momentum":round(momentum,3),"structure":round(structure,3),"volume":round(volume,3),"vwap":round(vwap,3),"derivatives":round(float(derivatives),3)},
      "anti_chase_penalty":round(chase_penalty,1),"anti_chase_reason":chase_reason,
      "entry_gate_passed":gate_passed,"entry_gate_checks":gate_checks,
      "candidate_side":preliminary_side,
      "rsi_15m":round(f["15m"]["rsi"],1),"adx_15m":round(f["15m"]["adx"],1),"volume_ratio":round(f["15m"]["volratio"],2),
      "funding":fr,"open_interest":float(oi_now),"oi_change_1h_pct":round(oid*100,2),
      "rr_tp1":2.0 if side!="NEUTRAL" else None,"rr_tp2":3.0 if side!="NEUTRAL" else None}

scan_lock=asyncio.Lock()

async def scan_once():
    global cache
    async with httpx.AsyncClient(headers={"User-Agent":"futures-trading-ai-v2-bybit","Accept":"application/json"}) as c:
        tickers=await get(c,"/v5/market/tickers",{"category":"linear"})
        instruments=await get(c,"/v5/market/instruments-info",{"category":"linear","limit":1000})
        active={x["symbol"] for x in instruments["list"]
                if x.get("status")=="Trading"
                and x.get("contractType")=="LinearPerpetual"
                and x.get("quoteCoin")=="USDT"}
        items=[x for x in tickers["list"] if x.get("symbol") in active]
        liquid=sorted(items,key=lambda x:float(x.get("turnover24h") or 0),reverse=True)

        # Stage 1: every active USDT perpetual enters the universe.
        # Cheap liquidity prefilter avoids running 3-timeframe analysis on hundreds
        # of illiquid contracts and keeps the public API within practical limits.
        min_turnover=float(os.getenv("MIN_TURNOVER_24H","1000000"))
        eligible=[x for x in liquid if float(x.get("turnover24h") or 0)>=min_turnover]
        deep_limit=max(1,int(os.getenv("DEEP_SCAN_PAIRS","60")))
        candidates=eligible[:deep_limit]
        symbols=[x["symbol"] for x in candidates]
        funding={x["symbol"]:x.get("fundingRate",0) or 0 for x in items}
        sem=asyncio.Semaphore(max(1,int(os.getenv("SCAN_CONCURRENCY","3"))))
        async def one(s):
            async with sem:
                try:
                    return await analyze(c,s,funding)
                except Exception as e:
                    print(f"PAIR_ERROR {s}: {type(e).__name__}: {e}",flush=True)
                    return None
        out=[x for x in await asyncio.gather(*(one(s) for s in symbols)) if x]

        # Final live-price validation: an analysis signal is not actionable if
        # price has already moved too far from its computed entry or crossed SL.
        live_price={x["symbol"]:float(x.get("lastPrice") or 0) for x in items}
        for sig in out:
            if sig["side"]=="NEUTRAL":
                continue
            lp=live_price.get(sig["symbol"],0)
            entry=sig["entry"]; stop=sig["stop"]
            risk=abs(stop-entry) if stop is not None else 0
            crossed_stop=(sig["side"]=="LONG" and lp<=stop) or (sig["side"]=="SHORT" and lp>=stop)
            drift_r=(abs(lp-entry)/risk) if lp>0 and risk>0 else 999
            price_valid=(not crossed_stop) and drift_r<=0.35
            sig["live_price"]=lp
            sig["entry_drift_r"]=round(drift_r,3)
            sig["price_validation_passed"]=price_valid
            sig["price_validation_reason"]=None if price_valid else ("STOP_ALREADY_CROSSED" if crossed_stop else "PRICE_TOO_FAR_FROM_ENTRY")
            if not price_valid:
                sig["side"]="NEUTRAL"
                sig["stop"]=sig["tp1"]=sig["tp2"]=None
                sig["rr_tp1"]=sig["rr_tp2"]=None

        ranked=sorted((x for x in out if x["side"]!="NEUTRAL"),key=lambda x:x["score"],reverse=True)

        # Diagnostic near-misses do not relax the trading gate.
        near=[]
        for x in out:
            if x["side"]!="NEUTRAL":
                continue
            checks=x.get("entry_gate_checks",{})
            failed_checks=[k for k,v in checks.items() if not v]
            # Prefer candidates failing the fewest checks, then the highest score.
            if checks:
                near.append({
                    "symbol":x["symbol"],"candidate_side":x.get("candidate_side"),
                    "score":x["score"],"failed_checks":failed_checks,
                    "passed_checks":sum(1 for v in checks.values() if v),
                    "rsi_15m":x["rsi_15m"],"adx_15m":x["adx_15m"],
                    "volume_ratio":x["volume_ratio"],
                    "oi_change_1h_pct":x["oi_change_1h_pct"],
                    "anti_chase_penalty":x["anti_chase_penalty"],
                    "entry":x.get("entry"),"stop":x.get("stop"),
                    "tp1":x.get("tp1"),"tp2":x.get("tp2"),
                    "atr_15m":x.get("atr_15m"),
                    "candidate_entry":x.get("candidate_entry"),
                    "candidate_stop":x.get("candidate_stop"),
                    "candidate_tp1":x.get("candidate_tp1"),
                    "candidate_tp2":x.get("candidate_tp2"),
                    "rr_tp1":x.get("rr_tp1"),"rr_tp2":x.get("rr_tp2"),
                    "live_price":x.get("live_price"),
                    "entry_drift_r":x.get("entry_drift_r"),
                    "price_validation_passed":x.get("price_validation_passed"),
                    "price_validation_reason":x.get("price_validation_reason"),
                    "status":"UNCONFIRMED"
                })
        near=sorted(near,key=lambda x:(-x["passed_checks"],-x["score"]))[:10]

        failed=len(symbols)-len(out)
        status="ok" if out and failed==0 else ("partial" if out else "error")
        cache={"status":status,"version":"2.10.0","updated_at":datetime.now(timezone.utc).isoformat(),
               "market_universe":len(active),"eligible_pairs":len(eligible),
               "deep_scan_candidates":len(symbols),"pairs_scanned":len(out),"failed_pairs":failed,
               "actionable_signals":len(ranked),"signals":ranked[:10],
               "near_misses":near}

async def safe_scan_once():
    if scan_lock.locked():
        return False
    async with scan_lock:
        await scan_once()
    return True

async def loop():
    while True:
        try: await safe_scan_once()
        except Exception as e:
            print(f"SCAN_ERROR {type(e).__name__}: {e}", flush=True)
            cache.update(status="error",error=f"{type(e).__name__}: {e}")
        await asyncio.sleep(max(int(os.getenv("SCAN_SECONDS","300")),300))

@app.on_event("startup")
async def startup(): asyncio.create_task(loop())
@app.get("/health")
def health(): return {"ok": True, "service": "futures-trading-ai-v2", "scan_status": cache["status"]}
@app.get("/")
def root(): return {"service":"futures-trading-ai-v2","mode":"SIGNAL_ONLY","status":cache["status"],"updated_at":cache["updated_at"]}
@app.get("/binodex-symbols")
async def binodex_symbols():
    key=os.getenv("OTCHARTS_API_KEY")
    if not key:
        return {"ok":False,"error":"API_KEY_MISSING"}
    try:
        async with httpx.AsyncClient(timeout=12) as c:
            r=await c.get("https://otcharts.com/v1/symbols",
                params={"venue":"binodex"},
                headers={"Authorization":"Bearer "+key})
        if r.status_code!=200:
            return {"ok":False,"http_status":r.status_code,"error":"SYMBOLS_FAILED"}
        j=r.json()
        return {"ok":True,"venue":"binodex","count":j.get("count"),
                "symbols":j.get("symbols",[])}
    except Exception as e:
        return {"ok":False,"error":type(e).__name__}

@app.get("/binodex-status")
async def binodex_status():
    key=os.getenv("OTCHARTS_API_KEY")
    if not key:
        return {"connected":False,"binodex_enabled":False,"error":"API_KEY_MISSING"}
    try:
        async with httpx.AsyncClient(timeout=12) as c:
            r=await c.get("https://otcharts.com/v1/usage",
                headers={"Authorization":"Bearer "+key})
        if r.status_code!=200:
            return {"connected":False,"binodex_enabled":False,
                    "http_status":r.status_code,"error":"AUTH_FAILED"}
        j=r.json()
        books=j.get("books",[])
        return {"connected":True,"binodex_enabled":"binodex" in books,
                "plan":j.get("planName") or j.get("plan"),
                "books":books,"requests":j.get("requests"),
                "streams":j.get("streams")}
    except Exception as e:
        return {"connected":False,"binodex_enabled":False,
                "error":type(e).__name__}

@app.get("/health")
def health():
    freshness=scan_freshness()
    return {"ok":freshness["fresh"],"scanner":cache["status"],"version":cache["version"],"pairs_scanned":cache["pairs_scanned"],"error":cache.get("error"),"updated_at":cache.get("updated_at"),**freshness}
@app.get("/bybit-health")
async def bybit_health():
    try:
        async with httpx.AsyncClient(timeout=12) as c:
            result=await get(c,"/v5/market/time")
        return {"ok":True,"provider":"bybit","server_time":result.get("timeSecond")}
    except Exception as e:
        return {"ok":False,"provider":"bybit","error":str(e)[:250]}

def scan_freshness():
    stamp=cache.get("updated_at")
    if not stamp:
        return {"fresh":False,"age_seconds":None,"max_age_seconds":max(900,2*int(os.getenv("SCAN_SECONDS","300")))}
    try:
        age=max(0,(datetime.now(timezone.utc)-datetime.fromisoformat(stamp.replace("Z","+00:00"))).total_seconds())
        max_age=max(900,2*int(os.getenv("SCAN_SECONDS","300")))
        return {"fresh":age<=max_age and cache.get("status")=="ok","age_seconds":round(age),"max_age_seconds":max_age}
    except (ValueError,TypeError):
        return {"fresh":False,"age_seconds":None,"max_age_seconds":900}

@app.get("/signals")
def signals():
    freshness=scan_freshness()
    if not freshness["fresh"]:
        return {**cache,**freshness,"actionable_signals":0,"signals":[],"status":"stale","warning":"LAST_SCAN_NOT_FRESH"}
    return {**cache,**freshness}
@app.get("/evaluation-candles")
async def evaluation_candles(symbol: str, start_ms: int, horizon: int = 24):
    """Read-only historical closed 15m candles for offline signal evaluation."""
    from fastapi import HTTPException
    import re
    if not re.fullmatch(r"[A-Z0-9]{2,25}USDT", symbol) or not 1 <= horizon <= 200:
        raise HTTPException(status_code=400, detail="Invalid symbol or horizon")
    interval_ms = 900_000
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    if start_ms % interval_ms or start_ms <= 0 or start_ms + horizon * interval_ms > now_ms:
        raise HTTPException(status_code=400, detail="Candles not fully closed or invalid start")
    try:
        async with httpx.AsyncClient(timeout=20) as c:
            data = await get(c, "/v5/market/kline", {
                "category": "linear", "symbol": symbol, "interval": "15",
                "start": start_ms, "end": start_ms + (horizon - 1) * interval_ms,
                "limit": horizon})
        bars = sorted([
            {"start_ms": int(x[0]), "open": float(x[1]), "high": float(x[2]),
             "low": float(x[3]), "close": float(x[4])}
            for x in data["list"]
            if start_ms <= int(x[0]) < start_ms + horizon * interval_ms
            and int(x[0]) + interval_ms <= now_ms
        ], key=lambda x: x["start_ms"])
        return {"symbol": symbol, "candles": bars, "count": len(bars)}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Historical candles unavailable: {type(e).__name__}")

@app.post("/scan")
async def scan():
    ran=await safe_scan_once()
    return {**signals(),"scan_executed":ran,"warning":"SCAN_ALREADY_RUNNING" if not ran else None}

@app.get("/historical-probe")
async def historical_probe(symbol: str = "BTCUSDT", days_ago: int = 30):
    """Read-only availability check; no orders and no performance claims."""
    from fastapi import HTTPException
    import re
    if symbol not in ("BTCUSDT", "ETHUSDT") or not 1 <= days_ago <= 90:
        raise HTTPException(status_code=400, detail="Invalid probe arguments")
    from datetime import timedelta
    start_ms = int((datetime.now(timezone.utc) - timedelta(days=days_ago)).timestamp() * 1000)
    probes = {
        "kline_15m": ("/v5/market/kline", {"category":"linear","symbol":symbol,"interval":"15","start":start_ms,"limit":2}),
        "funding_history": ("/v5/market/funding/history", {"category":"linear","symbol":symbol,"endTime":start_ms,"limit":2}),
        "open_interest_history": ("/v5/market/open-interest", {"category":"linear","symbol":symbol,"intervalTime":"15min","startTime":start_ms,"endTime":start_ms + 3600000,"limit":2}),
    }
    results = {}
    async with httpx.AsyncClient(timeout=20) as client:
        for label, (path, params) in probes.items():
            try:
                payload = await get(client, path, params)
                rows = payload.get("list", [])
                timestamps = []
                for row in rows:
                    raw = (row.get("timestamp") or row.get("fundingRateTimestamp") or row.get("startTime")) if isinstance(row, dict) else (row[0] if row else None)
                    if raw is not None:
                        try:
                            timestamps.append(int(raw))
                        except (ValueError, TypeError):
                            pass
                results[label] = {"ok":bool(rows),"count":len(rows),
                                  "earliest_timestamp_ms":min(timestamps) if timestamps else None,
                                  "latest_timestamp_ms":max(timestamps) if timestamps else None,
                                  "target_timestamp_ms":start_ms,
                                  "covers_target_sample":bool(timestamps) and min(timestamps) <= start_ms <= max(timestamps),
                                  "near_target":bool(timestamps) and min(abs(t-start_ms) for t in timestamps) <= (12 * 3600 * 1000 if label == "funding_history" else 60 * 60 * 1000)}
            except Exception as exc:
                results[label] = {"ok":False,"error_type":type(exc).__name__}
    return {"symbol":symbol,"days_ago":days_ago,"results":results,
            "all_available":all(v["ok"] for v in results.values()),
            "historical_sample_aligned":all(v.get("near_target",False) for v in results.values()),
            "backtest_completed":False,"validated_edge":False}


@app.get("/historical-coverage-window")
async def historical_coverage_window(symbol: str = "BTCUSDT", days_ago: int = 30, hours: int = 6):
    """Read-only bounded history completeness check; no trading or backtest claims."""
    from fastapi import HTTPException
    if symbol not in ("BTCUSDT", "ETHUSDT") or not 1 <= days_ago <= 90 or not 1 <= hours <= 12:
        raise HTTPException(status_code=400, detail="Invalid historical window")
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    end_ms = ((now_ms - days_ago * 86400000) // 900000) * 900000
    start_ms = end_ms - hours * 3600000
    out = {}
    specs = {
        "kline_15m": ("/v5/market/kline", {"category":"linear","symbol":symbol,"interval":"15","start":start_ms,"end":end_ms-1,"limit":200}, 900000),
        "open_interest_15m": ("/v5/market/open-interest", {"category":"linear","symbol":symbol,"intervalTime":"15min","startTime":start_ms,"endTime":end_ms-1,"limit":200}, 900000),
        "kline_1h": ("/v5/market/kline", {"category":"linear","symbol":symbol,"interval":"60","start":start_ms,"end":end_ms-1,"limit":200}, 3600000),
        "kline_4h": ("/v5/market/kline", {"category":"linear","symbol":symbol,"interval":"240","start":start_ms,"end":end_ms-1,"limit":200}, 14400000),
        "funding": ("/v5/market/funding/history", {"category":"linear","symbol":symbol,"startTime":start_ms,"endTime":end_ms-1,"limit":200}, None)
    }
    async with httpx.AsyncClient(timeout=20) as client:
        for name,(path,params,step) in specs.items():
            try:
                rows = (await get(client,path,params)).get("list",[])
                stamps = set()
                for row in rows:
                    raw = (row.get("timestamp") or row.get("fundingRateTimestamp")) if isinstance(row,dict) else row[0]
                    stamp = int(raw)
                    if start_ms <= stamp < end_ms:
                        stamps.add(stamp)
                expected_stamps = list(range(((start_ms + step - 1)//step)*step, end_ms, step)) if step else None
                missing = [t for t in expected_stamps if t not in stamps] if expected_stamps is not None else None
                out[name] = {"count":len(stamps),"expected":len(expected_stamps) if expected_stamps is not None else None,
                    "missing_count":len(missing) if missing is not None else None,
                    "complete":not missing if missing is not None else None,
                    "first_ms":min(stamps) if stamps else None,"last_ms":max(stamps) if stamps else None}
            except Exception as exc:
                out[name]={"error":type(exc).__name__,"complete":False}
    return {"symbol":symbol,"days_ago":days_ago,"hours":hours,"start_ms":start_ms,"end_ms":end_ms,
            "results":out,"window_complete":all(v.get("complete") is True for k,v in out.items() if k!="funding"),
            "backtest_completed":False,"validated_edge":False}
