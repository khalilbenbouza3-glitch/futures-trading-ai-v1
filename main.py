import asyncio, os
from datetime import datetime, timezone
import httpx, numpy as np, pandas as pd
from fastapi import FastAPI

BASE="https://fapi.binance.com"
app=FastAPI(title="Futures Trading AI V1",version="1.0.0")
cache={"status":"starting","updated_at":None,"pairs_scanned":0,"signals":[]}

async def get(c,path,params=None):
    r=await c.get(BASE+path,params=params,timeout=20); r.raise_for_status(); return r.json()

def ind(rows):
    d=pd.DataFrame(rows,columns=["t","o","h","l","c","v","ct","q","n","tb","tq","x"])
    for x in ["h","l","c","v"]: d[x]=pd.to_numeric(d[x])
    c,h,l,v=d.c,d.h,d.l,d.v
    e20=c.ewm(span=20).mean(); e50=c.ewm(span=50).mean(); e200=c.ewm(span=200).mean()
    z=c.diff(); g=z.clip(lower=0).ewm(alpha=1/14,adjust=False).mean(); loss=(-z.clip(upper=0)).ewm(alpha=1/14,adjust=False).mean()
    rsi=100-(100/(1+g/loss.replace(0,np.nan)))
    tr=pd.concat([h-l,(h-c.shift()).abs(),(l-c.shift()).abs()],axis=1).max(axis=1)
    atr=tr.ewm(alpha=1/14,adjust=False).mean()
    trend=1 if c.iloc[-1]>e20.iloc[-1]>e50.iloc[-1] else (-1 if c.iloc[-1]<e20.iloc[-1]<e50.iloc[-1] else 0)
    macro=1 if c.iloc[-1]>e200.iloc[-1] else -1
    mom=1 if rsi.iloc[-1]>=55 else (-1 if rsi.iloc[-1]<=45 else 0)
    return dict(price=float(c.iloc[-1]),atr=float(atr.iloc[-1]),rsi=float(rsi.iloc[-1]),trend=trend,macro=macro,mom=mom,vol=float(v.iloc[-1]/max(v.rolling(20).mean().iloc[-1],1e-12)))

async def analyze(c,symbol,funding):
    f={}
    for tf in ("15m","1h","4h"): f[tf]=ind(await get(c,"/fapi/v1/klines",{"symbol":symbol,"interval":tf,"limit":220}))
    oi=await get(c,"/fapi/v1/openInterest",{"symbol":symbol})
    w={"15m":.25,"1h":.35,"4h":.40}
    raw=sum(w[t]*(.55*f[t]["trend"]+.25*f[t]["macro"]+.20*f[t]["mom"]) for t in w)
    fr=float(funding.get(symbol,0)); raw+=(-.10 if fr>.0005 else (.10 if fr<-.0005 else 0))
    raw=max(-1,min(1,raw)); side="LONG" if raw>0 else "SHORT"; p=f["15m"]["price"]; risk=1.5*f["15m"]["atr"]
    stop=p-risk if side=="LONG" else p+risk
    return {"symbol":symbol,"side":side,"score":round(abs(raw)*100,1),"entry":p,"stop":stop,
      "tp1":p+2*risk if side=="LONG" else p-2*risk,"tp2":p+3*risk if side=="LONG" else p-3*risk,
      "rsi_15m":round(f["15m"]["rsi"],1),"volume_ratio":round(f["15m"]["vol"],2),"funding":fr,
      "open_interest":float(oi["openInterest"]),"rr_tp1":2.0,"rr_tp2":3.0}

async def scan_once():
    global cache
    async with httpx.AsyncClient(headers={"User-Agent":"futures-trading-ai-v1"}) as c:
        info,tickers,prem=await asyncio.gather(get(c,"/fapi/v1/exchangeInfo"),get(c,"/fapi/v1/ticker/24hr"),get(c,"/fapi/v1/premiumIndex"))
        valid={s["symbol"] for s in info["symbols"] if s["contractType"]=="PERPETUAL" and s["quoteAsset"]=="USDT" and s["status"]=="TRADING"}
        liquid=sorted((x for x in tickers if x["symbol"] in valid),key=lambda x:float(x["quoteVolume"]),reverse=True)
        symbols=[x["symbol"] for x in liquid[:int(os.getenv("SCAN_PAIRS","30"))]]
        funding={x["symbol"]:x.get("lastFundingRate",0) for x in prem}; sem=asyncio.Semaphore(5)
        async def one(s):
            async with sem:
                try:return await analyze(c,s,funding)
                except Exception:return None
        out=[x for x in await asyncio.gather(*(one(s) for s in symbols)) if x]
        out.sort(key=lambda x:x["score"],reverse=True)
        cache={"status":"ok","updated_at":datetime.now(timezone.utc).isoformat(),"pairs_scanned":len(out),"signals":out[:10]}

async def loop():
    while True:
        try: await scan_once()
        except Exception as e: cache.update(status="error",error=str(e))
        await asyncio.sleep(int(os.getenv("SCAN_SECONDS","300")))

@app.on_event("startup")
async def startup(): asyncio.create_task(loop())
@app.get("/")
def root(): return {"service":"futures-trading-ai-v1","mode":"SIGNAL_ONLY","status":cache["status"],"updated_at":cache["updated_at"]}
@app.get("/health")
def health(): return {"ok":True,"scanner":cache["status"],"pairs_scanned":cache["pairs_scanned"]}
@app.get("/signals")
def signals(): return cache
@app.post("/scan")
async def scan(): await scan_once(); return cache
