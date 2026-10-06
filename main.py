import asyncio, os
from datetime import datetime, timezone
import httpx, numpy as np, pandas as pd
from fastapi import FastAPI

BASE="https://api.bybit.com"
app=FastAPI(title="Futures Trading AI V2",version="2.2.0")
cache={"status":"starting","version":"2.2.0","updated_at":None,"pairs_scanned":0,"signals":[]}

async def get(c,path,params=None):
    r=await c.get(BASE+path,params=params,timeout=20); r.raise_for_status()
    j=r.json()
    if j.get("retCode",0)!=0: raise RuntimeError(f'Bybit {j.get("retCode")}: {j.get("retMsg")}')
    return j["result"]

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
        rows=[]
        for a in reversed(k["list"]):
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
    raw=float(np.clip(raw,-1,1)); score=round(abs(raw)*100,1)
    side="NEUTRAL" if score<55 else ("LONG" if raw>0 else "SHORT")
    p=f["15m"]["price"]; risk=1.5*f["15m"]["atr"]
    stop=tp1=tp2=None
    if side!="NEUTRAL":
        stop=p-risk if side=="LONG" else p+risk; tp1=p+2*risk if side=="LONG" else p-2*risk; tp2=p+3*risk if side=="LONG" else p-3*risk
    return {"symbol":symbol,"side":side,"score":score,"entry":p,"stop":stop,"tp1":tp1,"tp2":tp2,
      "components":{"trend":round(trend,3),"momentum":round(momentum,3),"structure":round(structure,3),"volume":round(volume,3),"vwap":round(vwap,3),"derivatives":round(float(derivatives),3)},
      "rsi_15m":round(f["15m"]["rsi"],1),"adx_15m":round(f["15m"]["adx"],1),"volume_ratio":round(f["15m"]["volratio"],2),
      "funding":fr,"open_interest":float(oi_now),"oi_change_1h_pct":round(oid*100,2),
      "rr_tp1":2.0 if side!="NEUTRAL" else None,"rr_tp2":3.0 if side!="NEUTRAL" else None}

async def scan_once():
    global cache
    async with httpx.AsyncClient(headers={"User-Agent":"futures-trading-ai-v2-bybit"}) as c:
        info,tickers,prem=await asyncio.gather(get(c,"/fapi/v1/exchangeInfo"),get(c,"/fapi/v1/ticker/24hr"),get(c,"/fapi/v1/premiumIndex"))
        valid={s["symbol"] for s in info["symbols"] if s["contractType"]=="PERPETUAL" and s["quoteAsset"]=="USDT" and s["status"]=="TRADING"}
        liquid=sorted((x for x in tickers if x["symbol"] in valid),key=lambda x:float(x["quoteVolume"]),reverse=True)
        symbols=[x["symbol"] for x in liquid[:int(os.getenv("SCAN_PAIRS","30"))]]
        funding={x["symbol"]:x.get("lastFundingRate",0) for x in prem}; sem=asyncio.Semaphore(2)
        async def one(s):
            async with sem:
                try:return await analyze(c,s,funding)
                except Exception:return None
        out=[x for x in await asyncio.gather(*(one(s) for s in symbols)) if x]
        ranked=sorted((x for x in out if x["side"]!="NEUTRAL"),key=lambda x:x["score"],reverse=True)
        cache={"status":"ok","version":"2.0.0","updated_at":datetime.now(timezone.utc).isoformat(),"pairs_scanned":len(out),
          "actionable_signals":len(ranked),"signals":ranked[:10]}

async def loop():
    while True:
        try: await scan_once()
        except Exception as e: cache.update(status="error",error=str(e))
        await asyncio.sleep(max(int(os.getenv("SCAN_SECONDS","600")),600))

@app.on_event("startup")
async def startup(): asyncio.create_task(loop())
@app.get("/")
def root(): return {"service":"futures-trading-ai-v2","mode":"SIGNAL_ONLY","status":cache["status"],"updated_at":cache["updated_at"]}
@app.get("/health")
def health(): return {"ok":True,"scanner":cache["status"],"version":cache["version"],"pairs_scanned":cache["pairs_scanned"]}
@app.get("/signals")
def signals(): return cache
@app.post("/scan")
async def scan(): await scan_once(); return cache
