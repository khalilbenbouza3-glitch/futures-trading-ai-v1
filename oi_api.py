"""Dedicated read-only API for untouched OI-consistency holdout tests."""
import asyncio, time
from fastapi import FastAPI, HTTPException
import historical_replay_chunk as hrc
from oi_consistency_replay import replay_chunk_oi_consistent

BAR=900_000
DAY=86_400_000
app=FastAPI(title="OI Consistency Holdout",version="1.0")
baseline_state={"status":"idle","read_only":True}
candidate_state={"status":"idle","read_only":True}
baseline_lock=asyncio.Lock()
candidate_lock=asyncio.Lock()

async def run_boundary(call,anchor_ms):
    original_time=hrc.time.time
    original_series=hrc.series
    async def padded_series(client,symbol,kind,start,end):
        if kind in ("oi","funding"):
            start-=DAY
        return await original_series(client,symbol,kind,start,end)
    try:
        hrc.time.time=lambda: anchor_ms/1000.0
        hrc.series=padded_series
        r=await call()
        r["anchor_ms"]=anchor_ms
        r["derivative_boundary_lookback_days"]=1
        return r
    finally:
        hrc.time.time=original_time
        hrc.series=original_series

def validate(symbol,days,end_days_ago,anchor_ms):
    if symbol not in ("BTCUSDT","ETHUSDT"):
        raise HTTPException(status_code=400,detail="Unsupported symbol")
    if not 1<=days<=30 or not 1<=end_days_ago<=120:
        raise HTTPException(status_code=400,detail="Invalid window")
    current=(int(time.time()*1000)//BAR)*BAR
    if anchor_ms<=0 or anchor_ms%BAR or anchor_ms>current:
        raise HTTPException(status_code=400,detail="Invalid anchor_ms")

async def baseline_worker(symbol,days,end_days_ago,initial_busy_until_ms,anchor_ms):
    global baseline_state
    try:
        r=await run_boundary(lambda: hrc.replay_chunk(symbol,days,end_days_ago,initial_busy_until_ms),anchor_ms)
        baseline_state={"status":"finished","read_only":True,"report":r}
    except Exception as exc:
        baseline_state={"status":"error","read_only":True,"error_type":type(exc).__name__,"error":str(exc)[:500]}
    finally:
        if baseline_lock.locked(): baseline_lock.release()

@app.post("/baseline/start")
async def baseline_start(symbol:str,days:int=30,end_days_ago:int=120,initial_busy_until_ms:int=-1,anchor_ms:int=0):
    global baseline_state
    validate(symbol,days,end_days_ago,anchor_ms)
    if baseline_lock.locked(): return baseline_state
    await baseline_lock.acquire()
    baseline_state={"status":"running","read_only":True,"symbol":symbol,"anchor_ms":anchor_ms}
    asyncio.create_task(baseline_worker(symbol,days,end_days_ago,initial_busy_until_ms,anchor_ms))
    return baseline_state

@app.get("/baseline/status")
async def baseline_status(): return baseline_state

async def candidate_worker(symbol,days,end_days_ago,initial_busy_until_ms,anchor_ms):
    global candidate_state
    try:
        r=await run_boundary(lambda: replay_chunk_oi_consistent(symbol,days,end_days_ago,initial_busy_until_ms),anchor_ms)
        candidate_state={"status":"finished","read_only":True,"report":r}
    except Exception as exc:
        candidate_state={"status":"error","read_only":True,"error_type":type(exc).__name__,"error":str(exc)[:500]}
    finally:
        if candidate_lock.locked(): candidate_lock.release()

@app.post("/candidate/start")
async def candidate_start(symbol:str,days:int=30,end_days_ago:int=120,initial_busy_until_ms:int=-1,anchor_ms:int=0):
    global candidate_state
    validate(symbol,days,end_days_ago,anchor_ms)
    if candidate_lock.locked(): return candidate_state
    await candidate_lock.acquire()
    candidate_state={"status":"running","read_only":True,"symbol":symbol,"anchor_ms":anchor_ms}
    asyncio.create_task(candidate_worker(symbol,days,end_days_ago,initial_busy_until_ms,anchor_ms))
    return candidate_state

@app.get("/candidate/status")
async def candidate_status(): return candidate_state

@app.get("/health")
async def health():
    return {"ok":True,"baseline":baseline_state.get("status"),"candidate":candidate_state.get("status"),"read_only":True}
