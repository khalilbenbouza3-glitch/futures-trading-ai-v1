"""Dedicated read-only API for bounded historical replay chunks, diagnostics, and isolated experiments.
Separated from the production scanner so long backtests cannot kill scanner state.
"""
import asyncio, time
from fastapi import FastAPI, HTTPException
import historical_replay_chunk as hrc
from diagnostic_features import diagnose
from experimental_replay import replay_chunk_filtered

BAR = 900_000
DAY = 86_400_000
app = FastAPI(title="Futures Backtest Chunks", version="1.4")
state = {"status": "idle", "read_only": True}
job_lock = asyncio.Lock()
diagnostic_state = {"status":"idle","read_only":True}
diagnostic_lock = asyncio.Lock()
experiment_state = {"status":"idle","read_only":True}
experiment_lock = asyncio.Lock()

@app.get("/health")
async def health():
    return {"ok": True, "status": state.get("status"),
            "diagnostic_status":diagnostic_state.get("status"),
            "experiment_status":experiment_state.get("status"),
            "read_only": True}

async def _run_with_historical_boundary(call, anchor_ms):
    original_time = hrc.time.time
    original_series = hrc.series

    async def padded_series(client, symbol_name, kind, start, end):
        if kind in ("oi", "funding"):
            start -= DAY
        return await original_series(client, symbol_name, kind, start, end)

    try:
        hrc.time.time = lambda: anchor_ms / 1000.0
        hrc.series = padded_series
        result = await call()
        result["anchor_ms"] = anchor_ms
        result["derivative_boundary_lookback_days"] = 1
        return result
    finally:
        hrc.time.time = original_time
        hrc.series = original_series

async def worker(symbol, days, end_days_ago, initial_busy_until_ms, anchor_ms):
    global state
    try:
        result=await _run_with_historical_boundary(
            lambda: hrc.replay_chunk(symbol=symbol,days=days,end_days_ago=end_days_ago,
                                     initial_busy_until_ms=initial_busy_until_ms),
            anchor_ms)
        state = {"status": "finished", "read_only": True, "report": result}
    except Exception as exc:
        state = {"status": "error", "read_only": True,
                 "error_type": type(exc).__name__, "error": str(exc)[:500],
                 "anchor_ms": anchor_ms}
    finally:
        if job_lock.locked():
            job_lock.release()

@app.post("/chunk/start")
async def start_chunk(symbol: str, days: int = 30, end_days_ago: int = 1,
                      initial_busy_until_ms: int = -1, anchor_ms: int = 0):
    global state
    if symbol not in ("BTCUSDT", "ETHUSDT"):
        raise HTTPException(status_code=400, detail="Unsupported symbol")
    if not 1 <= days <= 30 or not 1 <= end_days_ago <= 120:
        raise HTTPException(status_code=400, detail="Invalid window")
    current_anchor = (int(time.time() * 1000) // BAR) * BAR
    if anchor_ms == 0:
        anchor_ms = current_anchor
    if anchor_ms % BAR or anchor_ms <= 0 or anchor_ms > current_anchor:
        raise HTTPException(status_code=400, detail="Invalid anchor_ms")
    if job_lock.locked():
        return state
    await job_lock.acquire()
    state = {"status": "running", "read_only": True, "symbol": symbol,
             "days": days, "end_days_ago": end_days_ago,
             "initial_busy_until_ms": initial_busy_until_ms,
             "anchor_ms": anchor_ms}
    asyncio.create_task(worker(symbol, days, end_days_ago,
                               initial_busy_until_ms, anchor_ms))
    return state

@app.get("/chunk/status")
async def chunk_status():
    return state

async def diagnostic_worker(symbol):
    global diagnostic_state
    try:
        result=await diagnose(symbol)
        diagnostic_state={"status":"finished","read_only":True,"report":result}
    except Exception as exc:
        diagnostic_state={"status":"error","read_only":True,"error_type":type(exc).__name__,"error":str(exc)[:500]}
    finally:
        if diagnostic_lock.locked():
            diagnostic_lock.release()

@app.post("/diagnose/start")
async def start_diagnostic(symbol:str):
    global diagnostic_state
    if symbol not in ("BTCUSDT","ETHUSDT"):
        raise HTTPException(status_code=400,detail="Unsupported symbol")
    if diagnostic_lock.locked():
        return diagnostic_state
    await diagnostic_lock.acquire()
    diagnostic_state={"status":"running","symbol":symbol,"read_only":True}
    asyncio.create_task(diagnostic_worker(symbol))
    return diagnostic_state

@app.get("/diagnose/status")
async def diagnostic_status():
    return diagnostic_state

async def experiment_worker(symbol,days,end_days_ago,initial_busy_until_ms,anchor_ms,max_aligned_volume):
    global experiment_state
    try:
        result=await _run_with_historical_boundary(
            lambda: replay_chunk_filtered(symbol=symbol,days=days,end_days_ago=end_days_ago,
                                          initial_busy_until_ms=initial_busy_until_ms,
                                          max_aligned_volume=max_aligned_volume),
            anchor_ms)
        experiment_state={"status":"finished","read_only":True,"report":result}
    except Exception as exc:
        experiment_state={"status":"error","read_only":True,"error_type":type(exc).__name__,
                          "error":str(exc)[:500],"anchor_ms":anchor_ms}
    finally:
        if experiment_lock.locked():
            experiment_lock.release()

@app.post("/experiment/start")
async def start_experiment(symbol:str,days:int=30,end_days_ago:int=1,
                           initial_busy_until_ms:int=-1,anchor_ms:int=0,
                           max_aligned_volume:float=0.60):
    global experiment_state
    if symbol not in ("BTCUSDT","ETHUSDT"):
        raise HTTPException(status_code=400,detail="Unsupported symbol")
    if not 1<=days<=30 or not 1<=end_days_ago<=120:
        raise HTTPException(status_code=400,detail="Invalid window")
    if not 0.0<=max_aligned_volume<=1.0:
        raise HTTPException(status_code=400,detail="Invalid max_aligned_volume")
    current_anchor=(int(time.time()*1000)//BAR)*BAR
    if anchor_ms==0:
        anchor_ms=current_anchor
    if anchor_ms%BAR or anchor_ms<=0 or anchor_ms>current_anchor:
        raise HTTPException(status_code=400,detail="Invalid anchor_ms")
    if experiment_lock.locked():
        return experiment_state
    await experiment_lock.acquire()
    experiment_state={"status":"running","read_only":True,"symbol":symbol,"days":days,
                      "end_days_ago":end_days_ago,"initial_busy_until_ms":initial_busy_until_ms,
                      "anchor_ms":anchor_ms,"max_aligned_volume":max_aligned_volume}
    asyncio.create_task(experiment_worker(symbol,days,end_days_ago,initial_busy_until_ms,
                                          anchor_ms,max_aligned_volume))
    return experiment_state

@app.get("/experiment/status")
async def experiment_status():
    return experiment_state
