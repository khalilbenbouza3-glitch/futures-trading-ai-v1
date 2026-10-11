"""Dedicated read-only API for bounded historical replay chunks.
Separated from the production scanner so long backtests cannot kill scanner state.
"""
import asyncio
from fastapi import FastAPI, HTTPException
from historical_replay_chunk import replay_chunk

app = FastAPI(title="Futures Backtest Chunks", version="1.0")
state = {"status": "idle", "read_only": True}
job_lock = asyncio.Lock()

@app.get("/health")
async def health():
    return {"ok": True, "status": state.get("status"), "read_only": True}

async def worker(symbol, days, end_days_ago, initial_busy_until_ms):
    global state
    try:
        result = await replay_chunk(
            symbol=symbol,
            days=days,
            end_days_ago=end_days_ago,
            initial_busy_until_ms=initial_busy_until_ms,
        )
        state = {"status": "finished", "read_only": True, "report": result}
    except Exception as exc:
        state = {"status": "error", "read_only": True,
                 "error_type": type(exc).__name__, "error": str(exc)[:500]}
    finally:
        if job_lock.locked():
            job_lock.release()

@app.post("/chunk/start")
async def start_chunk(symbol: str, days: int = 30, end_days_ago: int = 1,
                      initial_busy_until_ms: int = -1):
    global state
    if symbol not in ("BTCUSDT", "ETHUSDT"):
        raise HTTPException(status_code=400, detail="Unsupported symbol")
    if not 1 <= days <= 30 or not 1 <= end_days_ago <= 120:
        raise HTTPException(status_code=400, detail="Invalid window")
    if job_lock.locked():
        return state
    await job_lock.acquire()
    state = {"status": "running", "read_only": True, "symbol": symbol,
             "days": days, "end_days_ago": end_days_ago,
             "initial_busy_until_ms": initial_busy_until_ms}
    asyncio.create_task(worker(symbol, days, end_days_ago, initial_busy_until_ms))
    return state

@app.get("/chunk/status")
async def chunk_status():
    return state
