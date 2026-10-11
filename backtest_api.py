"""Dedicated read-only API for bounded historical replay chunks.
Separated from the production scanner so long backtests cannot kill scanner state.
"""
import asyncio, time
from fastapi import FastAPI, HTTPException
import historical_replay_chunk as hrc

BAR = 900_000
app = FastAPI(title="Futures Backtest Chunks", version="1.1")
state = {"status": "idle", "read_only": True}
job_lock = asyncio.Lock()

@app.get("/health")
async def health():
    return {"ok": True, "status": state.get("status"), "read_only": True}

async def worker(symbol, days, end_days_ago, initial_busy_until_ms, anchor_ms):
    global state
    original_time = hrc.time.time
    try:
        # Freeze the replay's definition of "now" so adjacent chunks share
        # exactly one 15-minute anchor and have no drift, gaps, or overlaps.
        hrc.time.time = lambda: anchor_ms / 1000.0
        result = await hrc.replay_chunk(
            symbol=symbol,
            days=days,
            end_days_ago=end_days_ago,
            initial_busy_until_ms=initial_busy_until_ms,
        )
        result["anchor_ms"] = anchor_ms
        state = {"status": "finished", "read_only": True, "report": result}
    except Exception as exc:
        state = {"status": "error", "read_only": True,
                 "error_type": type(exc).__name__, "error": str(exc)[:500],
                 "anchor_ms": anchor_ms}
    finally:
        hrc.time.time = original_time
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
