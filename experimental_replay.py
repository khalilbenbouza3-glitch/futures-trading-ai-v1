"""Experimental read-only replay filters layered over the frozen V2.10 analyzer.
Production main.analyze is not changed. The filter is applied only inside replay.
"""
import main
import historical_replay_chunk as hrc

async def replay_chunk_filtered(symbol, days=30, end_days_ago=1,
                                initial_busy_until_ms=-1,
                                max_aligned_volume=None):
    original_analyze=main.analyze
    rejected_by_aligned_volume=0

    async def filtered_analyze(client, symbol_name, funding):
        nonlocal rejected_by_aligned_volume
        result=await original_analyze(client, symbol_name, funding)
        if max_aligned_volume is not None and result.get("side") in ("LONG","SHORT"):
            direction=1 if result["side"]=="LONG" else -1
            aligned_volume=direction*float(result.get("components",{}).get("volume",0.0))
            result["experimental_aligned_volume"]=aligned_volume
            if aligned_volume>float(max_aligned_volume):
                rejected_by_aligned_volume+=1
                result=dict(result)
                result["side"]="NEUTRAL"
                result["stop"]=None
                result["tp1"]=None
                result["tp2"]=None
                result["rr_tp1"]=None
                result["rr_tp2"]=None
                result["experimental_rejection_reason"]="ALIGNED_VOLUME_TOO_STRONG"
        return result

    main.analyze=filtered_analyze
    try:
        report=await hrc.replay_chunk(symbol,days,end_days_ago,initial_busy_until_ms)
    finally:
        main.analyze=original_analyze

    report["experimental_filter"]={
        "max_aligned_volume":max_aligned_volume,
        "rejected_by_aligned_volume":rejected_by_aligned_volume,
        "production_analyzer_modified":False,
    }
    return report
