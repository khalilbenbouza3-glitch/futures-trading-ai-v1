# Evaluation protocol (research only)

The evaluator does not fetch prices or execute orders. Supply a JSON file with
`signals` (including `symbol`, `side`, `updated_at`, `stop`, `tp1`) and
`candles` keyed by symbol. Each candle requires `start_ms`, `open`,
`high`, `low`, and `close`. All timestamps are UTC.

Example:
```bash
python -m unittest discover -s tests -v
python evaluation.py evaluation-input.json --output evaluation-result.json --horizon-bars 24
```

**Data integrity requirements**
- Archive each signal at the instant it is published; never reconstruct historical
  signals from future candles.
- Obtain subsequent *completed* 15-minute Bybit candles, not the forming candle.
- Verify contiguous candles, symbol consistency, and exact entry-bar alignment.
- Only evaluate genuinely actionable LONG/SHORT signals. Rejected near-misses
  must not be counted as executed trades.
- The evaluator enters at the next 15-minute bar's open, not at the original
  scanner price. This is a hypothetical fill and may not be achievable.
- When a bar touches stop and target, stop wins. Gaps at the entry bar that
  invalidate the original stop/target setup are excluded.
- Default round-trip fees are 12 bps and assumed slippage is 3 bps each way;
  these are *assumptions*, not verified exchange-specific trading costs.
- TIMEOUT is exited at the final bar close; unresolved samples do not count.
- Funding, liquidity constraints, spreads, position sizing, and portfolio
  overlap are not yet modeled.
- Do not claim statistical advantage from unit tests. Evaluate a large,
  chronological out-of-sample dataset, include uncertainty intervals, and
  compare with a frozen baseline before accepting strategy changes.

No automatic trading is implemented.
