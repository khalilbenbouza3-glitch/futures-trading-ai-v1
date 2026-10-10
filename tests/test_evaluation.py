import unittest
from datetime import datetime, timezone
from evaluation import evaluate_signal, summarize

STAMP = "2026-10-10T12:00:00+00:00"
START = int(datetime.fromisoformat(STAMP).timestamp() * 1000) + 900000


def signal(side="LONG", stop=95, tp1=110):
    return {"symbol": "TESTUSDT", "side": side, "stop": stop,
            "tp1": tp1, "updated_at": STAMP}


def candles(*bars):
    return [{"start_ms": START + i * 900000, "open": b[0], "high": b[1],
             "low": b[2], "close": b[3]} for i, b in enumerate(bars)]


class EvaluationTests(unittest.TestCase):
    def test_target_long(self):
        result = evaluate_signal(signal(), candles((100, 111, 99, 109)), horizon_bars=1)
        self.assertEqual(result["outcome"], "TARGET")
        self.assertGreater(result["net_r"], 0)

    def test_stop_priority_when_both_touched(self):
        result = evaluate_signal(signal(), candles((100, 111, 94, 100)), horizon_bars=1)
        self.assertEqual(result["outcome"], "STOP")

    def test_short_stop(self):
        result = evaluate_signal(signal("SHORT", 105, 90),
                                 candles((100, 106, 89, 99)), horizon_bars=1)
        self.assertEqual(result["outcome"], "STOP")

    def test_unresolved_not_counted_as_loss(self):
        result = evaluate_signal(signal(), [], horizon_bars=1)
        self.assertEqual(result["status"], "UNRESOLVED")
        self.assertIsNone(summarize([result])["win_rate"])

    def test_gap_through_stop(self):
        result = evaluate_signal(signal(), candles((92, 94, 90, 93)), horizon_bars=1)
        self.assertEqual(result["outcome"], "STOP")
        self.assertEqual(result["exit"], 92)

    def test_missing_candles(self):
        rows = candles((100, 102, 98, 101), (101, 103, 99, 102))
        rows[1]["start_ms"] += 900000
        self.assertEqual(evaluate_signal(signal(), rows, horizon_bars=2)["status"], "UNRESOLVED")

    def test_fee_slippage_reduce_return(self):
        bars = candles((100, 111, 99, 109))
        free = evaluate_signal(signal(), bars, horizon_bars=1, fee_bps=0, slippage_bps=0)
        cost = evaluate_signal(signal(), bars, horizon_bars=1, fee_bps=6, slippage_bps=3)
        self.assertLess(cost["net_return_pct"], free["net_return_pct"])

    def test_reject_invalid_stop(self):
        self.assertEqual(evaluate_signal(signal(stop=101), candles((100, 110, 99, 105)),
                                         horizon_bars=1)["status"], "INVALID")


if __name__ == "__main__":
    unittest.main()
