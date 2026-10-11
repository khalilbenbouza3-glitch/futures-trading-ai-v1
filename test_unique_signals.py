"""Regression checks for unique signal research metrics (standard library only)."""
import unittest
from analyze_unique_signals import analyze

def trade(symbol, side, entry_ms, exit_ms, net_r):
    return {"symbol": symbol, "side": side, "entry_bar_start_ms": entry_ms,
            "exit_bar_start_ms": exit_ms, "entry": 100.0, "exit": 101.0,
            "net_r": net_r}

class UniqueSignalTests(unittest.TestCase):
    def test_legacy_rows_are_excluded(self):
        result = analyze({"1": [{"symbol": "APTUSDT", "net_r": 1.5}]})
        self.assertEqual(result["legacy_untimed_rows_excluded"], 1)
        self.assertEqual(result["timed_unique_signals"], 0)
        self.assertIsNone(result["win_rate"])

    def test_exact_duplicate_across_scans(self):
        row = trade("APTUSDT", "LONG", 900000, 1800000, 1.5)
        result = analyze({"1": [row], "2": [dict(row)]})
        self.assertEqual(result["timed_unique_signals"], 1)
        self.assertEqual(result["timed_duplicates_removed"], 1)

    def test_distinct_and_overlapping(self):
        result = analyze({"1": [trade("APTUSDT", "LONG", 900000, 2700000, 1.5)],
                          "2": [trade("TIAUSDT", "LONG", 1800000, 3600000, -1.0)]})
        self.assertEqual(result["timed_unique_signals"], 2)
        self.assertEqual(result["overlapping_signals"], 1)
        self.assertAlmostEqual(result["mean_net_r"], 0.25)
        self.assertIsNone(result["max_drawdown"])

    def test_conflicting_same_identity_fails(self):
        row = trade("APTUSDT", "LONG", 900000, 1800000, 1.5)
        altered = dict(row, net_r=-1.0)
        with self.assertRaises(ValueError):
            analyze({"1": [row], "2": [altered]})

    def test_exit_before_entry_fails(self):
        with self.assertRaises(ValueError):
            analyze({"1": [trade("APTUSDT", "LONG", 1800000, 900000, 1.0)]})

if __name__ == "__main__":
    unittest.main()
