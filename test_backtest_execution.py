"""Deterministic tests for conservative backtest execution assumptions."""
import unittest
from backtest_execution import simulate

T=1800000000000
def sig(side="LONG"):
    return {"symbol":"BTCUSDT","side":side,"signal_time_ms":T,
            "entry":100,"stop":98 if side=="LONG" else 102,
            "tp1":104 if side=="LONG" else 96}
def bars(high=101,low=99,open_=100,count=96):
    return [{"start_ms":T+900000*(i+1),"open":open_,"high":high,
             "low":low,"close":100} for i in range(count)]

class ExecutionTests(unittest.TestCase):
    def test_no_lookahead(self):
        b=bars()
        b[0]["start_ms"]=T
        self.assertEqual(simulate(sig(),b)["reason"],"NEXT_BAR_MISSING")
    def test_ambiguous_bar_stop_first(self):
        r=simulate(sig(),bars(high=105,low=97))
        self.assertEqual(r["exit_reason"],"STOP_FIRST_AMBIGUOUS")
        self.assertLess(r["net_return_ex_funding"],0)
    def test_tp(self):
        r=simulate(sig(),bars(high=105,low=99))
        self.assertEqual(r["exit_reason"],"TP1")
    def test_short_stop(self):
        r=simulate(sig("SHORT"),bars(high=103,low=95))
        self.assertEqual(r["exit_reason"],"STOP_FIRST_AMBIGUOUS")
    def test_reject_incomplete_horizon(self):
        r=simulate(sig(),bars(count=2))
        self.assertEqual(r["reason"],"INSUFFICIENT_EXIT_HORIZON")
    def test_gap_through_stop(self):\n        b=bars()\n        b[1]["open"]=95\n        b[1]["low"]=94\n        b[1]["high"]=96\n        r=simulate(sig(),b)\n        self.assertEqual(r["exit_reason"],"STOP_GAP")\n        self.assertLess(r["exit"],98)\n    def test_invalid_stop(self):
        x=sig();x["stop"]=101
        self.assertEqual(simulate(x,bars())["reason"],"INVALID_RISK")

if __name__=="__main__":
    unittest.main()
