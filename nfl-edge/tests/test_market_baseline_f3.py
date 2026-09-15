"""Tests for F3 market baseline helpers."""
import sys, os, unittest
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ops"))
import market_baseline_f3 as f3

class EdgeDistTests(unittest.TestCase):
    def test_basic(self):
        d = f3.edge_dist([0.0, 0.1, 0.1, 0.05, 0.2])
        self.assertEqual(d["n"], 5)
        self.assertEqual(d["mean_abs_edge"], 0.09)
        self.assertEqual(d["p50"], 0.1)
        # breakeven at -110 is 52.4%, edge > 0.024
        self.assertEqual(d["frac_gt_024"], 0.8)  # 4/5 > 0.024

    def test_empty(self):
        self.assertEqual(f3.edge_dist([]), {})

    def test_no_large_edges(self):
        # Market probs cluster near 0.5; large edges should be rare
        d = f3.edge_dist([0.01] * 100)
        self.assertEqual(d["frac_gt_05"], 0.0)
        self.assertEqual(d["frac_gt_10"], 0.0)

class SummarizeTests(unittest.TestCase):
    def test_basic(self):
        s = f3.summarize([(1.0, 2024, "early"), (0.0, 2024, "early")])
        self.assertEqual(s["rate"], 0.5)
        self.assertEqual(s["n"], 2)

    def test_empty(self):
        s = f3.summarize([])
        self.assertIsNone(s["rate"])
        self.assertEqual(s["n"], 0)

if __name__ == "__main__":
    unittest.main()
