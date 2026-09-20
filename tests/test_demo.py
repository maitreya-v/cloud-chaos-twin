import copy
import json
import tempfile
import unittest
from pathlib import Path

from cct.demo import load_spans, summarize, write_report

SAMPLE = Path(__file__).resolve().parents[1] / "data/samples/spans.json"


class DemoTests(unittest.TestCase):
    def load_modified(self, dataset):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "sample.json"
            path.write_text(json.dumps(dataset), encoding="utf-8")
            return load_spans(path)

    def test_known_graph_and_phase_results(self):
        graph, rows = summarize(load_spans(SAMPLE))
        self.assertEqual(graph["missing_parent_spans"], 0)
        self.assertEqual(graph["edges"], [
            {"source": "checkoutservice", "target": "cartservice", "call_count": 3},
            {"source": "frontend", "target": "checkoutservice", "call_count": 3},
        ])
        fault = [row for row in rows if row["phase"] == "fault"]
        self.assertEqual(len(fault), 3)
        self.assertTrue(all(row["error_fraction"] == 1 for row in fault))

    def test_replay_duplicates_do_not_double_count(self):
        dataset = json.loads(SAMPLE.read_text())
        dataset["spans"].append(copy.deepcopy(dataset["spans"][0]))
        self.assertEqual(len(self.load_modified(dataset)["spans"]), 9)
        dataset["spans"][-1]["duration_ms"] += 1
        with self.assertRaisesRegex(ValueError, "Conflicting duplicate"):
            self.load_modified(dataset)

    def test_parent_identity_is_scoped_to_trace(self):
        dataset = load_spans(SAMPLE)
        dataset["spans"][1]["trace_id"] = "unrelated-trace"
        graph, _ = summarize(dataset)
        self.assertEqual(graph["missing_parent_spans"], 2)
        edge = next(e for e in graph["edges"] if e["source"] == "frontend")
        self.assertEqual(edge["call_count"], 2)

    def test_invalid_duration_and_real_data_are_rejected(self):
        dataset = load_spans(SAMPLE)
        dataset["spans"][0]["duration_ms"] = -1
        with self.assertRaises(ValueError):
            self.load_modified(dataset)
        dataset = load_spans(SAMPLE)
        dataset["synthetic"] = False
        with self.assertRaises(ValueError):
            self.load_modified(dataset)

    def test_report_outputs_and_html_escaping(self):
        dataset = load_spans(SAMPLE)
        dataset["spans"][0]["service"] = "<script>alert(1)</script>"
        with tempfile.TemporaryDirectory() as directory:
            report = write_report(dataset, directory)
            text = report.read_text()
            self.assertIn("Synthetic demo data", text)
            self.assertNotIn("<script>alert(1)</script>", text)
            self.assertIn("&lt;script&gt;", text)
            self.assertTrue((Path(directory) / "graph.json").exists())
            self.assertTrue((Path(directory) / "service_summary.csv").exists())


if __name__ == "__main__":
    unittest.main()
