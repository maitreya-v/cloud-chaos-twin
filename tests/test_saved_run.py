import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from analytics.demo import BASE, SERVICES, fixture
from analytics.forecast import predict
from analytics.health import fit
from analytics.io import read_json
from analytics.saved_run import EVIDENCE, build, render


class SavedRunTests(unittest.TestCase):
    def test_saved_receipt_and_private_timeline_provenance(self):
        receipt = read_json(EVIDENCE)
        summary = build(receipt)
        self.assertEqual([p["median_ms"] for p in summary["phase_latency"]], [0.274, 106.93, 0.282])
        self.assertEqual(sum(len(e["journeys"]) for e in summary["edges"]), 15)
        self.assertEqual(summary["nonempty_no_additional_cutoffs"], 11)
        self.assertEqual(summary["flagged_during_injection"], 6)
        self.assertIsNone(summary["five_second_labels"])
        run_id = summary["run_id"]
        relative = "runs/" + run_id + "/heldout-label-validation.json"
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / relative
            path.parent.mkdir(parents=True)
            detail = dict(run_id=run_id, counts=summary["label_counts"], timeline=[
                dict(end_ms=5000, independent_reference_labels={"frontend":"unknown"})])
            path.write_text(json.dumps(detail))
            with self.assertRaisesRegex(ValueError, "SHA-256"):
                build(receipt, Path(td))
            receipt["artifact_hashes"][relative] = hashlib.sha256(path.read_bytes()).hexdigest()
            self.assertIn("00:00:05", render(build(receipt, Path(td))))
        receipt["runs"][run_id]["capture"]["journey_examples"]["cart"]["matched_edges"] -= 1
        with self.assertRaisesRegex(ValueError, "Journey evidence"):
            build(receipt)

    def test_near_limit_uses_only_current_eligible_observations(self):
        spans, windows, split = fixture()
        cal = fit(windows, split, {"synthetic-train":[BASE, BASE + 30000]})
        # A healthy frontend close to its training limit, plus a later revision.
        cutoff = BASE + 30000
        rows = [dict(r) for r in windows]
        current = next(r for r in rows if r["run_id"] == "synthetic-test" and r["service"] == "frontend"
                       and r["end_ms"] == cutoff)
        current["p95_ms"] = 0.8 * cal["latency_limits_ms"]["frontend"]
        rows.append(dict(current, p95_ms=0, available_ms=cutoff+1))
        args = spans, rows, "synthetic-test", SERVICES, cutoff, cal
        baseline = predict(*args)
        candidate = predict(*args, near_limit=True)
        additions = [f for f in candidate["forecasts"] if f["method"] == "near_limit"]
        self.assertEqual([f["horizon_ms"] for f in additions], [30000])
        self.assertIn("frontend", additions[0]["predicted_new"])
        self.assertLessEqual(set(additions[0]["predicted_new"]), set(candidate["eligible"]))
        self.assertEqual(baseline["forecasts"], [f for f in candidate["forecasts"] if f["method"] != "near_limit"])
        current["complete"] = False
        trial = predict(*args, near_limit=True)
        self.assertNotIn("frontend", next(f for f in trial["forecasts"] if f["method"] == "near_limit")["predicted_new"])

