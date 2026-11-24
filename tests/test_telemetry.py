import copy
import json
import unittest
from pathlib import Path
from observability.fixture import benchmark, fixture
from observability.health import state
from observability.lineage import inspect_trace, coverage
from observability.stack import stack, probe_job
from observability.signals import nanos, selected_logs, pod_metrics
from observability.export_sample import sample
from observability.overhead import compare, load


class LineageTests(unittest.TestCase):
    def test_all_journeys(self):
        results = benchmark()["runs"]
        self.assertEqual([r["expected"] for r in results], [4,2,9])
        self.assertTrue(all(r["passes_declared_path"] and r["unexpected"] == 0 for r in results))
        self.assertEqual(sum(r["direct_matches"] for r in results), 0)

    def test_parent_order_duplicate_and_external_parent(self):
        trace = fixture("cart", 1)
        root = trace["spans"][0]
        root["references"] = [{"refType":"CHILD_OF","traceID":trace["traceID"],"spanID":"remote"}]
        trace["spans"].reverse()
        trace["spans"].append(copy.deepcopy(root))
        stats = inspect_trace(trace,trace["traceID"],"remote")
        self.assertEqual(stats["duplicates"], 1)
        self.assertTrue(coverage("cart",stats)["passes_declared_path"])
        self.assertFalse(coverage("cart",inspect_trace(trace,trace["traceID"]))["passes_declared_path"])
        trace["spans"][-1]["processID"]="cartservice"
        with self.assertRaisesRegex(ValueError, "conflicting"):
            inspect_trace(trace, trace["traceID"])

    def test_client_only_branch_and_orphan(self):
        trace = fixture("checkout", 2)
        trace["spans"] = [s for s in trace["spans"] if s["processID"] != "paymentservice"]
        stats = inspect_trace(trace,trace["traceID"])
        self.assertIn(["checkoutservice","paymentservice"],coverage("checkout",stats)["missing_edges"])
        trace["spans"] = [s for s in trace["spans"] if s["spanID"] != "0000000000000002"]
        stats = inspect_trace(trace,trace["traceID"])
        self.assertGreater(stats["unlinked_spans"], 0)
        self.assertGreater(stats["missing_parents"], 0)

    def test_cycle_and_cross_trace(self):
        trace = fixture("cart", 3)
        trace["spans"][0]["references"] = [dict(trace["spans"][1]["references"][0])]
        with self.assertRaisesRegex(ValueError,"cycle"):
            inspect_trace(trace,trace["traceID"])
        trace["spans"][0]["references"][0]["traceID"] = "other"
        with self.assertRaisesRegex(ValueError,"cross-trace"):
            inspect_trace(trace,trace["traceID"])


class SignalTests(unittest.TestCase):
    def test_selected_logs_interval_redaction_and_replay(self):
        stamp = "2026-11-01T10:02:03.000000001Z"
        start = nanos(stamp)
        lines = [stamp+' {"message":"order placed","email":"secret@example.com"}'] * 2
        def select(start_,end_,received):
            return selected_logs(lines+["unstructured"],start_,end_,received,"pod/server","r")
        first, counts = select(start,start+1,start+10)
        second,_ = select(start,start+1,start+20)
        self.assertEqual(counts,{"read":3,"selected":2,"malformed_or_unstructured":1})
        self.assertEqual(first[0]["attributes"],second[0]["attributes"])
        self.assertNotEqual(first[0]["attributes"],first[1]["attributes"])
        self.assertNotIn("secret",json.dumps(first))
        self.assertFalse(select(start-1,start,start)[0])

    def test_otlp_samples(self):
        d = sample()
        self.assertEqual(sum(len(g["scopeSpans"][0]["spans"]) for g in d["traces"]["resourceSpans"]),19)
        self.assertEqual(len(d["metrics"]["resourceMetrics"][0]["scopeMetrics"][0]["metrics"]),2)
        self.assertEqual(d["logs"]["resourceLogs"][0]["scopeLogs"][0]["logRecords"][0]["body"]["stringValue"],"shop.checkout")

    def test_missing_pod_state(self):
        p = {"metadata":{"labels":{"app":"frontend"},"uid":"p"},"status":{}}
        self.assertFalse(pod_metrics(p,123,"r")["scopeMetrics"][0]["metrics"])


class ScopeTests(unittest.TestCase):
    def test_stack_bounds_and_export_routes(self):
        base = stack()
        self.assertTrue(all(i["metadata"]["namespace"]=="cct-observability" for i in base))
        self.assertEqual({i["kind"] for i in base},{"ConfigMap","Deployment","Service"})
        for item in base:
            if item["kind"]=="Deployment":
                pod = item["spec"]["template"]["spec"]
                self.assertFalse(pod["automountServiceAccountToken"])
                self.assertTrue(pod["securityContext"]["runAsNonRoot"])
        def cfg(objs):
            return json.loads(next(i["data"]["config.yaml"] for i in objs if i["kind"]=="ConfigMap" and i["metadata"]["name"]=="cct-collector"))
        self.assertNotIn("kafka",cfg(base)["exporters"])
        connected = cfg(stack("broker:9092"))
        for signal in ("traces","metrics","logs"):
            self.assertIn("kafka",connected["service"]["pipelines"][signal]["exporters"])
            self.assertEqual(connected["exporters"]["kafka"][signal],{"encoding":"otlp_json","topic":"cct."+signal})
        self.assertEqual(connected["service"]["pipelines"]["traces/server"]["exporters"],["spanmetrics"])

    def test_probe_jobs(self):
        cfg,job = probe_job("cct-probe-test")
        self.assertIn("smoke_boutique.py",cfg["data"])
        self.assertEqual(job["spec"]["backoffLimit"],0)
        with self.assertRaises(ValueError):
            probe_job("../invalid")


class TimeTests(unittest.TestCase):
    def test_cutoffs_persistence_and_missing(self):
        def row(t):
            return dict(start_ms=t-5000,end_ms=t,available_ms=t,run_id="r",service="a",complete=True,
                        ready=True,restart_delta=0,requests=24,errors=0,p95_ms=150)
        rows=[row(5000),row(10000)]
        def label(rows):
            return state(rows,"r","a",10000,latency_limit_ms=100)
        self.assertEqual(label(rows),"affected")
        self.assertEqual(label(rows[1:]),"unknown")
        self.assertEqual(label([rows[0],dict(rows[1],requests=1)]),"unknown")
        self.assertEqual(label([dict(rows[1],p95_ms=10)]),"healthy")
        self.assertEqual(label([dict(rows[1],ready=False,requests=0)]),"affected")
        self.assertEqual(label([rows[0],dict(rows[1],available_ms=10001)]),"unknown")
        self.assertEqual(label([dict(rows[1],service="other")]),"unknown")

    def test_overhead_requires_two_matched_runs(self):
        base = load(Path(__file__).resolve().parents[1]/"docs/evidence/CCT-02-maitreya.json")
        with self.assertRaisesRegex(ValueError,"distinct"): compare(base,base)
        with self.assertRaisesRegex(ValueError,"seed"): compare(base,dict(base,seed=9))
        result = compare(base,dict(base,started_utc="synthetic-second",p95_ms=38))
        self.assertEqual(result["p95_ms"]["percent_change"],100)


if __name__ == "__main__":
    unittest.main()
