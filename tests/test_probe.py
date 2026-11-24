"""Exercise the shopping probe's production entry point against deterministic test doubles."""
import importlib.util
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from observability.fixture import fixture
from observability.signals import APP_COMPONENTS, capture, post
from observability.stack import relay_job

ROOT = Path(__file__).resolve().parents[1]
# When deployed, the probe imports its two companions from the same mounted directory.
with patch.object(sys,"path",[str(ROOT/"observability"),*sys.path]):
    spec = importlib.util.spec_from_file_location("journey_probe",ROOT/"observability/probe.py")
    probe = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(probe)


class Reply(io.BytesIO):
    def __init__(self, data, status=200):
        super().__init__(data.encode())
        self.status = status
        self.headers = {"Location":"/cart"}


class ProbeTests(unittest.TestCase):
    def test_probe_checks_cookie_journeys_exact_trace_and_metrics(self):
        traces = {}
        def open_shop(req,timeout):
            tid = req.get_header("Traceparent").split("-")[1]
            route = req.full_url.split("http://shop")[1]
            j = {"/":"browse","/cart/checkout":"checkout","/cart":"cart"}[route]
            t = fixture(j,int(tid,16))
            for s in t["spans"]:
                s.update(startTime=500,duration=2,operationName="test")
            traces[tid] = t
            if req.data and route=="/cart":
                return Reply("",302)
            if route=="/cart/checkout":
                return Reply("Your order is complete!")
            return Reply("Online Boutique "+probe.PRODUCT+" Your shopping cart is empty!")
        def jaeger(url):
            return {"data":[traces[url.rsplit("/",1)[-1]]]}
        args = SimpleNamespace(shop="http://shop",jaeger="http://query",prometheus="http://prom",wait=0)
        result={"requests":[]}
        before = {s:{"value":2} for s in ("frontend","checkoutservice","productcatalogservice")}
        after = {s:{"value":3} for s in before}
        with patch.object(probe.request,"build_opener",return_value=SimpleNamespace(open=open_shop)), \
             patch.object(probe,"fetch_json",side_effect=jaeger), patch.object(probe,"collector_status",return_value=[]), \
             patch.object(probe,"counters",side_effect=[before,after]):
            probe.run(args,result)
        self.assertTrue(result["passed"])
        self.assertEqual(len(result["requests"]),5)
        self.assertEqual(len(traces),5)
        self.assertEqual(result["requests"][0]["lineage"]["server_records"][0]["start_us"],500)

    def test_capture_and_relay(self):
        def kube(*args):
            if args[0]=="logs":
                return '2026-01-01T00:00:00.100000001Z {"message":"order placed","session":"discard"}'
            return json.dumps({"items":[{"metadata":{"labels":{"app":s},"name":s+"-pod","uid":s},
                "spec":{"containers":[{"name":"server"}]},
                "status":{"conditions":[{"type":"Ready","status":"True"}],
                          "containerStatuses":[{"restartCount":0}]}} for s in APP_COMPONENTS]})
        with tempfile.TemporaryDirectory() as tmp, patch("observability.signals.kubectl",side_effect=kube):
            d=Path(tmp)/"capture"
            summary=capture("cct-probe-test","2026-01-01T00:00:00Z","2026-01-01T00:00:01Z",d)
            self.assertTrue(summary["capture_complete"])
            cfg,job=relay_job("cct-probe-test",d)
            self.assertNotIn("discard",cfg["data"]["logs.json"])
            self.assertEqual(job["metadata"]["name"],"cct-probe-test-relay")
            with self.assertRaises(ValueError):
                relay_job("cct-probe-wrong",d)
            with self.assertRaises(FileExistsError):
                capture("next","2026-01-01T00:00:00Z","2026-01-01T00:00:01Z",d)
        with patch("observability.signals.request.urlopen",return_value=Reply('{"partialSuccess":{"rejectedLogRecords":"2"}}')):
            with self.assertRaisesRegex(ValueError,"partial success"):
                post("http://collector","logs",{})
