import gzip
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from ingestion.pilot import SOURCES, adapt, read_envelopes
from ingestion.contract import TOPICS
from observability.export_sample import sample
from observability.fixture import fixture, PATHS
from observability.lineage import inspect_trace
from processing.normalize import normalize


class PilotArchiveTests(unittest.TestCase):
    def make_source(self, directory):
        p=Path(directory)/"source";p.mkdir();(p/"signals").mkdir()
        run="fixture-checkout"
        server_count=0
        with gzip.open(p/"traces.jsonl.gz","wt") as out:
            for i,journey in enumerate(PATHS,1):
                trace=fixture(journey,i)
                for s in trace["spans"]:
                    s.update(startTime=1790000000000000,duration=10,operationName="rpc")
                server_count+=inspect_trace(trace,trace["traceID"])["server_spans"]
                out.write(json.dumps({"trace_id":trace["traceID"],"trace":trace,
                                      "client":{"run_id":run,"external_parent":"e"*16},
                                      "observed_ms":1790000001000+i})+"\n")
        receipt={"run_id":run,"passed":True,"remaining_fault":False,"remaining_workload":False,"cleanup_errors":[],
                 "capture":{"saved_traces":3,"server_records":server_count},"fault":{"requested_at":"2026-11-25T05:21:00Z"},
                 "finished_at":"2026-11-25T05:23:00Z","actor":"unit-test fixture", "source_revision":"fixture",
                 "measurement_interval_utc":["2026-11-25T05:20:00Z","2026-11-25T05:22:00Z"]}
        (p/"receipt.json").write_text(json.dumps(receipt))
        labels={"__name__":"cct_calls_total","service_name":"frontend","span_kind":"SPAN_KIND_SERVER"}
        prom={"observed_ms":1790000000500,"samples":{"resultType":"matrix","result":[{"metric":labels,"values":[[1790000000.125,"13"]]}]}}
        (p/"prometheus.jsonl").write_text((json.dumps(prom)+"\n")*2)
        (p/"pods.jsonl").write_text('{}\n')
        md={"annotations":{"cct.run_id":run}}
        spec={"selector":{"labelSelectors":{"app":"productcatalogservice"}},"duration":"30s"}
        (p/"fault.request.json").write_text(json.dumps({"metadata":md,"spec":spec}))
        recovered={"metadata":md,"spec":spec,"status":{"conditions":[{"type":"AllRecovered","status":"True"}],
                   "experiment":{"containerRecords":[{"id":"cct-app/productcatalogservice-fixture","phase":"Not Injected","events":[
                       {"operation":"Apply","type":"Succeeded","timestamp":"2026-11-25T05:21:00Z"},
                       {"operation":"Recover","type":"Succeeded","timestamp":"2026-11-25T05:21:30Z"}]}]}}}
        (p/"fault.recovered.json").write_text(json.dumps(recovered))
        for name in ("metrics","logs"):
            (p/"signals"/(name+".json")).write_text(json.dumps(sample()[name]))
        self.hash_source(p)
        return p

    def hash_source(self,p):
        (p/"file-hashes.json").write_text(json.dumps({n:hashlib.sha256((p/n).read_bytes()).hexdigest() for n in SOURCES}))

    def test_complete_replay_archive_preserves_identity_and_uncertainty(self):
        with tempfile.TemporaryDirectory() as tmp:
            source=self.make_source(tmp);out=Path(tmp)/"out"
            meta=adapt(source,out);messages,_=read_envelopes(out)
            self.assertEqual(meta["counts"],{"spans":18,"metrics":3,"logs":1})
            self.assertFalse(meta["independent_coverage_verified"])
            self.assertFalse(meta["historical_live_forecast_valid"])
            self.assertEqual(meta["message_counts"]["cct.experiments"],1)
            fault=next(e for e in messages if e["signal"]=="experiments")
            self.assertEqual(fault["payload"]["controller_timestamp_precision_ms"],1000)
            self.assertIsNone(fault["payload"]["actual_physical_start_ms"])
            self.assertEqual(list(normalize(fault,1800000000000,TOPICS["experiments"])),[])
            trace_obs={e["source_observed_ns"] for e in messages if e["signal"]=="traces"}
            self.assertEqual(trace_obs,{1790000001001000000,1790000001002000000,1790000001003000000})

    def test_overlapping_prometheus_reads_are_deduplicated_without_rounding(self):
        with tempfile.TemporaryDirectory() as tmp:
            source=self.make_source(tmp);out=Path(tmp)/"out";adapt(source,out)
            messages,_=read_envelopes(out)
            points=[r for e in messages for t,r in normalize(e,1800000000000,TOPICS[e["signal"]])
                    if t=="metrics" and r["metric"]=="cct.pilot.observed_call_counter"]
            self.assertEqual(len(points),1)
            self.assertEqual(points[0]["event_ns"],1790000000125000000)
            self.assertEqual(points[0]["metric_type"],"gauge")
            self.assertIsNone(points[0]["start_ns"])

    def test_changed_raw_source_fails_before_output_creation(self):
        with tempfile.TemporaryDirectory() as tmp:
            source=self.make_source(tmp);(source/"receipt.json").write_text('{}')
            out=Path(tmp)/"out"
            with self.assertRaisesRegex(ValueError,"source hash mismatch"):
                adapt(source,out)
            self.assertFalse(out.exists())

    def test_failed_or_unrecovered_pilot_cannot_be_published_as_success(self):
        with tempfile.TemporaryDirectory() as tmp:
            source=self.make_source(tmp);r=json.loads((source/"receipt.json").read_text());r["passed"]=False
            (source/"receipt.json").write_text(json.dumps(r));self.hash_source(source)
            with self.assertRaisesRegex(ValueError,"passed, cleaned-up"):
                adapt(source,Path(tmp)/"out")

    def test_archive_tampering_is_rejected_before_publish(self):
        with tempfile.TemporaryDirectory() as tmp:
            source=self.make_source(tmp);out=Path(tmp)/"out";adapt(source,out)
            with (out/"envelopes.jsonl").open("a") as f:
                f.write((out/"envelopes.jsonl").read_text().splitlines()[0]+"\n")
            with self.assertRaisesRegex(ValueError,"identity/hash"):
                read_envelopes(out)

    def test_unconfirmed_recovery_is_not_upgraded_to_a_completed_fault(self):
        with tempfile.TemporaryDirectory() as tmp:
            source=self.make_source(tmp)
            fault=json.loads((source/"fault.recovered.json").read_text())
            fault["status"]["conditions"][0]["status"]="False"
            (source/"fault.recovered.json").write_text(json.dumps(fault));self.hash_source(source)
            out=Path(tmp)/"out"
            with self.assertRaisesRegex(ValueError,"recovered fault target"):
                adapt(source,out)
            self.assertFalse(out.exists())

    def test_conflicting_overlap_is_not_silently_deduplicated(self):
        with tempfile.TemporaryDirectory() as tmp:
            source=self.make_source(tmp)
            rows=[json.loads(s) for s in (source/"prometheus.jsonl").read_text().splitlines()]
            rows[1]["samples"]["result"][0]["values"][0][1]="14"
            (source/"prometheus.jsonl").write_text(''.join(json.dumps(r)+'\n' for r in rows));self.hash_source(source)
            with self.assertRaisesRegex(ValueError,"conflicting Prometheus"):
                adapt(source,Path(tmp)/"out")

    def test_cross_run_signal_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            source=self.make_source(tmp)
            log=json.loads((source/"signals/logs.json").read_text())
            log["resourceLogs"][0]["resource"]["attributes"].append({"key":"cct.run_id","value":{"stringValue":"another-run"}})
            (source/"signals/logs.json").write_text(json.dumps(log));self.hash_source(source)
            with self.assertRaisesRegex(ValueError,"run identity mismatch"):
                adapt(source,Path(tmp)/"out")


if __name__=="__main__":unittest.main()
