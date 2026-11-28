#!/usr/bin/env python3
"""Owner-run healthy calibration and three bounded held-out fault trials.

Run only after the paired tracing benchmark has completed and restored settings.
No empirical completion is asserted until independently reconciled inputs pass.
"""
from collections import defaultdict
import datetime as dt
import gzip
import importlib.util
import json
from pathlib import Path
import statistics
import subprocess
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from analytics.heldout_validation import reference_windows,freeze,evaluate_labels
from analytics.evaluate import evaluate,aggregate
from observability.live_windows import LiveWindows
from observability.paired import save,website_ready,quiescent
from observability.reference_control import ReferenceSession
from observability.signals import nanos


def load(path):return json.loads(Path(path).read_text())


def phase_latency(directory):
    receipt=load(directory/"receipt.json");fault=receipt["fault"]
    start,end=[nanos(v)/1e9 for v in receipt["measurement_interval_utc"]]
    intervals={"healthy":(start,fault["request_started_ms"]/1000),
               "injection_observed":(fault["injected_observed_ms"]/1000,fault["recovered_observed_ms"]/1000),
               "recovery":(fault["recovered_observed_ms"]/1000,end)}
    values=defaultdict(list)
    for line in gzip.open(directory/"traces.jsonl.gz","rt"):
        trace=json.loads(line)["trace"]
        if not trace:continue
        spans={s["spanID"]:s for s in trace["spans"]}
        for span in spans.values():
            if trace["processes"][span["processID"]]["serviceName"]!="productcatalogservice":continue
            tags={t["key"]:t["value"] for t in span.get("tags",[])}
            if tags.get("span.kind")!="server":continue
            refs=[r for r in span.get("references",[]) if r["refType"]=="CHILD_OF"]
            parent=spans.get(refs[0]["spanID"]) if len(refs)==1 else None
            if not parent or not any(t["key"]=="span.kind" and t["value"]=="client" for t in parent.get("tags",[])):continue
            a=parent["startTime"]/1e6;b=(parent["startTime"]+parent["duration"])/1e6
            for phase,(lo,hi) in intervals.items():
                if lo<=a<=b<hi:values[phase].append(parent["duration"]/1000)
    result={k:{"n":len(v),"median_ms":statistics.median(v)} for k,v in values.items() if v}
    if set(result)!={"healthy","injection_observed","recovery"}:raise ValueError("Missing phase latency observations")
    base,during,recovered=(result[k] for k in ("healthy","injection_observed","recovery"))
    result["effect_and_recovery_passed"]=(min(v["n"] for v in (base,during,recovered))>=20 and
        during["median_ms"]>=base["median_ms"]+50 and recovered["median_ms"]<=max(base["median_ms"]*3,base["median_ms"]+10))
    result["scope"]="caller RPC to productcatalog; observed phase bounds, boundary-crossing calls excluded"
    save(directory/"phase-latency.json",result)
    return result


def read_windows(directory,run):
    native=load(directory/"service-windows.reviewed.json")
    pairs=[json.loads(s) for s in (directory/"reference-pairs.jsonl").read_text().splitlines()]
    pods=[json.loads(s) for s in (directory/"pods.jsonl").read_text().splitlines()]
    a,b=[nanos(v) for v in load(directory/"receipt.json")["measurement_interval_utc"]]
    independent=reference_windows(run,pairs,(a+999999)//1000000,b//1000000,pods)
    save(directory/"reference-service-windows.reviewed.json",independent)
    return native,independent


def main():
    import argparse
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--session-id",required=True)
    p.add_argument("--after-pair",type=Path,required=True)
    p.add_argument("--campaign",default="20261127")
    a=p.parse_args()
    a.after_pair=a.after_pair.resolve()
    paired=load(a.after_pair/"receipt.json")
    if not paired["passed"] or paired["restoration_errors"]:raise ValueError("Paired measurement has not passed/restored")
    if not a.session_id.startswith("cct-validation-"):raise ValueError("Use a unique validation session ID")
    directory=ROOT/"outputs/validation"/a.session_id;directory.mkdir(parents=True,exist_ok=False)
    plan={"train":[f"cct-probe-cct10-train-{a.campaign}-01"],
          "validation":[f"cct-probe-cct10-validation-{a.campaign}-01"],
          "test":[f"cct-probe-cct10-test-{a.campaign}-01",f"cct-probe-cct10-test-{a.campaign}-02"]}
    save(directory/"split.json",plan)
    receipt={"passed":False,"actor":"owner-assisted validation","paired_evidence":str(a.after_pair.relative_to(ROOT)),
             "split":plan,"runs":{},"restoration_errors":[],"teammate_signoff_recorded":False}
    lock=load(ROOT/"configs/reference.local.lock.json")
    reference=ReferenceSession(directory,a.session_id,lock["image"])
    spec=importlib.util.spec_from_file_location("cct_capture",ROOT/"scripts/cct10_pilot.py")
    capture=importlib.util.module_from_spec(spec);spec.loader.exec_module(capture)
    try:
        reference.install();reference.connect();website_ready()
        def collect(run,mode,calibration=None):
            out=directory/"runs"/run
            observer=LiveWindows(calibration)
            result=capture.run_protocol(run,out,profile="balanced",mode=mode,reference=reference,observer=observer)
            receipt["runs"][run]={"passed":result["passed"],"coverage_review":result.get("coverage_review"),"error":result.get("error")}
            save(directory/"progress.json",receipt)
            if not result["passed"] or not result.get("coverage_review",{}).get("verified"):
                raise ValueError("Capture or independent coverage failed: "+run+"; see its retained receipt")
            if observer.errors:raise ValueError("Prospective observer error: "+str(observer.errors))
            return out,result,observer
        train=plan["train"][0]
        out,result,observer=collect(train,"healthy")
        native,independent=read_windows(out,train)
        bounds={train:[nanos(v)//1000000 for v in result["measurement_interval_utc"]]}
        save(directory/"healthy-intervals.json",bounds)
        frozen=freeze(native,independent,plan,bounds)
        save(directory/"calibration.frozen.json",frozen)
        if len(observer.frames)<3:raise ValueError("Healthy live coverage produced fewer than three timely verified frames")
        print("Training-only thresholds frozen for all ten services in both channels",flush=True)
        for run in plan["validation"]+plan["test"]:
            out,result,observer=collect(run,"fault",frozen["native"])
            native,independent=read_windows(out,run)
            comparison=evaluate_labels(native,independent,run,frozen)
            save(out/"heldout-label-validation.json",comparison)
            evaluations=[evaluate(p,independent,frozen["native"],"independent Envoy upstream-completion ledger and HCM counters",
                                  "productcatalogservice",frozen["independent_reference"]) for p in observer.predictions]
            save(out/"prospective-evaluations.json",evaluations)
            if not evaluations:raise ValueError("No timely prospective decisions were recorded for "+run)
            save(out/"prospective-summary.json",aggregate(evaluations))
            latency=phase_latency(out)
            receipt["runs"][run]["effect_and_recovery_passed"]=latency["effect_and_recovery_passed"]
            if not latency["effect_and_recovery_passed"]:raise ValueError("Fault effect/recovery latency did not pass its declared gate")
        receipt["passed"]=True
    except BaseException as e:
        receipt["error"]=str(e);print("Validation stopped: "+str(e),flush=True)
    finally:
        receipt["restoration_errors"]=reference.restore()
        try:website_ready();quiescent();receipt["frontend_recovered"]=True
        except Exception as e:receipt["restoration_errors"].append(str(e))
        receipt["passed"] &= not receipt["restoration_errors"]
        receipt["finished_at"]=dt.datetime.now(dt.timezone.utc).isoformat()
        save(directory/"receipt.json",receipt)
        print(json.dumps({"passed":receipt["passed"],"error":receipt.get("error"),"restoration_errors":receipt["restoration_errors"],"evidence":str(directory)}),flush=True)
    return 0 if receipt["passed"] else 1


if __name__=="__main__":sys.exit(main())
