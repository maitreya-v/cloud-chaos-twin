"""Training-only calibration and independent, retrospective health-rule checks.

Event-time label agreement below is not a forecast or a live-warning claim.
Actual source/review availability remains unchanged in the archived inputs.
"""
from collections import Counter,defaultdict
import importlib.util
import math
from pathlib import Path
import time

from analytics.health import fit,classify
from ingestion.contract import digest
from observability.coverage import proofs_for_windows
from observability.reference import PORTS


def reference_windows(run,pairs,start_ms,end_ms,pods):
    groups=defaultdict(list)
    for p in pairs:
        start=int(p["reference_end_us"]//5000000)*5000
        if start>=start_ms and start+5000<=end_ms:groups[p["service"],start].append(p)
    rows=[]
    for (service,start),values in sorted(groups.items(),key=lambda x:(x[0][1],x[0][0])):
        durations=sorted((p["reference_end_us"]-p["reference_start_us"])/1000 for p in values)
        rows.append({"run_id":run,"service":service,"start_ms":start,"end_ms":start+5000,
            "requests":len(values),"errors":sum(p["error"] for p in values),"p95_ms":durations[math.ceil(.95*len(values))-1],
            "available_ms":max(start+5000,max(p["reference_seen_ms"] for p in values)),
            "ready":None,"restart_delta":None,"complete":False})
    proofs=proofs_for_windows(run,rows,pairs,"reference-pairs.jsonl",event_field="reference_end_us")
    path=Path(__file__).resolve().parents[1]/"CCT-10-Merul-Handoff-2026-11-26/join_windows.py"
    spec=importlib.util.spec_from_file_location("independent_reference_join",path)
    helper=importlib.util.module_from_spec(spec);spec.loader.exec_module(helper)
    return helper.join(rows,pods,proofs,run)


def freeze(native,reference,split,healthy_intervals):
    a=fit(native,split,healthy_intervals);b=fit(reference,split,healthy_intervals)
    if set(a["latency_limits_ms"])!=set(PORTS) or set(b["latency_limits_ms"])!=set(PORTS):
        raise ValueError("Not every service has three complete, >=20-request healthy training windows in both channels")
    return {"native":a,"independent_reference":b,"frozen_at_ms":time.time_ns()//1000000,
            "native_calibration_sha256":digest(a),"reference_calibration_sha256":digest(b),
            "reviewer":"owner-assisted rule review","teammate_signoff_recorded":False,
            "review":{"latency_rule":"twice median healthy training-window p95","error_fraction":.05,
                      "persistence_windows":2,"window_ms":5000,"minimum_requests":20,
                      "missing_or_low_volume":"unknown","hard_readiness_or_restart":"affected",
                      "limits":["Reference proxy and native SERVER durations measure different boundaries; each channel is fitted separately.",
                                "Thresholds are frozen before validation/test runs; no fault metadata is a feature.",
                                "This review does not establish long-term false-alarm rates or live warning lead time."]}}


def evaluate_labels(native,reference,run,calibrations):
    split=calibrations["native"]["split"]
    if run not in split["validation"]+split["test"]:raise ValueError("Held-out run absent from frozen split")
    original_native=[r for r in native if r["run_id"]==run]
    original_reference=[r for r in reference if r["run_id"]==run]
    # Finalized outcome labels are inspected in event time. This view is never
    # serialized as earlier-available features or passed to a forecast scorer.
    a=[dict(r,available_ms=r["end_ms"]) for r in original_native]
    b=[dict(r,available_ms=r["end_ms"]) for r in original_reference]
    ends=sorted({r["end_ms"] for r in a}|{r["end_ms"] for r in b})
    counts=Counter();timeline=[]
    for at in ends:
        model=classify(a,run,PORTS,at,calibrations["native"])
        outcome=classify(b,run,PORTS,at,calibrations["independent_reference"])
        for service in PORTS:
            pred,actual=model[service],outcome[service]
            if "unknown" in (pred,actual):counts["unknown_or_censored"]+=1
            elif pred==actual:counts["true_positive" if pred=="affected" else "true_negative"]+=1
            else:counts["false_positive" if pred=="affected" else "false_negative"]+=1
        timeline.append({"end_ms":at,"native_rule_labels":model,"independent_reference_labels":outcome})
    tp,fp,fn=(counts[k] for k in ("true_positive","false_positive","false_negative"))
    return {"run_id":run,"mode":"retrospective health-label agreement; not forecasting",
            "partition":"validation" if run in split["validation"] else "test",
            "counts":dict(counts),"label_precision":tp/(tp+fp) if tp+fp else None,
            "label_recall":tp/(tp+fn) if tp+fn else None,
            "label_f1":2*tp/(2*tp+fp+fn) if 2*tp+fp+fn else None,
            "timeline":timeline,"native_input_sha256":digest(original_native),"reference_input_sha256":digest(original_reference),
            "native_calibration_sha256":calibrations["native_calibration_sha256"],
            "historical_live_forecast_valid":False,"warning_lead_time_claimed":False}
