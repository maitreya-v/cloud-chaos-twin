"""Reconcile physically persisted pilot Parquet with its archived source.

No model fitting or coverage attestation. Reading Parquet requires pyarrow.
"""
import argparse
from collections import Counter
import hashlib
import importlib.util
import json
from pathlib import Path
import time

from ingestion.contract import TOPICS, canonical, digest
from ingestion.pilot import read_envelopes, source_hashes
from observability.signals import nanos
from processing.normalize import normalize


def table(path):
    import pyarrow.parquet as pq
    return pq.read_table(path).to_pylist()


def fingerprint(rows):
    return digest(sorted(rows,key=canonical))


def verify(source, archive, parquet, output, rebuild=None):
    source,archive,parquet,output=map(Path,(source,archive,parquet,output))
    messages,meta=read_envelopes(archive)
    if source_hashes(source)!=meta["source_sha256"]:
        raise ValueError("pilot source differs from replay archive")
    receipt=json.loads((parquet/"receipt.json").read_text())
    tables={name:table(parquet/name) for name in receipt["table_counts"]}
    if {k:len(v) for k,v in tables.items()}!=receipt["table_counts"]:
        raise ValueError("physical table counts differ from replay receipt")
    raw=tables["raw_telemetry"]+tables["raw_experiments"]
    if Counter(r["value"] for r in raw)!=Counter(canonical(e) for e in messages):
        raise ValueError("Kafka payloads differ from original envelopes")
    if any(r["timestampType"]!=1 or r["key"]!=meta["run_id"] for r in raw):
        raise ValueError("broker timestamp policy or run key differs")
    expected={name:{} for name in ("spans","metrics","logs")}
    for row in raw:
        for name,value in normalize(json.loads(row["value"]),row["broker_ms"],row["topic"]):
            old=expected[name].get(value["identity"])
            if old is not None and old["content_hash"]!=value["content_hash"]:
                raise ValueError("conflicting source identity")
            if old is None or old["available_ms"]>value["available_ms"]:
                expected[name][value["identity"]]=value
    for name,rows in expected.items():
        if fingerprint(list(rows.values()))!=fingerprint(tables[name]):
            raise ValueError("normalized identities/content/times differ: "+name)
    experiments=[json.loads(r["value"]) for r in tables["raw_experiments"]]
    if len(experiments)!=1 or experiments[0]["signal"]!="experiments":
        raise ValueError("expected one separately stored observed fault")
    for name in ("spans","metrics","logs","service_windows","edge_windows","metric_windows","log_windows"):
        if any("target_service" in r or "fault_type" in r for r in tables[name]):
            raise ValueError("fault metadata leaked into feature tables")
    windows=tables["service_windows"]
    if any(r["complete"] is not False or r["ready"] is not None or r["restart_delta"] is not None for r in windows):
        raise ValueError("raw replay asserted unsupported health/coverage")
    begin,end=(nanos(v) for v in meta["measurement_interval_utc"])
    selected=[r for r in windows if r["start_ms"]*1000000>=begin and r["end_ms"]*1000000<=end]
    window_source=source/"service-windows.partial.json"
    source_window_sha256=hashlib.sha256(window_source.read_bytes()).hexdigest()
    if json.loads((source/"file-hashes.json").read_text()).get("service-windows.partial.json")!=source_window_sha256:
        raise ValueError("pilot window reference hash mismatch")
    historical=json.loads(window_source.read_text())
    reference={(r["service"],r["start_ms"]):r for r in historical
               if r["start_ms"]*1000000>=begin and r["end_ms"]*1000000<=end}
    if {(r["service"],r["start_ms"]) for r in selected}!=set(reference):
        raise ValueError("measured window roster differs")
    maximum_p95_difference=0.0
    for row in selected:
        ref=reference[row["service"],row["start_ms"]]
        if (row["requests"],row["errors"])!=(ref["requests"],ref["errors"]):
            raise ValueError("measured requests/errors differ from pilot")
        maximum_p95_difference=max(maximum_p95_difference,abs(row["p95_ms"]-ref["p95_ms"]))
    if maximum_p95_difference>0.000001:
        raise ValueError("measured p95 differs from pilot")
    helper=Path(__file__).resolve().parents[1]/"CCT-10-Merul-Handoff-2026-11-26/join_windows.py"
    spec=importlib.util.spec_from_file_location("pilot_window_join",helper)
    join=importlib.util.module_from_spec(spec);spec.loader.exec_module(join)
    pods=[json.loads(s) for s in (archive/"pods.jsonl").read_text().splitlines()]
    joined=join.join(selected,pods,[],meta["run_id"])
    if any(r["complete"] for r in joined):
        raise ValueError("coverage became complete without independent evidence")
    # Source snapshots support readiness retrospectively. Kafka append times
    # remain the lower bound on pipeline availability; nothing is backdated.
    earliest_broker=min(r["broker_ms"] for r in raw)
    if any(r["available_ms"]<earliest_broker for r in joined):
        raise ValueError("replay availability was backdated")
    fingerprints={k:fingerprint(v) for k,v in tables.items()}
    rebuilt=False
    if rebuild is not None:
        other=json.loads((Path(rebuild)/"receipt.json").read_text())
        if set(other["table_counts"])!=set(tables):
            raise ValueError("rebuild table roster differs")
        for name,value in fingerprints.items():
            if fingerprint(table(Path(rebuild)/name))!=value:
                raise ValueError("rebuild content differs: "+name)
        rebuilt=True
    (output.parent/"measured-service-windows.partial.json").write_text(json.dumps(joined,indent=2,sort_keys=True)+"\n")
    result={"run_id":meta["run_id"],"passed":True,"verified_at_ms":time.time_ns()//1000000,
            "broker_messages":len(raw),"table_counts":receipt["table_counts"],"archive_sha256":meta["archive_sha256"],
            "source_window_sha256":source_window_sha256,
            "all_normalized_content_and_availability_match":True,"experiment_isolated":True,
            "controller_timestamp_precision_ms":experiments[0]["payload"]["controller_timestamp_precision_ms"],
            "lineage_status_counts":dict(Counter(r["lineage_status"] for r in tables["lineage"])),
            "fully_contained_measured_windows":len(selected),"measured_window_counts_match_source":True,
            "maximum_measured_p95_difference_ms":maximum_p95_difference,
            "fresh_ready_zero_restart_windows":sum(r["ready"] is True and r["restart_delta"]==0 for r in joined),
            "complete_windows":0,"calibration_eligible_windows":0,"replay_availability_not_backdated":True,
            "full_rebuild_matches":rebuilt,"table_logical_sha256":fingerprints,
            "historical_live_forecast_valid":False,
            "limits":["Raw replay spans include warm-up; measured window comparison excludes both partial boundary windows.",
                      "Sampled SERVER projection and selected metrics/logs are not independent backend accounting.",
                      "This is an offline replay; no calibration, impact-label acceptance or live warning claim."]}
    with output.open("x") as out:
        json.dump(result,out,indent=2,sort_keys=True);out.write("\n")
    return result


if __name__=="__main__":
    p=argparse.ArgumentParser(description=__doc__)
    for n in ("source","archive","parquet","output"):p.add_argument(n,type=Path)
    p.add_argument("--rebuild",type=Path)
    a=p.parse_args()
    print(json.dumps(verify(a.source,a.archive,a.parquet,a.output,a.rebuild),indent=2))
