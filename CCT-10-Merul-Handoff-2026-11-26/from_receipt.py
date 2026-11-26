"""Build explicitly partial service windows from a recorded Merul shopping probe. Python 3.9+."""
import argparse
from collections import defaultdict
from datetime import datetime, timezone, timedelta
import hashlib
import json
from math import ceil
from pathlib import Path

WIDTH = 5000
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def read(path):
    return json.loads(path.read_text(encoding="utf-8"))


def save(path, obj):
    path.write_text(json.dumps(obj, indent=2, allow_nan=False, sort_keys=True) + "\n", encoding="utf-8")


def ceil_ms(stamp):
    dt = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        raise ValueError("Expected timezone on a source timestamp")
    us = (dt - EPOCH) // timedelta(microseconds=1)
    return (us + 999) // 1000


def project(measured, out):
    j, summary, metrics = [read(measured / name) for name in
                           ("journeys.json", "summary.json", "signals/metrics.json")]
    if summary["phase"] != "measured" or j["run_id"] != summary["run_id"] or not j["passed"]:
        raise ValueError("Expected matching successful measured shopping receipts")
    groups, unique, journeys = defaultdict(list), {}, {}
    for req in j["requests"]:
        lineage = req.get("lineage")
        if not lineage:
            continue
        journeys[req["journey"]] = len(lineage["server_records"])
        seen = ceil_ms(req["observed_at"])
        for span in lineage["server_records"]:
            key = span["trace_id"], span["span_id"]
            if span["trace_id"] != req["trace_id"] or type(span["error"]) is not bool or span["duration_us"] < 0:
                raise ValueError("Invalid measured span")
            if key in unique:
                if unique[key] != span:
                    raise ValueError("Conflicting span identity")
                continue
            unique[key] = span
            end_us = span["start_us"] + span["duration_us"]
            start = end_us // (WIDTH * 1000) * WIDTH
            groups[span["service"], start].append((span["duration_us"], span["error"], seen))
    rows = []
    for (service, start), vals in sorted(groups.items(), key=lambda x: (x[0][1], x[0][0])):
        latencies = sorted(v[0] for v in vals)
        rows.append(dict(run_id=j["run_id"], service=service, start_ms=start, end_ms=start+WIDTH,
                         available_ms=max(start+WIDTH, max(v[2] for v in vals)), requests=len(vals),
                         errors=sum(v[1] for v in vals), p95_ms=latencies[ceil(0.95*len(vals))-1]/1000,
                         ready=None, restart_delta=None, complete=False))
    # Preserve actual post-run observations with their own timestamps, outside health windows.
    gauges = []
    for resource in metrics["resourceMetrics"]:
        attrs = {a["key"]: a["value"] for a in resource["resource"]["attributes"]}
        for scope in resource["scopeMetrics"]:
            for metric in scope["metrics"]:
                if metric["name"] not in ("cct.pod.ready", "cct.pod.restarts"):
                    continue
                for point in metric["gauge"]["dataPoints"]:
                    ids = {a["key"]: a["value"] for a in point["attributes"]}
                    if ids["cct.run_id"]["stringValue"] != j["run_id"]:
                        raise ValueError("Mismatched gauge run")
                    gauges.append(dict(service=attrs["service.name"]["stringValue"], metric=metric["name"],
                                       observed_unix_ns=point["timeUnixNano"], value=int(point["asInt"])))
    gauges.sort(key=lambda g: (g["service"], g["metric"], int(g["observed_unix_ns"])))
    out.mkdir(parents=True, exist_ok=True)
    save(out / "measured_windows.json", rows)
    save(out / "post_run_gauges.json", gauges)
    names = ["journeys.json", "summary.json", "signals/metrics.json", "signals/summary.json"]
    save(out / "source_audit.json", dict(run_id=j["run_id"], source_revision=summary["source_revision"],
         raw_source_sha256={name:hashlib.sha256((measured/name).read_bytes()).hexdigest() for name in names},
         run_interval_utc=[j["started_at"],j["finished_at"]], spans_by_journey=journeys,
         unique_server_spans=len(unique), observed_service_windows=len(rows), complete_windows=0,
         calibration_eligible_windows=0, historical_live_forecast_valid=False,
         observed_http_requests_in_receipt=len(j["requests"]), selected_logs=summary["unique_selected_events"],
         fault_run_supplied=False, verified_fault_target=None, actual_fault_start_ms=None,
         actual_fault_end_ms=None, recovery_result=None))
    return rows


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("measured", type=Path)
    p.add_argument("output", type=Path)
    args = p.parse_args()
    project(args.measured, args.output)
