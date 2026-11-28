#!/usr/bin/env python3
"""Summarize an archived pilot without upgrading its coverage or health labels."""
import argparse
from collections import Counter, defaultdict
import datetime as dt
import gzip
import json
import math
from pathlib import Path
import statistics
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from observability.lineage import inspect_trace, coverage


def stats(values):
    ordered = sorted(values)
    return {"n":len(ordered), "median_ms":statistics.median(ordered) if ordered else None,
            "p95_ms":ordered[math.ceil(.95*len(ordered))-1] if ordered else None}


def parent(span):
    refs = [r for r in span.get("references",[]) if r["refType"]=="CHILD_OF"]
    return refs[0]["spanID"] if len(refs)==1 else None


def tags(span):
    return {t["key"]:t["value"] for t in span.get("tags",[])}


def root_components(trace, external_parent):
    """Keep a redirect's independent frontend request trees distinct."""
    roots = [s for s in trace["spans"] if tags(s).get("span.kind")=="server"
             and trace["processes"][s["processID"]]["serviceName"]=="frontend"
             and parent(s) in (None,external_parent)]
    for root in roots:
        members = {root["spanID"]}
        while True:
            more = {s["spanID"] for s in trace["spans"] if parent(s) in members}
            if more <= members:
                break
            members |= more
        yield root, {**trace,"spans":[s for s in trace["spans"] if s["spanID"] in members]}


def epoch(value):
    return dt.datetime.fromisoformat(value.replace("Z","+00:00")).timestamp()


def summarize(directory):
    receipt = json.loads((directory/"receipt.json").read_text())
    run = receipt["run_id"]
    start, end = receipt["measurement_interval_utc"]
    fault = receipt["fault"]
    requests = [json.loads(s) for s in (directory/"requests.jsonl").read_text().splitlines()]
    intervals = {}
    if start and fault.get("request_started_ms") and fault.get("recovered_observed_ms"):
        intervals = {"healthy":(epoch(start),fault["request_started_ms"]/1000),
                     "injection_observed":(fault["injected_observed_ms"]/1000,fault["recovered_observed_ms"]/1000),
                     "recovery":(fault["recovered_observed_ms"]/1000,epoch(end))}
    def phase(a,b):
        return next((k for k,(low,high) in intervals.items() if low<=a<=b<high),None)
    client, rpc = defaultdict(list), defaultdict(list)
    for row in requests:
        p = phase(row["started_epoch"],row["completed_epoch"])
        if p:
            client[p].append(row["latency_ms"])
    examples, matches = {}, Counter()
    for line in gzip.open(directory/"traces.jsonl.gz","rt"):
        entry = json.loads(line)
        trace, event = entry["trace"], entry["client"]
        if not trace:
            continue
        candidates = list(root_components(trace,event["external_parent"]))
        for root, sub in candidates:
            rt = tags(root)
            journey = {("GET","/"):"browse",("POST","/cart"):"cart",
                       ("POST","/cart/checkout"):"checkout"}.get((rt.get("http.request.method"),rt.get("url.path")))
            if not journey or not start or not epoch(start)<=root["startTime"]/1e6<epoch(end):
                continue
            summary = inspect_trace(sub,entry["trace_id"],event["external_parent"],include_records=True)
            result = coverage(journey,summary)
            if result["passes_declared_path"]:
                matches[journey]+=1
                examples.setdefault(journey,{"trace_id":entry["trace_id"],"frontend_span_id":root["spanID"],
                                              "root_method":rt.get("http.request.method"),"root_path":rt.get("url.path"),
                                              "query_observed_ms":entry["observed_ms"],"coverage":result,"lineage":summary})
        spans = {s["spanID"]:s for s in trace["spans"]}
        for s in spans.values():
            if tags(s).get("span.kind")!="server" or trace["processes"][s["processID"]]["serviceName"]!="productcatalogservice":
                continue
            caller = spans.get(parent(s))
            if not caller or tags(caller).get("span.kind")!="client":
                continue
            p = phase(caller["startTime"]/1e6,(caller["startTime"]+caller["duration"])/1e6)
            if p:
                rpc[p].append(caller["duration"]/1000)
    (directory/"journey-examples.json").write_text(json.dumps({"run_id":run,"examples":examples},indent=2)+"\n")
    target_stats, unique_samples = Counter(), {}
    for line in (directory/"prometheus.jsonl").read_text().splitlines():
        row = json.loads(line)
        for t in row["targets"]:
            if t["labels"].get("job") in {"cct-service","cct-collector"}:
                target_stats[t["labels"]["job"]+":"+t["health"]]+=1
        for series in row["samples"]["result"]:
            labels = series["metric"]
            if labels.get("__name__")!="cct_calls_total" or labels.get("span_kind")!="SPAN_KIND_SERVER":
                continue
            identity = json.dumps(labels,sort_keys=True)
            for stamp,value in series["values"]:
                if start and epoch(start)<=stamp<epoch(end):
                    unique_samples[identity,stamp] = (labels["service_name"],float(value))
    totals = defaultdict(lambda:defaultdict(float))
    for (_,stamp),(service,value) in unique_samples.items():
        totals[service][stamp] += value
    counters = {}
    for service, samples in totals.items():
        first,last = min(samples),max(samples)
        counters[service] = {"first_scrape_epoch":first,"first_value":samples[first],
                             "last_scrape_epoch":last,"last_value":samples[last],
                             "observed_growth":samples[last]-samples[first],
                             "scope":"shared span-derived counters; not an independent or run-exclusive denominator"}
    windows_path = directory/"service-windows.partial.json"
    windows = json.loads(windows_path.read_text()) if windows_path.exists() else []
    by_service = defaultdict(list)
    for row in windows:
        by_service[row["service"]].append(row["requests"])
    recovered_path = directory/"fault.recovered.json"
    controller = json.loads(recovered_path.read_text()) if recovered_path.exists() else {}
    records = controller.get("status",{}).get("experiment",{}).get("containerRecords",[])
    timeline = [{"target":r["id"],"phase":r["phase"],"events":r.get("events",[])} for r in records]
    selected = receipt.get("selected_signals",{}).get("selected_by_service",{})
    summary = {"run_id":run,"execution_passed":receipt["passed"],"fault_timeline":timeline,
               "controller_timestamp_precision_ms":1000,"fault_observations":fault,
               "measurement_interval_utc":receipt["measurement_interval_utc"],
               "descriptive_intervals_epoch":intervals,
               "client_operations_by_interval":{k:stats(v) for k,v in client.items()},
               "productcatalog_caller_rpc_by_interval":{k:stats(v) for k,v in rpc.items()},
               "all_http_operations":len(requests),"all_http_failures":sum(not r["success"] for r in requests),
               "matching_journey_roots":dict(matches),"selected_log_counts":selected,
               "monitoring_target_polls":dict(target_stats),"capture":receipt.get("capture"),
               "spanmetric_counter_snapshots":counters,
               "window_request_counts":{k:{"windows":len(v),"min":min(v),"max":max(v),"windows_at_least_20":sum(n>=20 for n in v)} for k,v in sorted(by_service.items())},
               "health_unchanged":receipt.get("health_unchanged"),"remaining_fault":receipt.get("remaining_fault"),
               "remaining_workload":receipt.get("remaining_workload"),"cleanup_errors":receipt["cleanup_errors"],
               "cct10_complete":False,
               "limits":["Observed injection interval includes polling/controller timing uncertainty; it is not exact physical fault timing.",
                         "Client/RPC samples crossing interval boundaries are excluded from phase comparisons.",
                         "Closed-loop workload and low counts make latency comparisons descriptive.",
                         "All service windows remain incomplete without independent backend accounting; no thresholds or forecast scores were fitted.",
                         "One pilot is not three repeat trials or a paired telemetry-overhead benchmark."]}
    (directory/"analysis.json").write_text(json.dumps(summary,indent=2,sort_keys=True)+"\n")
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory",type=Path)
    args = parser.parse_args()
    print(json.dumps(summarize(args.directory),indent=2))
