"""Archive/replay a bounded pilot without inventing coverage or fault precision.

Traces are the recorded nearest-SERVER projection, including warm-up. Source
observation times are preserved per envelope; Kafka append time is added later.
Fault metadata is sent only to cct.experiments, never to feature extraction.
"""
import argparse
from collections import Counter, defaultdict
from decimal import Decimal
import gzip
import hashlib
import json
from pathlib import Path

from ingestion.contract import TOPICS, canonical, digest, envelope, validate
from observability.lineage import inspect_trace
from observability.signals import attributes, nanos
from processing.normalize import normalize


SOURCES = ("receipt.json", "traces.jsonl.gz", "prometheus.jsonl", "pods.jsonl",
           "fault.request.json", "fault.recovered.json", "signals/metrics.json", "signals/logs.json")


def read(path):
    return json.loads(path.read_text())


def source_hashes(source):
    declared = read(source/"file-hashes.json")
    result = {}
    for name in SOURCES:
        actual = hashlib.sha256((source/name).read_bytes()).hexdigest()
        if declared.get(name) != actual:
            raise ValueError("source hash mismatch: " + name)
        result[name] = actual
    return result


def resource(run, service):
    return {"attributes":attributes(**{"service.name":service,"cct.run_id":run})}


def experiment(source, receipt):
    requested, recovered = read(source/"fault.request.json"), read(source/"fault.recovered.json")
    run = receipt["run_id"]
    for obj in (requested,recovered):
        if obj["metadata"].get("annotations",{}).get("cct.run_id") != run:
            raise ValueError("fault run identity mismatch")
    if requested["spec"] != recovered["spec"]:
        raise ValueError("fault specification changed")
    conditions = {c["type"]:c["status"] for c in recovered["status"]["conditions"]}
    records = recovered["status"]["experiment"]["containerRecords"]
    target = requested["spec"]["selector"]["labelSelectors"]["app"]
    if conditions.get("AllRecovered")!="True" or len(records)!=1 or records[0]["phase"]!="Not Injected":
        raise ValueError("expected one recovered fault target")
    if not records[0]["id"].startswith("cct-app/"+target+"-"):
        raise ValueError("recovered target differs from selector")
    events = records[0]["events"]
    applied = [e for e in events if e["operation"]=="Apply" and e["type"]=="Succeeded"]
    restored = [e for e in events if e["operation"]=="Recover" and e["type"]=="Succeeded"]
    if len(applied)!=1 or len(restored)!=1 or nanos(restored[0]["timestamp"])<nanos(applied[0]["timestamp"]):
        raise ValueError("expected ordered successful Apply/Recover records")
    return {"schema_version":"1.0","run_id":run,"status":"observed-recovered",
            "target_service":target,"selected_target":records[0]["id"],"fault_type":"network_delay",
            "spec":requested["spec"],"controller_reported_start_utc":applied[0]["timestamp"],
            "controller_reported_end_utc":restored[0]["timestamp"],"controller_timestamp_precision_ms":1000,
            "actual_physical_start_ms":None,"actual_physical_end_ms":None,
            "owner_observations":receipt["fault"],"recovery_passed":True,
            "actor":receipt["actor"],"application_impact_labels_verified":False}


def records(source, receipt):
    run = receipt["run_id"]
    seen, span_count = set(), 0
    for line in gzip.open(source/"traces.jsonl.gz","rt"):
        entry = json.loads(line)
        trace_id = entry["trace_id"]
        if trace_id in seen or entry["client"]["run_id"]!=run or not entry["trace"]:
            raise ValueError("duplicate, missing or mismatched workload trace")
        seen.add(trace_id)
        stats = inspect_trace(entry["trace"],trace_id,entry["client"]["external_parent"],include_records=True)
        if stats["missing_parents"] or stats["unlinked_spans"]:
            raise ValueError("incomplete trace ancestry")
        groups = defaultdict(list)
        for row in stats["server_records"]:
            span = {"traceId":trace_id,"spanId":row["span_id"],"name":row["operation"],"kind":2,
                    "startTimeUnixNano":str(row["start_us"]*1000),
                    "endTimeUnixNano":str((row["start_us"]+row["duration_us"])*1000),
                    "status":{"code":2 if row["error"] else 0}}
            if row["parent_server_id"]:
                span["parentSpanId"] = row["parent_server_id"]
            groups[row["service"]].append(span)
            span_count += 1
        for service,spans in sorted(groups.items()):
            yield envelope(run,"traces",{"resourceSpans":[{"resource":resource(run,service),"scopeSpans":[{"spans":spans}]}]},False,entry["observed_ms"]*1000000)
    if len(seen)!=receipt["capture"]["saved_traces"] or span_count!=receipt["capture"]["server_records"]:
        raise ValueError("trace counts differ from capture receipt")
    # Exact deduplication of overlapping range-vector reads. These are observed
    # cumulative-value gauges: no counter epoch, deltas or histogram is invented.
    seen_points = {}
    for line in (source/"prometheus.jsonl").read_text().splitlines():
        observation = json.loads(line)
        if observation["samples"]["resultType"]!="matrix":
            raise ValueError("expected raw scrape-time range vectors")
        for series in observation["samples"]["result"]:
            labels = series["metric"]
            name = labels["__name__"]
            if name=="cct_calls_total" and labels.get("span_kind")=="SPAN_KIND_SERVER":
                service, metric = labels["service_name"], "cct.pilot.observed_call_counter"
            elif name=="up" and labels.get("job") in ("cct-service","cct-collector"):
                service, metric = "cct-collector", "cct.pilot.observed_target_up"
            else:
                continue
            for stamp,value in series["values"]:
                timestamp = int(Decimal(str(stamp))*1000000000)
                key = canonical(labels), timestamp
                if key in seen_points:
                    if seen_points[key]!=value:
                        raise ValueError("conflicting Prometheus scrape value")
                    continue
                seen_points[key]=value
                point = {"timeUnixNano":str(timestamp),"asDouble":float(value),
                         "attributes":attributes(**{"prometheus.labels":canonical(labels)})}
                group = {"resource":resource(run,service),"scopeMetrics":[{"metrics":[{
                    "name":metric,"unit":"1","gauge":{"dataPoints":[point]}}]}]}
                yield envelope(run,"metrics",{"resourceMetrics":[group]},False,observation["observed_ms"]*1000000)
    for name,key,scope,items in (("metrics","resourceMetrics","scopeMetrics","metrics"),
                                  ("logs","resourceLogs","scopeLogs","logRecords")):
        for group in read(source/"signals"/(name+".json"))[key]:
            stamps = []
            for block in group[scope]:
                for item in block[items]:
                    if name=="logs":
                        stamps.append(int(item["observedTimeUnixNano"]))
                    else:
                        stamps.extend(int(p["timeUnixNano"]) for p in item["gauge"]["dataPoints"])
            if stamps:
                yield envelope(run,name,{key:[group]},False,max(stamps))
    yield envelope(run,"experiments",experiment(source,receipt),False,nanos(receipt["finished_at"].replace("+00:00","Z")))


def adapt(source, output):
    source, output = Path(source), Path(output)
    hashes = source_hashes(source)
    receipt = read(source/"receipt.json")
    if receipt["passed"] is not True or receipt["remaining_fault"] or receipt["remaining_workload"] or receipt["cleanup_errors"]:
        raise ValueError("expected a passed, cleaned-up pilot")
    messages = list(records(source,receipt))
    counts, topics = Counter(), Counter()
    for e in messages:
        if len(canonical(e).encode())>900000:
            raise ValueError("pilot envelope exceeds publisher size limit")
        # Schema/count validation only; this is not a broker-delivery receipt.
        for table,_ in normalize(e,(e["source_observed_ns"]+999999)//1000000,TOPICS[e["signal"]]):
            counts[table]+=1
        topics[TOPICS[e["signal"]]]+=1
    output.mkdir(parents=True,exist_ok=False)
    with (output/"envelopes.jsonl").open("x") as out:
        for e in messages:
            out.write(canonical(e)+"\n")
    (output/"pods.jsonl").write_bytes((source/"pods.jsonl").read_bytes())
    meta = {"run_id":receipt["run_id"],"synthetic":False,"source_sha256":hashes,
            "trace_representation":"nearest-SERVER projection; external probe parents removed, original IDs retained",
            "scope":"all captured phases, including spawning/warm-up; use measured bounds when selecting features",
            "measurement_interval_utc":receipt["measurement_interval_utc"],"source_revision":receipt["source_revision"],
            "counts":dict(counts),"message_counts":dict(topics),"archive_sha256":digest(messages),
            "historical_live_forecast_valid":False,"independent_coverage_verified":False,
            "metric_selection":["SERVER cct_calls_total and target up as observed gauges","post-run pod gauges"],
            "metric_limits":"Original scrape times retained; no invented start epoch, interpolation, histogram or request denominator."}
    (output/"archive.json").write_text(json.dumps(meta,indent=2,sort_keys=True)+"\n")
    return meta


def read_envelopes(directory):
    directory = Path(directory)
    meta = read(directory/"archive.json")
    messages = [validate(json.loads(s)) for s in (directory/"envelopes.jsonl").read_text().splitlines()]
    if digest(messages)!=meta["archive_sha256"] or any(e["run_id"]!=meta["run_id"] or e["synthetic"] is not False for e in messages):
        raise ValueError("archive identity/hash mismatch")
    return messages,meta


if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    commands=parser.add_subparsers(dest="command",required=True)
    a=commands.add_parser("adapt"); a.add_argument("source",type=Path);a.add_argument("output",type=Path)
    p=commands.add_parser("publish");p.add_argument("archive",type=Path)
    p.add_argument("--bootstrap",required=True);p.add_argument("--receipt",type=Path,required=True)
    args=parser.parse_args()
    if args.command=="adapt":
        result=adapt(args.source,args.output)
    else:
        from ingestion.kafka_io import publish,write_new
        if args.receipt.exists():parser.error("select a new publish receipt")
        messages,meta=read_envelopes(args.archive)
        result=publish(args.bootstrap,messages)
        result.update(run_id=meta["run_id"],synthetic=False,archive_sha256=meta["archive_sha256"])
        write_new(args.receipt,result)
    print(json.dumps(result,indent=2))
