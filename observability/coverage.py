"""Conservative reconciliation of independent proxy records with SERVER spans.

The proxy has no tracer. Its start/end records and HCM counters establish the
request universe; W3C parent IDs link individual completions to native spans.
No incomplete review emits complete=true. Proxy/server completion boundaries
differ. The server start must fit the independent request (two milliseconds of
clock tolerance); post-response SDK bookkeeping is recorded as completion skew.
"""
from collections import Counter,defaultdict
from decimal import Decimal
import hashlib
import json
import math
import re
import time

from observability.lineage import inspect_trace
from observability.reference import PORTS


def traceparent(value):
    if not isinstance(value,str):raise ValueError("missing independent trace context")
    m=re.fullmatch(r"00-([0-9a-f]{32})-([0-9a-f]{16})-[0-9a-f]{2}",value)
    if not m or not int(m[1],16) or not int(m[2],16):raise ValueError("invalid independent trace context")
    return m[1],m[2]


def finite(value):
    if type(value) not in (int,float) or not math.isfinite(value):raise ValueError("nonfinite reference value")
    return value


def identity(service,record):
    trace,parent=traceparent(record.get("traceparent"))
    method=record.get("method");path=record.get("path")
    if not method or not isinstance(path,str) or not path.startswith("/"):
        raise ValueError("reference method/path missing")
    start=int(Decimal(record["start_epoch_s"])*1000000)
    return (service,trace,parent,method,path),start


def closed_counters(before,after):
    if set(before)!=set(PORTS) or set(after)!=set(PORTS):raise ValueError("reference roster incomplete")
    for service in PORTS:
        a,b=before[service],after[service]
        if a["pod_uid_sha256"]!=b["pod_uid_sha256"]:raise ValueError("reference pod replaced")
        for row in (a,b):
            c=row["counters"];events=row["received_events"]
            total=c["http.cct_reference.downstream_rq_total"]
            complete=c["http.cct_reference.downstream_rq_completed"]
            if total!=complete or c["http.cct_reference.downstream_rq_active"]!=0:
                raise ValueError("reference requests still in flight")
            if events!={"DownstreamStart":total,"DownstreamEnd":complete} or row["received_log_records"]!=total+complete:
                raise ValueError("reference logs differ from independent request counters")
            if any(c.get(k)!=0 for k in ("filesystem.write_failed","filesystem.reopen_failed","filesystem.write_total_buffered")):
                raise ValueError("reference output failure or undrained log buffer")
        if b["counters"]["http.cct_reference.downstream_rq_total"]<a["counters"]["http.cct_reference.downstream_rq_total"]:
            raise ValueError("reference counters reset")


def native_records(trace_id,entry):
    trace=entry["trace"]
    summary=inspect_trace(trace,trace_id,entry["client"]["external_parent"],include_records=True)
    if summary["missing_parents"] or summary["unlinked_spans"]:raise ValueError("native trace ancestry incomplete")
    native={r["span_id"]:r for r in summary["server_records"]};result=[]
    for span in trace["spans"]:
        if span["spanID"] not in native:continue
        row=native[span["spanID"]];service=row["service"]
        tags={t["key"]:t["value"] for t in span.get("tags",[])}
        refs=[r for r in span.get("references",[]) if r["refType"]=="CHILD_OF"]
        if len(refs)!=1:raise ValueError("native server has no unique incoming parent")
        if service=="frontend":
            method=tags.get("http.request.method",tags.get("http.method"))
            path=tags.get("url.path",tags.get("http.target"))
        else:
            method="POST";path=tags.get("url.path",tags.get("http.target"))
            if path is None:
                path=("/"+str(tags["rpc.service"])+"/"+str(tags["rpc.method"])) if tags.get("rpc.service") and tags.get("rpc.method") else "/"+span["operationName"].removeprefix("POST ").lstrip("/")
        result.append(((service,trace_id,refs[0]["spanID"],method,path),row))
    return result


def reconcile(entries,requests,before,after,ledger,tolerance_us=2000,trace_cutoff_ms=None,caches=None):
    # Live callers provide immutable JSON snapshots. A final uncached review
    # independently repeats every check on the physically saved archive.
    closed_counters(before,after)
    known={r["trace_id"] for r in requests}
    if len(known)!=len(requests):raise ValueError("duplicate workload operation trace ID")
    selected=known if trace_cutoff_ms is None else {r["trace_id"] for r in requests if r["started_epoch"]*1000<=trace_cutoff_ms+tolerance_us/1000}
    if not selected<=set(entries) or (trace_cutoff_ms is None and set(entries)!=known) or any(not entries[k].get("trace") for k in selected):
        raise ValueError("workload traces were not fully archived")
    rows=defaultdict(list);start_records={};end_records={};log_counts=Counter()
    for observation in ledger:
        service=observation["service"]
        if service not in PORTS or observation["pod_uid_sha256"]!=before[service]["pod_uid_sha256"]:
            raise ValueError("reference log pod/roster mismatch")
        lo,hi=before[service]["received_log_records"],after[service]["received_log_records"]
        if not lo<observation["sequence"]<=hi:raise ValueError("reference slice crosses counter bounds")
        record=observation["record"]
        cache_key=(service,observation["sequence"])
        cached=caches["reference"].get(cache_key) if caches is not None else None
        if cached is not None and cached[0] is observation:key,start=cached[1]
        else:
            key,start=identity(service,record)
            if caches is not None:caches["reference"][cache_key]=(observation,(key,start))
        if key[1] not in known:raise ValueError("background or broken-propagation trace in controlled capture")
        fullkey=key,start
        event=record.get("event")
        dst=start_records if event=="DownstreamStart" else end_records if event=="DownstreamEnd" else None
        if dst is None or fullkey in dst:raise ValueError("unexpected or duplicate reference event")
        dst[fullkey]=observation;log_counts[service]+=1
    for service in PORTS:
        if log_counts[service]!=after[service]["received_log_records"]-before[service]["received_log_records"]:
            raise ValueError("reference slice lost records")
    if start_records.keys()!=end_records.keys():raise ValueError("reference request lacks start or completion")
    for (key,start),end in end_records.items():
        if key[1] in selected:rows[key].append((start,end))
    spans=defaultdict(list)
    for trace_id,entry in entries.items():
        if trace_id not in selected:continue
        cached=caches["trace"].get(trace_id) if caches is not None else None
        if cached is not None and cached[0] is entry["trace"]:projection=cached[1]
        else:
            projection=native_records(trace_id,entry)
            if caches is not None:caches["trace"][trace_id]=(entry["trace"],projection)
        for key,row in projection:spans[key].append(row)
    if rows.keys()!=spans.keys():raise ValueError("independent/native completion identities differ")
    pairs=[]
    for key,values in rows.items():
        targets=sorted(spans[key],key=lambda r:r["start_us"])
        values=sorted(values,key=lambda x:x[0])
        if len(values)!=len(targets):raise ValueError("independent/native request multiplicity differs")
        for (start,observation),native in zip(values,targets):
            ref=observation["record"]
            duration=finite(ref["duration_us"]);upstream=finite(ref["upstream_end_us"])
            if min(duration,upstream)<0:raise ValueError("reference duration invalid")
            native_end=native["start_us"]+native["duration_us"]
            if native["start_us"]<start-tolerance_us or native["start_us"]>start+upstream+tolerance_us:
                raise ValueError("native start not enclosed by independent request timing")
            status=finite(ref["http_status"]);grpc=ref.get("grpc_status")
            error=(status>=500 or status<100 or (grpc is not None and finite(grpc)!=0) or ref.get("flags") not in ("-",None,""))
            if error!=native["error"]:raise ValueError("independent/native error result differs")
            pairs.append({"service":key[0],"trace_id":key[1],"incoming_parent_id":key[2],"method":key[3],"path":key[4],
                "span_id":native["span_id"],"native_start_us":native["start_us"],"native_end_us":native_end,
                "native_duration_us":native["duration_us"],"reference_start_us":start,"reference_end_us":start+upstream,
                "native_completion_skew_us":native_end-(start+upstream),
                "reference_duration_us":duration,"reference_seen_ms":observation["seen_ms"],"error":error,
                "reference_sequence":observation["sequence"]})
    return pairs


def proofs_for_windows(run,windows,pairs,evidence_ref,reviewer="owner-assisted automated reconciliation",event_field="native_end_us"):
    if event_field not in ("native_end_us","reference_end_us"):raise ValueError("unknown completion clock")
    groups=defaultdict(list)
    for p in pairs:
        start=int(p[event_field]//5000000)*5000
        groups[p["service"],start].append(p)
    available=(time.time_ns()+999999)//1000000
    proofs=[]
    for row in windows:
        values=groups[row["service"],row["start_ms"]]
        if row["run_id"]!=run or len(values)!=row["requests"] or sum(v["error"] for v in values)!=row["errors"]:
            raise ValueError("window request/error denominator differs from paired completions")
        proofs.append({"run_id":run,"service":row["service"],"start_ms":row["start_ms"],"end_ms":row["end_ms"],
            "requests":len(values),"errors":sum(v["error"] for v in values),"duration_count":len(values),
            "verified":True,"records_reconciled":True,"available_ms":max(available,row["end_ms"]),
            "independent_source":"Envoy request start/end ledger and independent HCM counters; no Envoy tracer",
            "evidence_ref":evidence_ref,"reviewer":reviewer})
    return proofs
