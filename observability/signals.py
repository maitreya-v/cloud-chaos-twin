"""Capture selected Boutique logs and pod-state gauges with Merul's Kubernetes identity."""
import argparse
import calendar
import datetime as dt
import hashlib
import json
import re
import subprocess
import time
from collections import Counter
from pathlib import Path
from urllib import request

LOG_SERVICES = {"frontend", "checkoutservice"}
APP_COMPONENTS = {"frontend", "checkoutservice", "cartservice", "currencyservice", "paymentservice",
                  "shippingservice", "emailservice", "adservice", "recommendationservice",
                  "productcatalogservice", "redis-cart"}
EVENTS = {"request complete": "http.complete", "home": "shop.browse", "order placed": "shop.checkout"}
PREFIXES = {"[PlaceOrder]": "checkout.start", "payment went through (": "payment.accepted",
            "order confirmation email sent to ": "confirmation.sent"}


def nanos(stamp):
    match = re.fullmatch(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d{1,9}))?Z", stamp)
    if not match:
        raise ValueError("expected RFC3339 UTC time ending in Z")
    whole = dt.datetime.strptime(match[1], "%Y-%m-%dT%H:%M:%S")
    return calendar.timegm(whole.timetuple()) * 10**9 + int((match[2] or "").ljust(9, "0"))


def attributes(**pairs):
    return [{"key": k, "value": {"stringValue": str(v)}} for k,v in pairs.items()]


def selected_logs(lines, start, end, available, identity, run_id):
    stats, occurrences, out = Counter(), Counter(), []
    for line in lines:
        stats["read"] += 1
        try:
            stamp, body = line.split(" ", 1)
            event_time = nanos(stamp)
            record = json.loads(body)
            if not isinstance(record, dict):
                raise ValueError("not a structured log")
        except (ValueError, TypeError):
            stats["malformed_or_unstructured"] += 1
            continue
        if not start <= event_time < end:
            stats["outside_interval"] += 1
            continue
        msg = record.get("message", record.get("msg"))
        event = EVENTS.get(msg) if isinstance(msg, str) else None
        if not event and isinstance(msg, str):
            event = next((v for k,v in PREFIXES.items() if msg.startswith(k)), None)
        if not event:
            stats["outside_selection"] += 1
            continue
        key = (event_time, event)
        occurrences[key] += 1
        event_id = hashlib.sha256(json.dumps([identity, *key, occurrences[key]]).encode()).hexdigest()
        out.append({"timeUnixNano": str(event_time), "observedTimeUnixNano": str(available),
                    "body": {"stringValue": event},
                    "attributes": attributes(**{"cct.event_id": event_id, "cct.run_id": run_id,
                                                "cct.schema_version": "1.0"})})
        stats["selected"] += 1
    return out, dict(stats)


def kubectl(*args):
    return subprocess.check_output(["kubectl", "--request-timeout=15s", "-n", "cct-app", *args], text=True)


def resources(pod):
    md = pod["metadata"]
    return {"attributes": attributes(**{"service.name": md["labels"]["app"], "k8s.pod.uid": md["uid"],
                                        "k8s.namespace.name": "cct-app"})}


def pod_metrics(pod, observed, run):
    status = pod.get("status", {})
    samples = []
    ready = next((c["status"] for c in status.get("conditions", []) if c["type"] == "Ready"), "Unknown")
    for name, value in [("cct.pod.ready", {"True": 1, "False": 0}.get(ready)),
                        ("cct.pod.restarts", sum(c["restartCount"] for c in status["containerStatuses"])
                         if status.get("containerStatuses") else None)]:
        if value is not None:
            point = {"timeUnixNano": str(observed), "asInt": str(value),
                     "attributes": attributes(**{"cct.run_id": run, "cct.schema_version": "1.0"})}
            samples.append({"name": name, "unit": "1", "gauge": {"dataPoints": [point]}})
    return {"resource": resources(pod), "scopeMetrics": [{"metrics": samples}]}


def post(endpoint, signal, obj):
    payload = json.dumps(obj).encode()
    req = request.Request(endpoint.rstrip("/") + "/v1/" + signal, data=payload,
                          headers={"Content-Type": "application/json"})
    with request.urlopen(req, timeout=10) as response:
        result = json.loads(response.read() or b"{}")
        partial = result.get("partialSuccess", {})
        if partial.get("errorMessage") or any(int(v) for k,v in partial.items() if k.startswith("rejected")):
            raise ValueError("Collector reported partial success: " + str(partial))


def capture(run, since, until, directory, endpoint=None):
    start, end = nanos(since), nanos(until)
    if not start < end <= time.time_ns():
        raise ValueError("a finished, nonempty UTC interval is required")
    directory.mkdir(parents=True, exist_ok=False)
    logs, metrics, counts, errors = [], [], {}, []
    pods = json.loads(kubectl("get", "pods", "-l", "app", "-o", "json"))["items"]
    observed = time.time_ns()
    seen = set()
    observed_gauges = {}
    for pod in pods:
        md = pod["metadata"]
        service = md["labels"]["app"]
        if service not in APP_COMPONENTS:
            continue
        group = pod_metrics(pod, observed, run)
        metrics.append(group)
        observed_gauges[md["uid"]] = {"service":service, "gauges":sorted(
            m["name"] for m in group["scopeMetrics"][0]["metrics"])}
        if service not in LOG_SERVICES:
            continue
        seen.add(service)
        for container in pod["spec"]["containers"]:
            key = md["uid"] + "/" + container["name"]
            try:
                lines = kubectl("logs", md["name"], "-c", container["name"], "--timestamps", "--since-time="+since)
                records, counts[key] = selected_logs(lines.splitlines(), start, end, time.time_ns(), key, run)
                logs.append({"resource": resources(pod), "scopeLogs": [{"logRecords": records}]})
            except (OSError, subprocess.CalledProcessError) as exc:
                errors.append({"pod": md["name"], "error": type(exc).__name__})
    absent = sorted(LOG_SERVICES - seen)
    payloads = {"logs": {"resourceLogs": logs}, "metrics": {"resourceMetrics": metrics}}
    for sig, obj in payloads.items():
        (directory / (sig+".json")).write_text(json.dumps(obj) + "\n")
    selected_by_service = Counter()
    for group in logs:
        service = next(a["value"]["stringValue"] for a in group["resource"]["attributes"] if a["key"]=="service.name")
        selected_by_service[service] += len(group["scopeLogs"][0]["logRecords"])
    missing_gauges = sorted(uid for uid,row in observed_gauges.items() if len(row["gauges"]) != 2)
    missing_components = sorted(APP_COMPONENTS - {r["service"] for r in observed_gauges.values()})
    log_complete = not errors and not absent and all(selected_by_service[s] > 0 for s in LOG_SERVICES)
    summary = {"schema_version": "1.0", "run_id": run, "log_interval_utc": [since, until],
               "pod_state_observed_ns": str(observed), "pod_count": len(pods), "log_counts": counts,
               "selected_by_service": dict(selected_by_service), "absent_log_services": absent,
               "read_errors": errors, "forwarded": [],
               "log_selection_complete": log_complete, "pod_state_gauges": observed_gauges,
               "missing_gauge_pods": missing_gauges, "missing_components": missing_components,
               "capture_complete": log_complete and not missing_gauges and not missing_components}
    try:
        if endpoint:
            for sig,obj in payloads.items():
                post(endpoint, sig, obj)
                summary["forwarded"].append(sig)
    finally:
        (directory / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    return summary


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--output", required=True)
    p.add_argument("--evidence", type=Path, help="read the probe run ID and UTC bounds from its JSON receipt")
    for name in ("run-id","since","until"):
        p.add_argument("--"+name)
    p.add_argument("--otlp-http", help="optional approved Collector HTTP endpoint")
    a = p.parse_args()
    if a.evidence:
        if any((a.run_id,a.since,a.until)):
            p.error("choose --evidence or explicit run and bounds")
        result = json.loads(a.evidence.read_text())
        a.run_id = result["run_id"]
        a.since = result["started_at"].replace("+00:00","Z")
        a.until = result["finished_at"].replace("+00:00","Z")
    if not all((a.run_id,a.since,a.until)):
        p.error("supply --evidence or --run-id, --since, and --until")
    summary = capture(a.run_id, a.since, a.until, Path(a.output), a.otlp_http)
    print(json.dumps(summary, indent=2))
    raise SystemExit(0 if summary["capture_complete"] else 1)
