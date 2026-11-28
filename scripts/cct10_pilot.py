#!/usr/bin/env python3
"""Owner-run, bounded productcatalog delay pilot. Evidence is partial, not calibration.

Only creates its own disposable load pod/config and the admission-fenced 30s
NetworkChaos. Requires the existing healthy Boutique, monitoring and Chaos Mesh.
Touch OUTPUT/STOP to abort. Run with .local/env loaded and keep the host online.
"""
import argparse
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
import datetime as dt
import gzip
import hashlib
import importlib.util
import json
import math
from pathlib import Path
import queue
import re
import signal
import socket
import subprocess
import sys
import threading
import time
from urllib import request, parse, error

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from observability import lineage, signals


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    obj = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(obj)
    return obj


HANDOFF = ROOT / "CCT-10-Merul-Handoff-2026-11-26"
pods_helper = module("pilot_pods", HANDOFF / "capture_pods.py")
join_helper = module("pilot_join", HANDOFF / "join_windows.py")
KUBE = ["kubectl", "--kubeconfig", str(ROOT / ".local/kubeconfig"),
        "--context", "kind-cloud-chaos-twin", "--request-timeout=4s"]
FAULT = "cct-productcatalog-delay"
NS = "cct-app"


def ms():
    return (time.time_ns() + 999999) // 1000000


def utc(epoch=None):
    return dt.datetime.fromtimestamp(epoch if epoch is not None else time.time(), dt.timezone.utc).isoformat().replace("+00:00", "Z")


def save(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n")


def kube(args, obj=None):
    result = subprocess.run(KUBE + args, input=json.dumps(obj) if obj is not None else None,
                            capture_output=True, text=True, timeout=8)
    if result.returncode:
        raise RuntimeError(result.stderr.strip() or "Kubernetes request failed")
    return result.stdout


def kjson(args, obj=None):
    return json.loads(kube(args, obj) or "{}")


def http(base, path):
    with request.urlopen(base + path, timeout=3) as response:
        return json.load(response)


def fault_manifest(run):
    return {"apiVersion": "chaos-mesh.org/v1alpha1", "kind": "NetworkChaos",
            "metadata": {"name": FAULT, "namespace": NS, "annotations": {"cct.run_id": run}},
            "spec": {"action": "delay", "mode": "one", "direction": "to",
                     "selector": {"namespaces": [NS], "labelSelectors": {"app": "productcatalogservice"}},
                     "delay": {"latency": "100ms", "jitter": "10ms", "correlation": "0"}, "duration": "30s"}}


def owned_delete(kind, name, run):
    obj = kjson(["-n", NS, "get", kind, name, "--ignore-not-found", "-o", "json"])
    if not obj:
        return
    if obj["metadata"].get("annotations", {}).get("cct.run_id") != run:
        raise RuntimeError("Refusing cleanup of a resource owned by another run: " + name)
    # Preserve normal controller recovery/finalizers; never force deletion.
    kube(["-n", NS, "delete", kind, name, "--wait=false"])


def health_inventory(items):
    result = {}
    for p in items:
        md, status = p["metadata"], p.get("status", {})
        if md["namespace"] not in {NS, "cct-observability", "cct-chaos-system"}:
            continue
        if md.get("labels", {}).get("cct.pilot"):
            continue
        key = md["namespace"] + "/" + md["name"]
        result[key] = {"uid": md["uid"], "ready": any(c.get("type") == "Ready" and c.get("status") == "True"
                     for c in status.get("conditions", [])),
                     "restarts": {c["name"]: c["restartCount"] for c in status.get("containerStatuses", [])},
                     "deleting": bool(md.get("deletionTimestamp"))}
    return result


def target_failures(targets, previous):
    """Count distinct failed scrapes, never repeated polls of one failure."""
    by_job = defaultdict(list)
    for t in targets:
        by_job[t["labels"].get("job")].append(t)
    failed = False
    for job in ("cct-service", "cct-collector"):
        rows = by_job.get(job, [])
        if len(rows) != 1:
            raise RuntimeError("Expected exactly one monitoring target for " + job)
        row = rows[0]
        old_stamp, n = previous.get(job, (None, 0))
        if row.get("health") == "up":
            previous[job] = (row.get("lastScrape"), 0)
        elif row.get("lastScrape") != old_stamp:
            previous[job] = (row.get("lastScrape"), n + 1)
        failed |= previous.get(job, (None, 0))[1] >= 2
    return failed


class Archive:
    """Continuously retain observations before Jaeger's 1000-trace eviction.

    Requeries at 2/8/20/40s after each HTTP completion. Saved data is still
    explicitly partial: no independent backend denominator is inferred here.
    """
    def __init__(self, base, directory):
        self.base, self.directory = base, directory
        self.pending = queue.Queue()
        self.entries, self.errors = {}, []
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()

    def fetch(self, trace):
        started = ms()
        try:
            data = http(self.base, "/api/traces/" + trace)
            return {"trace_id": trace, "query_started_ms": started, "observed_ms": ms(),
                    "data": data.get("data", []), "errors": data.get("errors")}
        except (OSError, ValueError) as exc:
            return {"trace_id": trace, "query_started_ms": started, "observed_ms": ms(),
                    "data": [], "error": type(exc).__name__}

    def run(self):
        schedule, active = [], {}
        with gzip.open(self.directory / "jaeger-observations.jsonl.gz", "wt") as out, ThreadPoolExecutor(max_workers=6) as pool:
            while not self.stop.is_set() or schedule or active or not self.pending.empty():
                while not self.pending.empty():
                    event = self.pending.get()
                    trace = event["trace_id"]
                    if trace in self.entries:
                        continue
                    self.entries[trace] = {"client": event, "trace": None, "span_observed_ms": {}, "queries": 0}
                    for delay in (2, 8, 20, 40):
                        schedule.append((event["completed_epoch"] + delay, trace))
                now = time.time()
                due = [x for x in schedule if x[0] <= now]
                schedule = [x for x in schedule if x[0] > now]
                for _, trace in due:
                    active[pool.submit(self.fetch, trace)] = trace
                for future in list(active):
                    if not future.done():
                        continue
                    trace = active.pop(future)
                    observation = future.result()
                    out.write(json.dumps(observation) + "\n")
                    out.flush()
                    entry = self.entries[trace]
                    entry["queries"] += 1
                    if observation.get("error"):
                        self.errors.append({k:v for k,v in observation.items() if k != "data"})
                    for current in observation["data"]:
                        if current.get("traceID") != trace:
                            continue
                        for span in current["spans"]:
                            entry["span_observed_ms"].setdefault(span["spanID"], observation["observed_ms"])
                        # Each response is a coherent Jaeger snapshot. Do not union
                        # conflicting spans or process IDs from different responses.
                        if entry["trace"] is None or len(current["spans"]) >= len(entry["trace"]["spans"]):
                            entry["trace"] = current
                            entry["observed_ms"] = observation["observed_ms"]
                time.sleep(0.1)

    def finish(self):
        self.stop.set()
        self.thread.join(timeout=60)
        if self.thread.is_alive():
            raise RuntimeError("Trace archival did not finish")
        with gzip.open(self.directory / "traces.jsonl.gz", "wt") as out:
            for trace, entry in self.entries.items():
                out.write(json.dumps({"trace_id": trace, **entry}) + "\n")


def forward(service, port, directory, processes):
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        local = sock.getsockname()[1]
    log = (directory / (service + "-forward.log")).open("w")
    proc = subprocess.Popen(KUBE + ["-n", "cct-observability", "port-forward", "--address=127.0.0.1",
                            "svc/" + service, f"{local}:{port}"], stdout=log, stderr=log)
    processes.append((proc, log))
    base = f"http://127.0.0.1:{local}"
    for _ in range(60):
        if proc.poll() is not None:
            raise RuntimeError("Monitoring forward exited: " + service)
        try:
            with socket.create_connection(("127.0.0.1", local), timeout=0.1):
                return base
        except OSError:
            time.sleep(0.1)
    raise RuntimeError("Monitoring forward was not ready: " + service)


def manifests(run):
    code = (ROOT / "workload/fault_pilot_locust.py").read_text()
    md = {"name": run, "namespace": NS, "annotations": {"cct.run_id": run}, "labels": {"cct.pilot": "true"}}
    config = {"apiVersion": "v1", "kind": "ConfigMap", "metadata": md, "data": {"locustfile.py": code}}
    image = json.loads((ROOT / "configs/boutique.local.lock.json").read_text())["images"]["loadgenerator"]["image"]
    pod = {"apiVersion": "v1", "kind": "Pod", "metadata": md, "spec": {
        "restartPolicy": "Never", "activeDeadlineSeconds": 700, "terminationGracePeriodSeconds": 5,
        "automountServiceAccountToken": False,
        "securityContext": {"runAsUser": 1000, "runAsGroup": 1000, "runAsNonRoot": True,
                            "fsGroup": 1000, "seccompProfile": {"type": "RuntimeDefault"}},
        "containers": [{"name": "locust", "image": image, "imagePullPolicy": "Never",
            "command": ["/bin/sh", "-c", "python -B -u -m locust -f /cct/locustfile.py --host=http://frontend --headless -u 5 -r 1 --run-time 9m --only-summary; result=$?; echo $result >/results/exit-code; sleep 120"],
            "env": [{"name": "CCT_RUN_ID", "value": run}],
            "resources": {"requests": {"cpu": "100m", "memory": "128Mi"}, "limits": {"cpu": "500m", "memory": "512Mi"}},
            "securityContext": {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True, "capabilities": {"drop": ["ALL"]}},
            "volumeMounts": [{"name": "workload", "mountPath": "/cct", "readOnly": True},
                             {"name": "results", "mountPath": "/results"}, {"name": "tmp", "mountPath": "/tmp"}]}],
        "volumes": [{"name": "workload", "configMap": {"name": run}}, {"name": "results", "emptyDir": {}},
                    {"name": "tmp", "emptyDir": {}}]}}
    return config, pod


def derive(directory, run, archive, measured_start, measured_end):
    groups, counts, invalid, journey_examples = defaultdict(list), Counter(), [], {}
    server_count = 0
    with gzip.open(directory / "server-records.jsonl.gz", "wt") as output:
        for trace_id, entry in archive.entries.items():
            trace, client = entry["trace"], entry["client"]
            if not trace:
                continue
            try:
                stats = lineage.inspect_trace(trace, trace_id, client["external_parent"], include_records=True)
            except (ValueError, KeyError) as exc:
                invalid.append({"trace_id": trace_id, "error": str(exc)})
                continue
            name = {("GET", "/"): "browse", ("POST", "/cart"): "cart", ("POST", "/cart/checkout"): "checkout"}.get((client["method"], client["path"]))
            if name:
                cov = lineage.coverage(name, stats)
                counts[name + "_traces"] += 1
                if cov["passes_declared_path"]:
                    counts[name + "_declared_path_passes"] += 1
                    if measured_start is not None and measured_start <= client["started_epoch"] <= measured_end:
                        journey_examples.setdefault(name, {"trace_id": trace_id, **cov, "services": stats["services"]})
            for span in stats["server_records"]:
                end_us = span["start_us"] + span["duration_us"]
                record = {"run_id": run, **span, "available_ms": max(math.ceil(end_us/1000), entry["observed_ms"])}
                output.write(json.dumps(record) + "\n")
                server_count += 1
                if measured_start is None or not measured_start*1e6 <= end_us < measured_end*1e6:
                    continue
                start = end_us // 5000000 * 5000
                groups[span["service"], start].append(record)
    rows = []
    for (service, start), records in sorted(groups.items(), key=lambda x:(x[0][1],x[0][0])):
        vals = sorted(s["duration_us"] for s in records)
        rows.append({"run_id": run, "service": service, "start_ms": start, "end_ms": start+5000,
                     "requests": len(vals), "errors": sum(s["error"] for s in records),
                     "p95_ms": vals[math.ceil(.95*len(vals))-1]/1000,
                     "available_ms": max(start+5000, max(s["available_ms"] for s in records)),
                     "ready": None, "restart_delta": None, "complete": False})
    save(directory / "service-windows.json", rows)
    joined = []
    if rows:
        samples = [json.loads(s) for s in (directory / "pods.jsonl").read_text().splitlines()]
        joined = join_helper.join(rows, samples, [], run)
        save(directory / "service-windows.partial.json", joined)
    return {"workload_traces": len(archive.entries), "saved_traces": sum(e["trace"] is not None for e in archive.entries.values()),
            "server_records": server_count, "journey_counts": dict(counts), "journey_examples": journey_examples,
            "invalid_traces": invalid, "query_errors": len(archive.errors), "windows": len(rows),
            "fresh_readiness_windows": sum(r["ready"] is True and r["restart_delta"] == 0 for r in joined),
            "complete_windows": 0, "calibration_eligible_windows": 0,
            "coverage": "No independent per-service denominator; all windows remain incomplete."}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    run = args.run_id
    if not re.fullmatch(r"cct-probe-[a-z0-9-]{1,45}", run):
        parser.error("Use a unique cct-probe-* DNS label")
    directory = ROOT / "outputs/experiments" / run
    directory.mkdir(parents=True, exist_ok=False)
    receipt = {"run_id": run, "started_at": utc(), "source_revision": subprocess.check_output(["git","rev-parse","HEAD"],cwd=ROOT,text=True).strip(),
               "actor": "owner-assisted controlled pilot", "passed": False, "fault": {}, "cleanup_errors": [],
               "protocol": {"users":5,"spawn_rate":1,"seed":42,"think_seconds":[1,10],"warmup_seconds":120,
                            "healthy_seconds":120,"fault_seconds":30,"recovery_seconds":120}}
    save(directory / "protocol.json", receipt)
    event_queue, processes = queue.Queue(), []
    archive, reader, logproc = None, None, None
    abort = threading.Event()
    recovered = threading.Event()
    fault_attempted, workload_attempted = False, False
    phases, requests = [], []
    measured_start = measured_end = None
    signal.signal(signal.SIGTERM, lambda *_: abort.set())
    signal.signal(signal.SIGINT, lambda *_: abort.set())
    try:
        if kjson(["get", "networkchaos", "-A", "-o", "json"])["items"]:
            raise RuntimeError("An existing fault must be cleared by its owner before this pilot")
        for kind in ("pod", "configmap"):
            if kjson(["-n", NS, "get", kind, run, "--ignore-not-found", "-o", "json"]):
                raise RuntimeError("Workload name already exists")
        initial = kjson(["get", "pods", "-A", "-o", "json"])["items"]
        baseline = health_inventory(initial)
        app = [p for p in initial if p["metadata"]["namespace"] == NS]
        if {p["metadata"].get("labels",{}).get("app") for p in app} != signals.APP_COMPONENTS or len(app) != 11:
            raise RuntimeError("Expected exactly the eleven deployed Boutique components")
        if not baseline or not all(p["ready"] and not p["deleting"] for p in baseline.values()):
            raise RuntimeError("Application, monitoring and fault controller must all be Ready")
        save(directory / "initial-health.json", baseline)
        jaeger = forward("cct-jaeger", 16686, directory, processes)
        prometheus = forward("cct-prometheus", 9090, directory, processes)
        targets = http(prometheus, "/api/v1/targets")["data"]["activeTargets"]
        previous = {}
        target_failures(targets, previous)
        if any(t.get("health") != "up" for t in targets if t["labels"].get("job") in {"cct-service","cct-collector"}):
            raise RuntimeError("Monitoring targets must both be up before starting")
        if not http(jaeger, "/api/services").get("data"):
            raise RuntimeError("Jaeger has no service data")
        manifest = fault_manifest(run)
        save(directory / "fault.request.json", manifest)
        kube(["create", "--dry-run=server", "-f", "-"], manifest)
        config, pod = manifests(run)
        save(directory / "workload.json", pod)
        save(directory / "source-hashes.json", {name:hashlib.sha256((ROOT/name).read_bytes()).hexdigest()
             for name in ["workload/fault_pilot_locust.py","scripts/cct10_pilot.py","configs/boutique.local.lock.json",
                          "CCT-10-Merul-Handoff-2026-11-26/capture_pods.py","CCT-10-Merul-Handoff-2026-11-26/join_windows.py"]})
        archive = Archive(jaeger, directory)
        workload_attempted = True
        kube(["create", "-f", "-"], config)
        kube(["create", "-f", "-"], pod)
        deadline = time.monotonic() + 550
        # Wait for startup while still refusing any application health change.
        while True:
            current = kjson(["get","pods","-A","-o","json"])["items"]
            if health_inventory(current) != baseline:
                raise RuntimeError("Health changed while starting the workload")
            own = next(p for p in current if p["metadata"]["namespace"] == NS and p["metadata"]["name"] == run)
            if own.get("status",{}).get("phase") == "Running":
                break
            if time.monotonic() > deadline-520 or abort.is_set():
                raise RuntimeError("Workload did not start promptly")
            time.sleep(1)
        logproc = subprocess.Popen(KUBE + ["-n",NS,"logs","-f",run,"--pod-running-timeout=10s"],
                                   stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True)
        def read_logs():
            with (directory / "workload-console.log").open("w") as output:
                for line in logproc.stdout:
                    output.write(line); output.flush()
                    if line.startswith("CCT_"):
                        kind, value = line.split(" ",1)
                        try:
                            event_queue.put((kind,json.loads(value)))
                        except ValueError:
                            abort.set()
        reader = threading.Thread(target=read_logs,daemon=True)
        reader.start()
        print("Pilot workload started; continuous capture active: " + run,flush=True)
        next_prom = next_resources = next_report = 0
        finish_sent = False
        workload_complete = False
        with (directory/"pods.jsonl").open("x",buffering=1) as podout, (directory/"health.jsonl").open("x",buffering=1) as healthout, (directory/"prometheus.jsonl").open("x",buffering=1) as promout, (directory/"fault-status.jsonl").open("x",buffering=1) as faultout, (directory/"requests.jsonl").open("x",buffering=1) as reqout:
            while not workload_complete:
                tick, started = time.monotonic(), ms()
                while not event_queue.empty():
                    kind, value = event_queue.get()
                    if kind == "CCT_EVENT":
                        requests.append(value); reqout.write(json.dumps(value)+"\n")
                        archive.pending.put(value)
                        if not value["success"] or value["status"] >= 500:
                            raise RuntimeError("Workload request failed: " + value["method"] + " " + value["path"])
                    elif kind == "CCT_PHASE":
                        phases.append(value)
                        print("Workload phase: " + value["phase"],flush=True)
                        if value["phase"] == "measured":
                            measured_start = value["measured_started_epoch"]
                        if value["phase"] == "complete":
                            measured_end = value["ended_epoch"]
                            receipt["workload_report"] = value
                            workload_complete = True
                items = kjson(["get","pods","-A","-o","json"])["items"]
                now_health = health_inventory(items)
                app_items = [p for p in items if p["metadata"]["namespace"] == NS and p["metadata"].get("labels",{}).get("app") in signals.APP_COMPONENTS]
                podout.write(json.dumps({"run_id":run,"started_ms":started,"seen_ms":ms(),"services":pods_helper.redact(app_items)})+"\n")
                healthout.write(json.dumps({"seen_ms":ms(),"pods":now_health})+"\n")
                if now_health != baseline:
                    raise RuntimeError("Pod readiness, restart count or identity changed")
                own = next((p for p in items if p["metadata"]["namespace"] == NS and p["metadata"]["name"] == run),{})
                if own.get("status",{}).get("phase") != "Running":
                    raise RuntimeError("Workload pod stopped unexpectedly")
                if abort.is_set() or (directory/"STOP").exists():
                    raise RuntimeError("Stop requested or recovery watchdog expired")
                if time.monotonic() > deadline or logproc.poll() is not None or not archive.thread.is_alive():
                    raise RuntimeError("Workload/capture deadline or process failure")
                if time.monotonic() >= next_prom:
                    target_data = http(prometheus,"/api/v1/targets")["data"]["activeTargets"]
                    # Range vectors preserve scrape timestamps; instant-vector
                    # evaluation timestamps are not relabeled as scrape times.
                    query = '{job=~"cct-service|cct-collector",__name__=~"cct_.*|up|otelcol_.*(accepted|refused|failed|dropped|enqueue_failed).*"}[15s]'
                    raw = http(prometheus,"/api/v1/query?"+parse.urlencode({"query":query}))
                    if raw.get("status") != "success":
                        raise RuntimeError("Prometheus sample query failed")
                    promout.write(json.dumps({"observed_ms":ms(),"targets":target_data,"samples":raw["data"]})+"\n")
                    if target_failures(target_data, previous):
                        raise RuntimeError("Monitoring target down for two distinct scrapes")
                    next_prom = time.monotonic()+5
                if time.monotonic() >= next_resources:
                    usage = kjson(["get","--raw","/api/v1/nodes/cloud-chaos-twin-control-plane/proxy/stats/summary"])["node"]
                    node = kjson(["get","node","cloud-chaos-twin-control-plane","-o","json"])
                    capacity = int(node["status"]["capacity"]["memory"].removesuffix("Ki"))*1024
                    with (directory/"resources.jsonl").open("a") as out:
                        out.write(json.dumps({"observed_ms":ms(),"node":usage,"memory_capacity_bytes":capacity})+"\n")
                    conditions = {c["type"]:c["status"] for c in node["status"]["conditions"]}
                    if usage["memory"]["workingSetBytes"] > .8*capacity or conditions.get("Ready") != "True" or any(conditions.get(k)!="False" for k in ("MemoryPressure","DiskPressure","PIDPressure")):
                        raise RuntimeError("Node resource headroom/health stop condition")
                    next_resources = time.monotonic()+10
                if measured_start is not None and time.time() >= measured_start+120 and not fault_attempted:
                    # Check the cluster again immediately before injection.
                    if kjson(["get","networkchaos","-A","-o","json"])["items"]:
                        raise RuntimeError("Another fault appeared before injection")
                    fault_attempted = True
                    receipt["fault"]["requested_at"] = utc()
                    receipt["fault"]["request_started_ms"] = ms()
                    def watchdog():
                        if not recovered.wait(45):
                            abort.set()
                            try:
                                owned_delete("networkchaos",FAULT,run)
                            except Exception:
                                pass  # Main finally retries and records cleanup errors.
                    threading.Thread(target=watchdog,daemon=True).start()
                    created = kjson(["create","-f","-","-o","json"],manifest)
                    receipt["fault"]["uid"] = created["metadata"]["uid"]
                    receipt["fault"]["request_completed_ms"] = ms()
                    save(directory/"fault.created.json",created)
                    print("Fault requested: productcatalogservice outgoing 100ms ±10ms, 30s",flush=True)
                if fault_attempted and not recovered.is_set():
                    obj = kjson(["-n",NS,"get","networkchaos",FAULT,"-o","json"])
                    faultout.write(json.dumps({"observed_ms":ms(),"object":obj})+"\n")
                    cond = {c["type"]:c["status"] for c in obj.get("status",{}).get("conditions",[])}
                    if cond.get("AllInjected") == "True" and "injected_observed_ms" not in receipt["fault"]:
                        receipt["fault"]["injected_observed_ms"] = ms()
                        save(directory/"fault.injected.json",obj)
                        print("Fault injection confirmed",flush=True)
                    if "injected_observed_ms" in receipt["fault"] and cond.get("AllRecovered") == "True":
                        receipt["fault"]["recovered_observed_ms"] = ms()
                        save(directory/"fault.recovered.json",obj)
                        recovered.set()
                        print("Automatic recovery confirmed; observing for 120 seconds",flush=True)
                if recovered.is_set() and not finish_sent and ms() >= receipt["fault"]["recovered_observed_ms"]+120000:
                    kube(["-n",NS,"exec",run,"--","touch","/results/finish"])
                    finish_sent = True
                if time.monotonic() >= next_report:
                    print(json.dumps({"at":utc(),"requests":len(requests),"measured_seconds":round(time.time()-measured_start,1) if measured_start else None,"fault":receipt["fault"]}),flush=True)
                    next_report = time.monotonic()+30
                    save(directory/"progress.json",{"receipt":receipt,"phases":phases,"requests":len(requests)})
                time.sleep(max(0,1-(time.monotonic()-tick)))
        if not recovered.is_set() or not finish_sent or receipt["workload_report"]["reason"] != "completed" or receipt["workload_report"]["locust_errors"]:
            raise RuntimeError("Pilot did not finish its full recovery protocol")
        receipt["passed"] = True
    except Exception as exc:
        receipt["error"] = str(exc)
        measured_end = measured_end or time.time()
        print("Pilot stopped: " + str(exc),flush=True)
    finally:
        measured_end = measured_end or time.time()
        receipt["measurement_interval_utc"] = [utc(measured_start) if measured_start else None,utc(measured_end)]
        save(directory/"phases.json",phases)
        if fault_attempted:
            try:
                owned_delete("networkchaos",FAULT,run)
            except Exception as exc:
                receipt["cleanup_errors"].append("fault: "+str(exc))
        if workload_attempted:
            try:
                kube(["-n",NS,"exec",run,"--","touch","/results/stop"])
            except Exception as exc:
                receipt["cleanup_errors"].append("workload stop: "+str(exc))
        if archive:
            try:
                print("Saving remaining trace observations",flush=True)
                archive.finish()
                receipt["capture"] = derive(directory,run,archive,measured_start,measured_end)
            except Exception as exc:
                receipt["capture_error"] = str(exc)
        if measured_start is not None:
            try:
                receipt["selected_signals"] = signals.capture(run,utc(measured_start),utc(measured_end),directory/"signals")
            except Exception as exc:
                receipt["signals_error"] = str(exc)
        if workload_attempted:
            for kind in ("pod","configmap"):
                try:
                    owned_delete(kind,run,run)
                except Exception as exc:
                    receipt["cleanup_errors"].append(kind+": "+str(exc))
        if logproc:
            logproc.terminate()
            try:
                logproc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                logproc.kill(); logproc.wait()
        for proc, log in processes:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill(); proc.wait()
            log.close()
        try:
            # Wait for normal finalizer cleanup, never remove finalizers.
            for _ in range(15):
                remaining = kjson(["-n",NS,"get","networkchaos",FAULT,"--ignore-not-found","-o","json"])
                own = kjson(["-n",NS,"get","pod",run,"--ignore-not-found","-o","json"])
                if not remaining and not own:
                    break
                time.sleep(1)
            receipt["remaining_fault"] = bool(remaining)
            receipt["remaining_workload"] = bool(own)
            final = health_inventory(kjson(["get","pods","-A","-o","json"])["items"])
            save(directory/"final-health.json",final)
            receipt["health_unchanged"] = final == baseline
        except Exception as exc:
            receipt["cleanup_errors"].append("final verification: "+str(exc))
        receipt["passed"] &= not receipt["cleanup_errors"] and not receipt.get("remaining_fault",True) and not receipt.get("remaining_workload",True) and receipt.get("health_unchanged",False)
        receipt["finished_at"] = utc()
        save(directory/"receipt.json",receipt)
        print(json.dumps({"passed":receipt["passed"],"error":receipt.get("error"),"evidence":str(directory)}),flush=True)
    return 0 if receipt["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
