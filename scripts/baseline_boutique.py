#!/usr/bin/env python3
"""Run the CCT-02 baseline and retain workload, readiness and resource evidence.

Requires the already-deployed local Boutique and its native image lock. Samples
Kubernetes every ~5 seconds; does not install a monitoring stack or inject faults.
"""

import datetime
import hashlib
import json
from pathlib import Path
import subprocess
import time

ROOT = Path(__file__).resolve().parents[1]
KUBE = ["kubectl", "--kubeconfig", str(ROOT / ".local/kubeconfig"),
        "--context", "kind-cloud-chaos-twin", "--request-timeout=15s"]
NODE = "cloud-chaos-twin-control-plane"
NS = "cct-app"
DURATION_TOLERANCE_SECONDS = 0.01


def command(args, **kwargs):
    return subprocess.check_output(args, text=True, timeout=30, **kwargs).strip()


def kjson(args):
    return json.loads(command(KUBE + args))


def save(path, obj):
    path.write_text(json.dumps(obj, indent=2) + "\n")


def snapshot(run_pod):
    before = time.time()
    pods = kjson(["get", "pods", "-A", "-o", "json"])["items"]
    node = kjson(["get", "node", NODE, "-o", "json"])
    usage = kjson(["get", "--raw", "/api/v1/nodes/" + NODE + "/proxy/stats/summary"])
    observed = []
    for p in pods:
        if p["metadata"]["name"] == run_pod:
            continue
        status = p["status"]
        observed.append({"namespace": p["metadata"]["namespace"], "name": p["metadata"]["name"],
                         "app": p["metadata"].get("labels", {}).get("app"),
                         "uid": p["metadata"]["uid"], "phase": status["phase"],
                         "ready": any(c["type"] == "Ready" and c["status"] == "True"
                                      for c in status.get("conditions", [])),
                         "containers": [{"name": c["name"], "restarts": c["restartCount"],
                                         "image_id": c.get("imageID"), "state": c.get("state"),
                                         "last_state": c.get("lastState")}
                                        for c in status.get("containerStatuses", [])]})
    return {"sample_started_epoch": before, "sample_ended_epoch": time.time(), "pods": observed,
            "node_conditions": {c["type"]: c["status"] for c in node["status"]["conditions"]},
            "node_capacity": node["status"]["capacity"], "node_usage": usage["node"],
            "pod_usage": [{"name": p["podRef"]["name"], "cpu": p.get("cpu"), "memory": p.get("memory"),
                           "containers": [{"name": c["name"], "cpu": c.get("cpu"), "memory": c.get("memory")}
                                          for c in p.get("containers", [])]}
                          for p in usage["pods"] if p["podRef"]["namespace"] == NS]}


def evaluate(measurement, samples, initial, exit_code):
    start, end = measurement["measurement_started_epoch"], measurement["measurement_ended_epoch"]
    window = [s for s in samples if start <= s["sample_started_epoch"] <= end]
    failures = []
    # Event-loop timers can fire a fraction of a millisecond early. Keep the
    # actual duration in evidence and allow only 10 ms of timer precision.
    if measurement["measured_seconds"] < 900 - DURATION_TOLERANCE_SECONDS:
        failures.append("Measured window shorter than 900 seconds beyond the 10 ms timer tolerance")
    if measurement["requests"] <= 0 or measurement["failures"] or measurement["locust_errors"] or exit_code:
        failures.append("Workload failed or produced no requests")
    if measurement["users"] != 5 or measurement["users_at_end"] != 5:
        failures.append("Expected five users throughout the measured run")
    for method, path in [("GET", "/"), ("POST", "/cart"), ("POST", "/cart/checkout")]:
        if not any(e["method"] == method and e["name"] == path and e["requests"] > 0
                   for e in measurement["endpoints"]):
            failures.append("Missing journey requests: " + method + " " + path)
    if not any(e["method"] == "GET" and e["name"].startswith("/product/") and e["requests"] > 0
               for e in measurement["endpoints"]):
        failures.append("Missing product browsing requests")
    expected = {p["uid"]: p for p in initial["pods"]}
    app_names = {p["app"] for p in initial["pods"] if p["namespace"] == NS}
    required = {"adservice", "shippingservice", "checkoutservice", "currencyservice", "emailservice",
                "productcatalogservice", "frontend", "recommendationservice", "cartservice", "redis-cart", "paymentservice"}
    if app_names != required:
        failures.append("Initial application service set differs from the required 11 components")
    restarts = {p["uid"]: sum(c["restarts"] for c in p["containers"]) for p in initial["pods"]}
    good = []
    for sample in window:
        if "error" in sample:
            failures.append("Resource/readiness sample failed")
            continue
        good.append(sample)
        if {p["uid"] for p in sample["pods"]} != set(expected):
            failures.append("Pod replacement, removal or unexpected additional pod during baseline")
        for p in sample["pods"]:
            if not p["ready"] or p["phase"] != "Running":
                failures.append("Pod unready: " + p["name"])
            if sum(c["restarts"] for c in p["containers"]) != restarts.get(p["uid"], 0):
                failures.append("Container restart: " + p["name"])
            if any(c.get("state", {}).get("terminated", {}).get("reason") == "OOMKilled"
                   or c.get("last_state", {}).get("terminated", {}).get("reason") == "OOMKilled"
                   for c in p["containers"]):
                failures.append("OOM termination observed: " + p["name"])
        conditions = sample["node_conditions"]
        if conditions.get("Ready") != "True" or any(conditions.get(k) != "False"
                for k in ("MemoryPressure", "DiskPressure", "PIDPressure")):
            failures.append("Node unready or under resource pressure")
    times = [start] + [s["sample_started_epoch"] for s in good] + [end]
    max_gap = max(b-a for a, b in zip(times, times[1:]))
    if not good or max_gap > 15:
        failures.append("Readiness/resource coverage gap exceeds 15 seconds")
    memory = [s["node_usage"]["memory"]["workingSetBytes"] for s in good]
    cpu = [s["node_usage"]["cpu"].get("usageNanoCores", 0) / 1e9 for s in good]
    capacity = int(initial["node_capacity"]["memory"].removesuffix("Ki")) * 1024
    cpu_capacity = int(initial["node_capacity"]["cpu"])
    peak_memory = max(memory, default=capacity)
    if peak_memory / capacity > 0.8:
        failures.append("Less than 20% VM memory headroom at the measured peak")
    sustained = None
    for sample, used in zip(good, cpu):
        if used / cpu_capacity > 0.8:
            sustained = sustained or sample["sample_started_epoch"]
            if sample["sample_started_epoch"] - sustained >= 60:
                failures.append("CPU usage above 80% for at least 60 seconds")
        else:
            sustained = None
    return {"passed": not failures, "failure_reasons": sorted(set(failures)),
            "duration_tolerance_seconds": DURATION_TOLERANCE_SECONDS,
            "sample_count": len(good), "max_sampling_gap_seconds": round(max_gap, 2),
            "node_memory_capacity_bytes": capacity, "peak_node_working_set_bytes": peak_memory,
            "minimum_vm_memory_headroom_percent": round(100*(1-peak_memory/capacity), 1),
            "mean_node_cpu_cores": round(sum(cpu)/len(cpu), 3) if cpu else None,
            "peak_node_cpu_cores": round(max(cpu, default=0), 3),
            "node_cpu_capacity": cpu_capacity,
            "measurement": measurement,
            "limits": ["Readiness and resource usage sampled approximately every five seconds.",
                       "CPU/memory values describe the Linux VM, not all macOS applications.",
                       "Latency percentiles at low request counts are descriptive.",
                       "Five closed-loop users do not produce a fixed request arrival rate."]}


def main():
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    name = "cct-baseline-" + stamp.lower()
    directory = ROOT / "outputs/baselines" / stamp
    directory.mkdir(parents=True)
    initial = snapshot(name)
    if not all(p["ready"] for p in initial["pods"]):
        raise RuntimeError("All existing pods must be Ready before starting")
    save(directory / "initial.json", initial)
    lock = json.loads((ROOT / "configs/boutique.local.lock.json").read_text())
    save(directory / "image-lock.json", lock)
    workload = (ROOT / "workload/baseline_locust.py").read_text()
    config = {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": name, "namespace": NS},
              "data": {"locustfile.py": workload}}
    script = ("python -m locust -f /cct/locustfile.py --host=http://frontend "
              "--headless -u 5 -r 1 --run-time 20m --only-summary "
              "--csv=/results/requests --csv-full-history; "
              "result=$?; echo $result > /results/exit-code; sleep 86400")
    pod = {"apiVersion": "v1", "kind": "Pod", "metadata": {"name": name, "namespace": NS,
            "labels": {"app": "cct-baseline", "cct-run": name}}, "spec": {
        "restartPolicy": "Never", "terminationGracePeriodSeconds": 5,
        "automountServiceAccountToken": False,
        "securityContext": {"runAsUser": 1000, "runAsGroup": 1000, "runAsNonRoot": True, "fsGroup": 1000},
        "containers": [{"name": "locust", "image": lock["images"]["loadgenerator"]["image"],
            "imagePullPolicy": "Never", "command": ["/bin/sh", "-c", script],
            "env": [{"name": "CCT_SEED", "value": "42"}],
            "resources": {"requests": {"cpu": "100m", "memory": "128Mi"},
                          "limits": {"cpu": "500m", "memory": "512Mi"}},
            "securityContext": {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True,
                                "capabilities": {"drop": ["ALL"]}},
            "volumeMounts": [{"name": "workload", "mountPath": "/cct", "readOnly": True},
                             {"name": "results", "mountPath": "/results"},
                             {"name": "tmp", "mountPath": "/tmp"}]}],
        "volumes": [{"name": "workload", "configMap": {"name": name}},
                    {"name": "results", "emptyDir": {}}, {"name": "tmp", "emptyDir": {}}]}}
    save(directory / "run-config.json", {"pod": pod, "warmup_seconds": 120, "measured_seconds": 900,
         "users": 5, "spawn_rate": 1, "seed": 42, "wait_seconds": [1, 10],
         "workload_sha256": hashlib.sha256(workload.encode()).hexdigest(),
         "upstream_workload_sha256": hashlib.sha256((ROOT / "vendor/online-boutique/src/loadgenerator/locustfile.py").read_bytes()).hexdigest()})
    for obj in (config, pod):
        command(KUBE + ["create", "-f", "-"], input=json.dumps(obj))
    print("Baseline run: " + name + "\nEvidence: " + str(directory), flush=True)
    samples = []
    deadline, next_report = time.monotonic() + 1250, 0
    done = False
    with (directory / "resources.jsonl").open("w", buffering=1) as output:
        while time.monotonic() < deadline:
            started = time.monotonic()
            try:
                sample = snapshot(name)
            except (subprocess.SubprocessError, ValueError, KeyError) as error:
                sample = {"sample_started_epoch": time.time(), "error": str(error)}
            samples.append(sample)
            output.write(json.dumps(sample) + "\n")
            if time.monotonic() >= next_report:
                status = subprocess.run(KUBE + ["-n", NS, "exec", name, "--", "cat", "/results/progress.json"],
                                        capture_output=True, text=True, timeout=30)
                if status.returncode == 0:
                    progress = json.loads(status.stdout)
                    elapsed = time.time()-progress.get("measurement_started_epoch", time.time())
                    print("phase={}; measured={:.0f}s; requests={}; failures={}".format(
                        progress["phase"], max(0, elapsed), progress["requests"], progress["failures"]), flush=True)
                    if progress["phase"] == "complete":
                        done = True
                        break
                else:
                    print("Waiting for workload startup...", flush=True)
                next_report = time.monotonic() + 30
            time.sleep(max(0, 5-(time.monotonic()-started)))
    logs = command(KUBE + ["-n", NS, "logs", name])
    (directory / "locust.log").write_text(logs + "\n")
    subprocess.run(KUBE + ["-n", NS, "cp", name + ":/results/.", str(directory / "locust")], check=True)
    if not done:
        raise RuntimeError("Baseline did not finish; workload pod retained for diagnosis")
    measurement = json.loads((directory / "locust/baseline.json").read_text())
    exit_code = int((directory / "locust/exit-code").read_text())
    result = evaluate(measurement, samples, initial, exit_code)
    save(directory / "summary.json", result)
    save(directory / "final.json", snapshot(name))
    (directory / "events.json").write_text(command(KUBE + ["-n", NS, "get", "events", "-o", "json"]) + "\n")
    command(KUBE + ["-n", NS, "delete", "pod", name, "--wait=true", "--timeout=20s"])
    command(KUBE + ["-n", NS, "delete", "configmap", name])
    print(json.dumps({"passed": result["passed"], "failure_reasons": result["failure_reasons"],
                      "summary": str(directory / "summary.json")}, indent=2), flush=True)
    if not result["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
