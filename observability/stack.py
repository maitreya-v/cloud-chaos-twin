"""Render only namespaced resources as a Kubernetes List. Python 3.9+."""
import argparse
import hashlib
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
NS = "cct-observability"
IMAGES = {"collector": "otel/opentelemetry-collector-contrib:0.128.0",
          "jaeger": "jaegertracing/all-in-one:1.76.0",
          "prometheus": "prom/prometheus:v3.5.0", "probe": "python:3.12.11-alpine3.22"}
SECURITY = {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True,
            "capabilities": {"drop": ["ALL"]}}
POD = {"automountServiceAccountToken": False, "securityContext": {
    "runAsNonRoot": True, "runAsUser": 10001, "runAsGroup": 10001,
    "fsGroup": 10001, "seccompProfile": {"type": "RuntimeDefault"}}}


def resource(kind, name, **fields):
    return {"apiVersion": {"Deployment": "apps/v1", "Job": "batch/v1"}.get(kind, "v1"),
            "kind": kind, "metadata": {"name": name, "namespace": NS}, **fields}


def collector_config(broker=None):
    cfg = json.loads((HERE / "collector.json").read_text())
    if broker:
        kafka = {"brokers": [broker], "producer": {"required_acks": 1}}
        for signal in ("traces", "metrics", "logs"):
            kafka[signal] = {"topic": "cct." + signal, "encoding": "otlp_json"}
            cfg["service"]["pipelines"][signal]["exporters"].append("kafka")
        cfg["exporters"]["kafka"] = kafka
    return cfg


def component(name, ports, args, probe, memory, config=None, disk=None, env=None):
    app = "cct-" + name
    labels = {"app": app}
    config_text = json.dumps(config, indent=2) if config else ""
    container = {"name": name, "image": IMAGES[name], "args": args, "securityContext": SECURITY,
                 "ports": [{"name": k, "containerPort": v} for k, v in ports.items()],
                 "readinessProbe": {"httpGet": {"path": probe[1], "port": probe[0]}},
                 "resources": {"requests": {"cpu": "50m", "memory": "64Mi"},
                               "limits": {"cpu": "500m", "memory": memory}}}
    volumes, mounts, items = [], [], []
    if config:
        items.append(resource("ConfigMap", app, data={"config.yaml": config_text}))
        volumes.append({"name": "config", "configMap": {"name": app}})
        mounts.append({"name": "config", "mountPath": "/conf", "readOnly": True})
    if disk:
        volumes.append({"name": "data", "emptyDir": {"sizeLimit": disk}})
        mounts.append({"name": "data", "mountPath": "/data"})
    if env:
        container["env"] = [{"name": k, "value": v} for k, v in env.items()]
    container["volumeMounts"] = mounts
    meta = {"labels": labels, "annotations": {"cct.config-sha256": hashlib.sha256(config_text.encode()).hexdigest()}}
    items.append(resource("Deployment", app, spec={"replicas": 1, "strategy": {"type": "Recreate"},
        "selector": {"matchLabels": labels}, "template": {"metadata": meta, "spec": {
            **POD, "containers": [container], "volumes": volumes}}}))
    items.append(resource("Service", app, spec={"type": "ClusterIP", "selector": labels,
        "ports": [{"name": k, "port": v, "targetPort": k} for k, v in ports.items()]}))
    return items


def stack(broker=None):
    items = component("jaeger", {"grpc": 4317, "query": 16686, "admin": 14269}, [],
                      (14269, "/"), "256Mi", env={"SPAN_STORAGE_TYPE": "memory",
                      "MEMORY_MAX_TRACES": "1000", "COLLECTOR_OTLP_ENABLED": "true"})
    items.extend(component("collector", {"grpc": 4317, "http": 4318, "metrics": 8889, "self": 8888},
                           ["--config=/conf/config.yaml"], (13133, "/"), "256Mi", collector_config(broker), "32Mi"))
    prometheus = {"global": {"scrape_interval": "5s", "scrape_timeout": "4s"}, "scrape_configs": [
        {"job_name": job, "static_configs": [{"targets": [target]}]} for job, target in
        [("cct-service", "cct-collector:8889"), ("cct-collector", "cct-collector:8888")]]}
    items.extend(component("prometheus", {"web": 9090}, ["--config.file=/conf/config.yaml",
        "--storage.tsdb.path=/data", "--storage.tsdb.retention.time=2h", "--storage.tsdb.retention.size=200MB"],
        (9090, "/-/ready"), "256Mi", prometheus, "512Mi"))
    return items


def validate_name(name):
    import re
    if not re.fullmatch(r"cct-probe-[a-z0-9](?:[a-z0-9-]{0,34}[a-z0-9])?", name):
        raise ValueError("Use a unique cct-probe-<lowercase-run-id> (up to 36 suffix characters)")


def probe_job(name):
    validate_name(name)
    data = {p: (HERE / p).read_text() for p in ("probe.py", "lineage.py")}
    data["smoke_boutique.py"] = (HERE.parent / "scripts/smoke_boutique.py").read_text()
    return script_job(name, data, ["python","-B","/probe/probe.py","--run-id",name])


def script_job(name, data, command):
    cfg = resource("ConfigMap", name, data=data, immutable=True)
    container = {"name": "probe", "image": IMAGES["probe"], "securityContext": SECURITY,
        "command": command,
        "volumeMounts": [{"name": "script", "mountPath": "/probe", "readOnly": True}],
        "resources": {"requests": {"cpu": "10m", "memory": "32Mi"}, "limits": {"cpu": "200m", "memory": "96Mi"}}}
    job = resource("Job", name, spec={"backoffLimit": 0, "activeDeadlineSeconds": 300,
        "template": {"spec": {**POD, "restartPolicy": "Never", "containers": [container],
        "volumes": [{"name": "script", "configMap": {"name": name}}]}}})
    return [cfg, job]


def relay_job(name, directory):
    validate_name(name)
    summary = json.loads((directory/"summary.json").read_text())
    if summary["run_id"] != name:
        raise ValueError("relay run name must match the capture run_id")
    data = {p: (directory/p).read_text() for p in ("logs.json","metrics.json")}
    if sum(len(v.encode()) for v in data.values()) > 400000:
        raise ValueError("capture too large for a ConfigMap; use an approved OTLP endpoint")
    data["signals.py"] = (HERE/"signals.py").read_text()
    data["forward.py"] = """import json
from pathlib import Path
from signals import post
for signal in ("logs","metrics"):
    post("http://cct-collector:4318",signal,json.loads((Path("/probe")/(signal+".json")).read_text()))
    print(signal, "accepted by OTLP receiver", flush=True)
"""
    return script_job(name+"-relay", data, ["python","-B","/probe/forward.py"])


def pin_images(items):
    import sys
    sys.path.insert(0, str(HERE.parent/"scripts"))
    from prepare_boutique import registry_pin
    for item in items:
        if item["kind"] in ("Job","Deployment"):
            for container in item["spec"]["template"]["spec"]["containers"]:
                container["image"] = registry_pin(container["image"])
    return items


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    group = p.add_mutually_exclusive_group()
    group.add_argument("--kafka", metavar="BROKER:PORT", help="opt-in plaintext private broker agreed with Param")
    group.add_argument("--probe", metavar="RUN_NAME", help="render only a one-time in-cluster verification job")
    group.add_argument("--relay", type=Path, help="render a sender Job from a sanitized signals capture directory")
    p.add_argument("--run-name", help="original cct-probe run name (required for --relay)")
    p.add_argument("--pin-arm64", action="store_true", help="resolve image tags to native digests using Docker buildx")
    a = p.parse_args()
    if bool(a.relay) != bool(a.run_name):
        p.error("use --relay and --run-name together")
    items = relay_job(a.run_name,a.relay) if a.relay else (probe_job(a.probe) if a.probe else stack(a.kafka))
    if a.pin_arm64:
        pin_images(items)
    print(json.dumps({"apiVersion": "v1", "kind": "List",
                      "items": items}, indent=2))
