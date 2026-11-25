"""Deploy the namespace-scoped pilot and verify Jaeger and both Prometheus targets."""
import datetime as dt
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from urllib.parse import urlsplit
from urllib.request import urlopen


# Linux/arm64 manifests for the release tags in stack.IMAGES.
ARM64 = {
    "jaegertracing/all-in-one:1.76.0": "6ecdbc53defdcc67e5be4721a6e709c1056206ce63d99f2db75b772dddef0e93",
    "otel/opentelemetry-collector-contrib:0.128.0": "ce3d11caa20e1f106e02eead46c1c92a8d6c3bdbd9bc3f1bad4798de926978d6",
    "prom/prometheus:v3.5.0": "dfc3cce6bfa8485ccad377c66f4ef9833524fde710756faa725251b639d19d86",
}


def get_json(url):
    with urlopen(url, timeout=4) as response:
        return json.load(response)


def check_services():
    """Runs inside the cluster after the monitoring Deployments are Ready."""
    deadline = time.monotonic() + 75
    while True:
        try:
            services = get_json("http://cct-jaeger:16686/api/services")
            targets = get_json("http://cct-prometheus:9090/api/v1/targets?state=active")
            if not isinstance(services["data"], list) or targets["status"] != "success":
                raise ValueError("unexpected Jaeger or Prometheus response")
            health = {t["labels"]["job"]: t["health"] for t in targets["data"]["activeTargets"]}
            if all(health.get(job) == "up" for job in ("cct-collector", "cct-service")):
                print(json.dumps({"jaeger_query": "ready", "prometheus_targets": health}))
                return
            error = f"Prometheus targets: {health}"
        except (OSError, ValueError, KeyError) as exc:
            error = str(exc)
        if time.monotonic() >= deadline:
            raise RuntimeError(error)
        time.sleep(3)


def save_list(path, items):
    path.write_text(json.dumps({"apiVersion": "v1", "kind": "List", "items": items}, indent=2) + "\n")


def deploy():
    from .stack import NS, script_job, stack

    for tool in ("kubectl", "tailscale"):
        if not shutil.which(tool):
            raise RuntimeError(f"{tool} is required in this Terminal")
    kubeconfig = Path.home() / ".kube" / "cct-team.yaml"
    kubeconfig.parent.mkdir(exist_ok=True, parents=True)
    os.environ["KUBECONFIG"] = str(kubeconfig)
    subprocess.run(["tailscale", "configure", "kubeconfig", "cct-kubernetes"], check=True)

    def kubectl(*args, **kwargs):
        return subprocess.run(["kubectl", "--kubeconfig", str(kubeconfig), "-n", NS, *args],
                              text=True, check=True, **kwargs)

    server = kubectl("config", "view", "--minify", "-o", "jsonpath={.clusters[0].cluster.server}",
                     stdout=subprocess.PIPE).stdout
    host = urlsplit(server)
    if host.scheme != "https" or not re.fullmatch(r"cct-kubernetes(?:\.[a-z0-9-]+\.ts\.net)?", host.hostname or ""):
        raise RuntimeError("The project kubeconfig did not select the cct-kubernetes HTTPS endpoint")
    kubectl("auth", "whoami", "--request-timeout=20s")
    timestamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = Path("outputs/telemetry")
    out.mkdir(parents=True, exist_ok=True)
    receipt = Path(tempfile.mkdtemp(prefix=f"rollout-{timestamp}-", dir=out))
    print(f"Rollout evidence: {receipt.resolve()}", flush=True)
    items = stack()
    for obj in items:
        if obj["kind"] == "Deployment":
            for container in obj["spec"]["template"]["spec"]["containers"]:
                image = container["image"]
                container["image"] = f"{image}@sha256:{ARM64[image]}"
    manifest = receipt / "stack.json"
    save_list(manifest, items)
    apply = ["apply", "--server-side", "--field-manager=cct-monitoring", "-f", str(manifest)]
    kubectl(*apply, "--dry-run=server")
    kubectl(*apply)
    for app in ("cct-jaeger", "cct-collector", "cct-prometheus"):
        try:
            kubectl("rollout", "status", f"deployment/{app}", "--timeout=240s")
        except subprocess.CalledProcessError:
            kubectl("get", "pods", "-l", f"app={app}", "-o", "wide")
            raise
    columns = "custom-columns=POD:.metadata.name,READY:.status.containerStatuses[0].ready,IMAGE_ID:.status.containerStatuses[0].imageID"
    pods = kubectl("get", "pods", "-l", "app in (cct-jaeger,cct-collector,cct-prometheus)",
                   "-o", columns, stdout=subprocess.PIPE).stdout
    (receipt / "pods.txt").write_text(pods)

    # The maintainer can create a Job in this namespace without port-forward permissions.
    name = "cct-check-" + receipt.name.removeprefix("rollout-").lower().replace("_", "-").rstrip("-")
    resources = script_job(name, {"rollout.py": Path(__file__).read_text()},
                           ["python", "-B", "/probe/rollout.py", "--check-services"])
    check_manifest = receipt / "check.json"
    save_list(check_manifest, resources)
    try:
        kubectl("create", "-f", str(check_manifest))
        try:
            kubectl("wait", "--for=condition=complete", "job/" + name, "--timeout=180s")
        finally:
            log = kubectl("logs", "job/" + name, "--pod-running-timeout=10s",
                          stdout=subprocess.PIPE).stdout
            (receipt / "services.log").write_text(log)
            print(log, end="", flush=True)
    finally:
        # Only the disposable check resources are removed. All three services keep running.
        subprocess.run(["kubectl", "--kubeconfig", str(kubeconfig), "-n", NS, "delete",
                        "--ignore-not-found", "--wait=false", "-f", str(check_manifest)], check=False)
    result = json.loads(log)
    if result.get("jaeger_query") != "ready" or not all(result.get("prometheus_targets", {}).get(j) == "up"
                                                         for j in ("cct-service", "cct-collector")):
        raise RuntimeError("Missing service verification in the check Job receipt")
    result.update(collector_grpc="cct-collector.cct-observability.svc:4317",
                  verified_at=dt.datetime.now(dt.timezone.utc).isoformat())
    (receipt / "ready.json").write_text(json.dumps(result, indent=2) + "\n")
    print("MONITORING READY — Collector endpoint: cct-collector.cct-observability.svc:4317", flush=True)
    print(f"Share {receipt / 'ready.json'} with the deployment administrator for Boutique tracing.")


if __name__ == "__main__":
    try:
        if sys.argv[1:] == ["--check-services"]:
            check_services()
        elif not sys.argv[1:]:
            deploy()
        else:
            raise ValueError("Use python3 -m observability.rollout from the repository root")
    except (OSError, subprocess.CalledProcessError, KeyError, ValueError, RuntimeError) as exc:
        sys.exit(f"Monitoring verification did not finish: {exc}")
