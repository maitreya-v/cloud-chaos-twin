#!/usr/bin/env python3
"""Verify a 10-second delay on disposable canaries; never target Boutique pods."""
import argparse
import datetime as dt
import json
from pathlib import Path
import statistics
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
KUBE = ["kubectl", "--kubeconfig", str(ROOT / ".local/kubeconfig"),
        "--context", "kind-cloud-chaos-twin", "-n", "cct-app", "--request-timeout=60s"]
NAMES = ["networkchaos/cct-runtime-check", "deployment/cct-chaos-canary",
         "service/cct-chaos-canary", "pod/cct-chaos-client"]


def run(args, **kwargs):
    result = subprocess.run(KUBE + args, text=True, capture_output=True, timeout=75, **kwargs)
    if result.returncode:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip())
    return result.stdout


def resources(image):
    security = {"runAsNonRoot": True, "runAsUser": 10001, "runAsGroup": 10001,
                "seccompProfile": {"type": "RuntimeDefault"}}
    container_security = {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True,
                          "capabilities": {"drop": ["ALL"]}}
    server = """from http.server import BaseHTTPRequestHandler,HTTPServer
class Handler(BaseHTTPRequestHandler):
 def do_GET(self):
  body=b'cct-canary\\n'; self.send_response(200); self.send_header('Content-Length',str(len(body))); self.end_headers(); self.wfile.write(body)
 def log_message(self,*args): pass
HTTPServer(('0.0.0.0',8080),Handler).serve_forever()
"""
    def pod_spec(command):
        return {"automountServiceAccountToken": False, "terminationGracePeriodSeconds": 5,
                "securityContext": security, "containers": [{"name": "canary", "image": image,
                "imagePullPolicy": "IfNotPresent", "command": ["python", "-B", "-u", "-c", command],
                "securityContext": container_security,
                "resources": {"requests": {"cpu": "10m", "memory": "32Mi"},
                              "limits": {"cpu": "200m", "memory": "64Mi"}}}]}
    meta = {"name": "cct-chaos-canary", "namespace": "cct-app"}
    labels = {"app": "cct-chaos-canary", "cct.runtime-check": "true"}
    spec = pod_spec(server)
    spec["containers"][0]["readinessProbe"] = {
        "httpGet": {"path": "/", "port": 8080}, "periodSeconds": 2, "timeoutSeconds": 1}
    deployment = {"apiVersion": "apps/v1", "kind": "Deployment", "metadata": meta,
                  "spec": {"replicas": 1, "selector": {"matchLabels": labels},
                           "template": {"metadata": {"labels": labels}, "spec": spec}}}
    service = {"apiVersion": "v1", "kind": "Service", "metadata": meta,
               "spec": {"type": "ClusterIP", "selector": labels,
                        "ports": [{"port": 8080, "targetPort": 8080}]}}
    client_spec = pod_spec("import time; time.sleep(300)")
    client_spec.update(restartPolicy="Never", activeDeadlineSeconds=300)
    client = {"apiVersion": "v1", "kind": "Pod",
              "metadata": {"name": "cct-chaos-client", "namespace": "cct-app",
                           "labels": {"app": "cct-chaos-client", "cct.runtime-check": "true"}},
              "spec": client_spec}
    return {"apiVersion": "v1", "kind": "List", "items": [deployment, service, client]}


def sample():
    program = """import json,time,urllib.request
samples=[]
for _ in range(5):
 start=time.perf_counter()
 with urllib.request.urlopen('http://cct-chaos-canary.cct-app.svc:8080/',timeout=4) as r:
  assert r.status==200 and r.read()==b'cct-canary\\n'
 samples.append((time.perf_counter()-start)*1000)
print(json.dumps(samples))
"""
    return json.loads(run(["exec", "pod/cct-chaos-client", "--", "python", "-B", "-c", program]))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True, help="Immutable native Python probe image")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if "@sha256:" not in args.image:
        parser.error("Use a digest-pinned image")
    args.output.mkdir(parents=True, exist_ok=False)
    receipt = {"started_at": dt.datetime.now(dt.timezone.utc).isoformat(),
               "target": "isolated cct-chaos-canary", "passed": False}
    created = False
    try:
        for name in NAMES:
            existing = json.loads(run(["get", name, "--ignore-not-found", "-o", "json"]) or "{}")
            if existing:
                raise RuntimeError("Canary name already exists: " + name)
        manifest = args.output / "canaries.json"
        manifest.write_text(json.dumps(resources(args.image), indent=2) + "\n")
        fault = {"apiVersion": "chaos-mesh.org/v1alpha1", "kind": "NetworkChaos",
                 "metadata": {"name": "cct-runtime-check", "namespace": "cct-app"},
                 "spec": {"action": "delay", "mode": "one",
                          "selector": {"namespaces": ["cct-app"],
                                       "labelSelectors": {"app": "cct-chaos-canary"}},
                          "direction": "to", "delay": {"latency": "100ms", "jitter": "0ms",
                                                      "correlation": "0"}, "duration": "10s"}}
        fault_path = args.output / "fault.json"
        fault_path.write_text(json.dumps(fault, indent=2) + "\n")
        run(["create", "--dry-run=server", "-f", str(fault_path)])
        created = True
        run(["create", "-f", str(manifest)])
        run(["rollout", "status", "deployment/cct-chaos-canary", "--timeout=55s"])
        run(["wait", "--for=condition=Ready", "pods", "-l", "cct.runtime-check=true", "--timeout=55s"])
        receipt["baseline_ms"] = sample()
        receipt["fault_requested_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
        run(["create", "-f", str(fault_path)])
        run(["wait", "--for=condition=AllInjected", "networkchaos/cct-runtime-check", "--timeout=35s"])
        receipt["injected_observed_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
        receipt["during_ms"] = sample()
        receipt["injected_status"] = json.loads(run(["get", "networkchaos/cct-runtime-check", "-o", "json"]))["status"]
        run(["wait", "--for=condition=AllRecovered", "networkchaos/cct-runtime-check", "--timeout=35s"])
        receipt["recovered_observed_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
        receipt["recovered_ms"] = sample()
        receipt["recovered_status"] = json.loads(run(["get", "networkchaos/cct-runtime-check", "-o", "json"]))["status"]
        medians = {key: statistics.median(receipt[key + "_ms"])
                   for key in ("baseline", "during", "recovered")}
        receipt["median_ms"] = medians
        receipt["passed"] = (medians["during"] >= medians["baseline"] + 75
                             and medians["recovered"] < medians["baseline"] + 50)
    except (RuntimeError, subprocess.SubprocessError, OSError, ValueError) as error:
        receipt["error"] = str(error)
    finally:
        if created:
            cleanup_errors = []
            for name in NAMES:
                try:
                    run(["delete", name, "--ignore-not-found", "--wait=true", "--timeout=20s"])
                except (RuntimeError, subprocess.SubprocessError) as error:
                    cleanup_errors.append(str(error))
            try:
                run(["wait", "--for=delete", "pods", "-l", "cct.runtime-check=true", "--timeout=20s"])
            except (RuntimeError, subprocess.SubprocessError) as error:
                cleanup_errors.append(str(error))
            receipt["cleanup_errors"] = cleanup_errors
            receipt["passed"] = receipt["passed"] and not cleanup_errors
        receipt["finished_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
        (args.output / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps({"passed": receipt["passed"], "median_ms": receipt.get("median_ms"),
                      "error": receipt.get("error"), "receipt": str(args.output / "receipt.json")}))
    return 0 if receipt["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
