"""Read pod readiness/restarts at one-second intervals for a scheduled run. Python 3.9+."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import time


def redact(items):
    services = {}
    for pod in items:
        meta, status = pod["metadata"], pod.get("status", {})
        service = meta.get("labels", {}).get("app")
        if not service:
            continue
        expected = {c["name"] for c in pod.get("spec", {}).get("containers", [])}
        containers = {c["name"]: c.get("restartCount") for c in status.get("containerStatuses", [])}
        known = bool(expected) and set(containers) == expected and all(type(x) is int and x >= 0 for x in containers.values())
        cond = next((c.get("status") for c in status.get("conditions", []) if c.get("type") == "Ready"), None)
        ready = {"True": True, "False": False}.get(cond)
        key = hashlib.sha256(meta["uid"].encode()).hexdigest()
        services.setdefault(service, {})[key] = dict(ready=ready, restarts=containers if known else None,
                                                    deleting=bool(meta.get("deletionTimestamp")))
    return services


def capture(args):
    kubeconfig = args.kubeconfig.expanduser()
    if not kubeconfig.is_file():
        raise ValueError("The specified project kubeconfig is missing")
    cmd = ["kubectl", "--kubeconfig", str(kubeconfig), "--request-timeout=3s", "get", "--raw",
           "/api/v1/namespaces/cct-app/pods?labelSelector=app"]
    deadline = time.monotonic() + args.seconds
    success = 0
    with args.output.open("x", encoding="utf-8") as dst:
        while time.monotonic() < deadline:
            tick = time.monotonic()
            row = dict(run_id=args.run_id, started_ms=time.time_ns()//1000000)
            try:
                data = json.loads(subprocess.check_output(cmd, stderr=subprocess.DEVNULL, timeout=5))
                if data.get("metadata", {}).get("continue"):
                    raise ValueError("incomplete list")
                row["services"] = redact(data["items"])
                success += 1
            except (subprocess.SubprocessError, OSError, ValueError, KeyError) as e:
                row["services"] = None
                row["error"] = type(e).__name__
            row["seen_ms"] = (time.time_ns()+999999)//1000000
            dst.write(json.dumps(row, sort_keys=True)+"\n")
            dst.flush()
            time.sleep(max(0, min(deadline-time.monotonic(), 1-(time.monotonic()-tick))))
    if not success:
        raise SystemExit("No successful pod reads. No readiness coverage was recorded.")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--run-id", required=True)
    p.add_argument("--seconds", type=int, required=True)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--kubeconfig", type=Path, default=Path.home()/".kube/cct-team.yaml")
    args = p.parse_args()
    if args.seconds < 1 or not args.run_id.strip():
        p.error("use a positive duration and nonempty run ID")
    capture(args)
