"""Compare accepted, matched baseline_boutique summaries. Ratio alone does not establish causality."""
import argparse
import json
from pathlib import Path


def compare(off, on):
    if not all(r["passed"] is True for r in (off,on)):
        raise ValueError("both archived runs must pass their baseline gate")
    for key in ("users","seed","warmup_seconds","target_seconds","measured_workload_sha256"):
        if off.get(key) is None or off[key] != on.get(key):
            raise ValueError("runs differ or lack provenance for "+key)
    if off["started_utc"] == on["started_utc"]:
        raise ValueError("comparison requires two distinct runs")
    if off.get("upstream_workload_sha256") != on.get("upstream_workload_sha256"):
        raise ValueError("upstream workload differs or has incomplete provenance")
    result = {}
    for key in ("p95_ms","requests_per_second","mean_vm_cpu_cores","peak_vm_working_set_bytes"):
        a,b = off[key],on[key]
        result[key] = {"off":a,"on":b,"percent_change":100*(b-a)/a if a>0 else None}
    return result


def load(path):
    def read(p):
        return json.loads(p.read_text())
    if path.is_dir():
        config = read(path / "run-config.json")
        gate = read(path / "summary.json")
        measurement = read(path / "locust/baseline.json")
        return {"passed":gate["passed"], "users":config["users"], "seed":config["seed"],
                "warmup_seconds":config["warmup_seconds"], "target_seconds":config["measured_seconds"],
                "measured_workload_sha256":config["workload_sha256"],
                "upstream_workload_sha256":config["upstream_workload_sha256"],
                "started_utc":measurement["measurement_started_epoch"],
                "p95_ms":measurement["p95_ms"], "requests_per_second":measurement["requests_per_second"],
                "mean_vm_cpu_cores":gate["mean_node_cpu_cores"],
                "peak_vm_working_set_bytes":gate["peak_node_working_set_bytes"]}
    value = read(path)
    b = value.get("baseline",value).copy()
    if "passed_with_documented_timer_tolerance" in b:
        b["passed"] = b["passed_with_documented_timer_tolerance"]
    return b


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("off",type=Path)
    p.add_argument("on",type=Path)
    a = p.parse_args()
    print(json.dumps(compare(load(a.off),load(a.on)),indent=2))
