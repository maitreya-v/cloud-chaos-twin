"""Join Param's service windows to fresh Kubernetes observations and reviewed coverage evidence."""
import argparse
from bisect import bisect_left
import json
import math
from pathlib import Path

WIDTH = 5000
MAX_GAP_MS = 2000


def table(path):
    if path.is_dir() or path.suffix == ".parquet":
        import pyarrow.parquet as pq
        return pq.read_table(path).to_pylist()
    if path.suffix == ".jsonl":
        return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    data = json.loads(path.read_text())
    if not isinstance(data, list):
        raise ValueError("Input must be a JSON array, JSONL or Parquet table")
    return data


def window_key(row):
    start, end = row["start_ms"], row["end_ms"]
    if type(start) is not int or type(end) is not int or start < 0 or start % WIDTH or end-start != WIDTH:
        raise ValueError("Expected an aligned five-second window")
    if not all(isinstance(row[k],str) and row[k].strip() for k in ("run_id","service")):
        raise ValueError("Missing run/service")
    return row["run_id"], row["service"], start, end


def pod_evidence(samples, times, service, start, end):
    # Entire read intervals for the two outer samples lie before/after the window.
    i = bisect_left(times,start)-1
    j = bisect_left(times,end)
    while j < len(samples) and samples[j]["started_ms"] < end:
        j += 1
    if i < 0 or j == len(samples):
        return None, None, None
    points = samples[i:j+1]
    if (start-points[0]["started_ms"] > MAX_GAP_MS or points[-1]["seen_ms"]-end > MAX_GAP_MS
        or any(b["seen_ms"]-a["started_ms"] > MAX_GAP_MS for a,b in zip(points,points[1:]))):
        return None, None, None
    states = [(s.get("services") or {}).get(service) for s in points]
    if not states[0] or any(not st or st.keys()!=states[0].keys() or any(p["deleting"] for p in st.values()) for st in states):
        return None, None, None
    inside = [i for i,s in enumerate(points) if start <= s["started_ms"] and s["seen_ms"] < end]
    if not inside or end-points[inside[-1]]["started_ms"] > MAX_GAP_MS:
        return None, None, None
    ready_vals = [p["ready"] for p in states[inside[-1]].values()]
    ready = False if False in ready_vals else (True if all(v is True for v in ready_vals) else None)
    counters = []
    for st in states:
        if any(p["restarts"] is None for p in st.values()):
            return ready, None, points[-1]["seen_ms"]
        counters.append({(uid, name): value for uid,p in st.items() for name,value in p["restarts"].items()})
    delta = None
    if counters[0] and all(c.keys()==counters[0].keys() for c in counters):
        if all(b[k]>=a[k] for a,b in zip(counters,counters[1:]) for k in a):
            outer = sum(counters[-1][k]-counters[0][k] for k in counters[0])
            inner = sum(counters[inside[-1]][k]-counters[inside[0]][k] for k in counters[0])
            if outer == inner:
                delta = inner
    return ready, delta, points[-1]["seen_ms"]


def join(rows, samples, proofs, run):
    selected = [s for s in samples if s["run_id"]==run]
    times = [s["seen_ms"] for s in selected]
    if any(type(s["started_ms"]) is not int or type(s["seen_ms"]) is not int
           or not 0 <= s["started_ms"] <= s["seen_ms"] for s in selected):
        raise ValueError("Invalid sample times")
    if any(b["started_ms"] < a["seen_ms"] or b["seen_ms"] <= a["seen_ms"] for a,b in zip(selected,selected[1:])):
        raise ValueError("Overlapping or out-of-order pod reads")
    verified = {}
    for p in proofs:
        key = window_key(p)
        if key[0]!=run:
            continue
        if key in verified:
            raise ValueError("Duplicate coverage proof")
        verified[key]=p
    result, keys = [], set()
    for src in rows:
        if src["run_id"] != run:
            continue
        if src.get("synthetic") is True:
            raise ValueError("Expected a recorded run")
        key=window_key(src)
        if key in keys:
            raise ValueError("Duplicate window. Supply a single replay revision per window.")
        keys.add(key)
        _, service, start, end = key
        at=src["available_ms"]
        if type(at) is not int or at < end:
            raise ValueError("The source window needs a valid availability timestamp")
        n, err, latency = (src[k] for k in ("requests","errors","p95_ms"))
        if any(x is not None and (type(x) is not int or x<0) for x in (n,err)):
            raise ValueError("Invalid counts")
        if n is not None and err is not None and err>n:
            raise ValueError("Error count exceeds requests")
        if latency is not None and (type(latency) not in (float,int) or not math.isfinite(latency) or latency<0):
            raise ValueError("Invalid p95")
        ready, delta, pod_at = pod_evidence(selected,times,service,start,end)
        row = dict(run_id=run,service=service,start_ms=start,end_ms=end,available_ms=max(at,pod_at or at),
                   requests=n,errors=err,p95_ms=latency,ready=ready,restart_delta=delta,complete=False)
        p=verified.get(key)
        if p and p.get("verified") is True:
            if not all(isinstance(p.get(k),str) and p[k].strip() for k in ("independent_source","evidence_ref","reviewer")):
                raise ValueError("Verified coverage requires an independent source, evidence and reviewer")
            if type(p.get("available_ms")) is not int or p["available_ms"]<end:
                raise ValueError("Verified coverage needs an actual availability time")
            comparable=(n is not None and err is not None and (n==0 or latency is not None))
            if any(type(p.get(k)) is not int or p[k]<0 for k in ("requests","errors","duration_count")):
                raise ValueError("Invalid independent denominator or duration count")
            matches=(comparable and p["requests"]==n and p["errors"]==err and p["duration_count"]==n)
            if matches and p.get("records_reconciled") is True and ready is not None and delta is not None:
                row.update(complete=True, available_ms=max(row["available_ms"],p["available_ms"]))
        result.append(row)
    if not result:
        raise ValueError("No input service windows match this run ID")
    return sorted(result,key=lambda r:(r["start_ms"],r["service"]))


if __name__ == "__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--windows",type=Path,required=True)
    parser.add_argument("--pods",type=Path,required=True)
    parser.add_argument("--coverage",type=Path,help="Optional reviewed per-window evidence")
    parser.add_argument("--run-id",required=True)
    parser.add_argument("--output",type=Path,required=True)
    args=parser.parse_args()
    rows=join(table(args.windows),table(args.pods),table(args.coverage) if args.coverage else [],args.run_id)
    with args.output.open("x",encoding="utf-8") as fh:
        json.dump(rows,fh,indent=2,sort_keys=True,allow_nan=False)
        fh.write("\n")
    print(f"{len(rows)} windows, {sum(r['complete'] for r in rows)} complete")
