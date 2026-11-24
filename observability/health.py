"""Candidate CCT-10 rule for one service at one prediction cutoff (times in UTC ms)."""
from math import isfinite


def state(rows, run_id, service, cutoff_ms, *, latency_limit_ms, error_limit=0.05,
          min_requests=20, width_ms=5000):
    """Use the latest available revision of each closed, aligned service window.

    A restart delta or lost readiness is independently sufficient. Otherwise require
    two consecutive adequately sampled windows. The caller fits limits on training runs.
    """
    if not (isfinite(latency_limit_ms) and latency_limit_ms > 0 and 0 < error_limit <= 1
            and min_requests > 0 and width_ms > 0):
        raise ValueError("invalid label parameters")
    end = (cutoff_ms // width_ms) * width_ms
    eligible = {}
    for r in rows:
        if (r["run_id"],r["service"]) != (run_id,service):
            continue
        at = r["end_ms"]
        if at not in (end-width_ms,end) or r["available_ms"] > cutoff_ms:
            continue
        if r["start_ms"] != at-width_ms or r["available_ms"] < at:
            raise ValueError("invalid closed-window timestamps")
        prior = eligible.get(at)
        if prior and prior["available_ms"] == r["available_ms"] and prior != r:
            raise ValueError("conflicting window revision")
        if prior is None or prior["available_ms"] < r["available_ms"]:
            eligible[at] = r

    def classify(r):
        if not r:
            return "unknown"
        if r.get("ready") is False or (r.get("restart_delta") or 0) > 0:
            return "hard-failure"
        n, errors, p95 = r.get("requests"), r.get("errors"), r.get("p95_ms")
        if (r.get("complete") is not True or r.get("ready") is not True or r.get("restart_delta") != 0
                or n is None or n < min_requests or errors is None or not 0 <= errors <= n
                or p95 is None or not isfinite(p95) or p95 < 0):
            return "unknown"
        return "breach" if p95 > latency_limit_ms or errors/n >= error_limit else "healthy"

    latest = classify(eligible.get(end))
    if latest == "hard-failure":
        return "affected"
    if latest == "breach":
        previous = classify(eligible.get(end-width_ms))
        return "affected" if previous in ("breach","hard-failure") else "unknown"
    return latest
