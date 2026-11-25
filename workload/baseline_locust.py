"""Time and record the unmodified workload in the pinned Boutique image.

Run with one local Locust process. All users finish spawning before a 120s
warm-up, followed by a fresh 900s measured window. This is a closed-user load.
"""

import importlib.util
import json
import os
from pathlib import Path
import random
import time

import gevent
from locust import events

spec = importlib.util.spec_from_file_location("boutique_upstream", "/loadgen/locustfile.py")
upstream = importlib.util.module_from_spec(spec)
spec.loader.exec_module(upstream)
WebsiteUser = upstream.WebsiteUser

SEED = int(os.environ.get("CCT_SEED", "42"))
WARMUP = int(os.environ.get("CCT_WARMUP_SECONDS", "120"))
MEASURE = int(os.environ.get("CCT_MEASURE_SECONDS", "900"))
RESULTS = Path("/results")
random.seed(SEED)
upstream.fake.seed_instance(SEED)
state = {"phase": "spawning", "seed": SEED, "started": False}
environment = None
request_file = (RESULTS / "request-events.jsonl").open("w", buffering=1)


def save(name, data):
    temporary = RESULTS / (name + ".tmp")
    temporary.write_text(json.dumps(data, indent=2) + "\n")
    temporary.replace(RESULTS / name)


def totals():
    total = environment.stats.total
    return {"requests": total.num_requests, "failures": total.num_failures,
            "median_ms": total.median_response_time,
            "p95_ms": total.get_response_time_percentile(0.95),
            "average_ms": total.avg_response_time,
            "endpoints": [{"method": s.method, "name": s.name,
                           "requests": s.num_requests, "failures": s.num_failures,
                           "median_ms": s.median_response_time,
                           "p95_ms": s.get_response_time_percentile(0.95)}
                          for s in environment.stats.entries.values()]}


def progress():
    while state["phase"] != "complete":
        save("progress.json", {**state, "at_epoch": time.time(),
                               "users": environment.runner.user_count, **totals()})
        gevent.sleep(5)


def finish():
    elapsed = time.monotonic() - state["measurement_started_monotonic"]
    while elapsed < MEASURE:
        gevent.sleep(max(0.01, MEASURE - elapsed))
        elapsed = time.monotonic() - state["measurement_started_monotonic"]
    report = {**state, "phase": "complete", "measurement_ended_epoch": time.time(),
              "measured_seconds": elapsed, "users_at_end": environment.runner.user_count,
              "locust_errors": len(environment.runner.exceptions), **totals()}
    report["requests_per_second"] = report["requests"] / elapsed
    state["phase"] = "complete"
    save("baseline.json", report)
    save("progress.json", report)
    print("CCT measured baseline finished", flush=True)
    environment.runner.quit()


def start_measurement():
    save("warmup.json", {**state, "warmup_ended_epoch": time.time(), **totals()})
    environment.stats.reset_all()
    state.update(phase="baseline", measurement_started_epoch=time.time(),
                 measurement_started_monotonic=time.monotonic())
    save("baseline-start.json", state)
    print("CCT measured baseline started: {} seconds".format(MEASURE), flush=True)
    gevent.spawn_later(MEASURE, finish)


@events.init.add_listener
def init(environment, **kwargs):
    globals()["environment"] = environment


@events.spawning_complete.add_listener
def spawned(user_count, **kwargs):
    if state["started"]:
        return
    if user_count != 5:
        raise ValueError("The baseline protocol requires exactly five users")
    state.update(started=True, phase="warmup", users=user_count,
                 warmup_started_epoch=time.time())
    gevent.spawn(progress)
    gevent.spawn_later(WARMUP, start_measurement)
    print("CCT warm-up started: {} seconds".format(WARMUP), flush=True)


@events.request.add_listener
def record(request_type, name, response_time, response_length, exception, **kwargs):
    request_file.write(json.dumps({"completed_at_epoch": time.time(), "phase": state["phase"],
                                   "method": request_type, "name": name,
                                   "latency_ms": response_time, "bytes": response_length,
                                   "success": exception is None}) + "\n")
