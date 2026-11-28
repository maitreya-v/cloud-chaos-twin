"""Five upstream Boutique users; trace IDs and bounded host-controlled phases.

Uses the locked loadgenerator image's original task weights and 1–10s waits.
Never logs request bodies, cookies, payment fields or response bodies.
"""
import importlib.util
import json
import os
from pathlib import Path
import random
import secrets
import time

import gevent
from locust import events

spec = importlib.util.spec_from_file_location("boutique_upstream", "/loadgen/locustfile.py")
upstream = importlib.util.module_from_spec(spec)
spec.loader.exec_module(upstream)
WebsiteUser = upstream.WebsiteUser
WebsiteUser.network_timeout = 10
WebsiteUser.connection_timeout = 5
random.seed(42)
upstream.fake.seed_instance(42)
RUN = os.environ["CCT_RUN_ID"]
RESULTS = Path("/results")
state = {"phase": "spawning", "run_id": RUN}
environment = None
original_init = WebsiteUser.__init__


def emit(kind, row):
    print("CCT_" + kind + " " + json.dumps(row, separators=(",", ":")), flush=True)


def traced_init(self, *args, **kwargs):
    original_init(self, *args, **kwargs)
    original = self.client.request

    def request(method, url, *pos, **kw):
        trace, parent = secrets.token_hex(16), secrets.token_hex(8)
        kw["headers"] = {**kw.get("headers", {}), "traceparent": f"00-{trace}-{parent}-01"}
        kw["context"] = {**kw.get("context", {}), "trace_id": trace, "external_parent": parent}
        if kw.get("catch_response"):
            raise ValueError("Pilot wrapper expects the upstream non-context-manager tasks")
        kw["catch_response"] = True
        with original(method, url, *pos, **kw) as response:
            if url == "/cart/checkout" and b"Your order is complete!" not in (response.content or b""):
                response.failure("checkout confirmation missing")
        return response

    self.client.request = request


WebsiteUser.__init__ = traced_init


def phase(name, **more):
    state.update(phase=name, **more)
    emit("PHASE", {**state, "observed_epoch": time.time()})


def totals():
    total = environment.stats.total
    return {"requests": total.num_requests, "failures": total.num_failures,
            "users": environment.runner.user_count,
            "locust_errors": len(environment.runner.exceptions)}


def control():
    started = time.monotonic()
    while True:
        if (RESULTS / "stop").exists() or (RESULTS / "finish").exists():
            reason = "aborted" if (RESULTS / "stop").exists() else "completed"
            break
        if time.monotonic() - started > 490:
            reason = "coordinator deadline exceeded"
            break
        gevent.sleep(0.2)
    report = {**state, **totals(), "reason": reason, "ended_epoch": time.time()}
    (RESULTS / "report.json").write_text(json.dumps(report) + "\n")
    phase("complete", **{k: v for k, v in report.items() if k != "phase"})
    environment.runner.quit()


def measured():
    emit("WARMUP", {**totals(), "ended_epoch": time.time()})
    environment.stats.reset_all()
    phase("measured", measured_started_epoch=time.time())


@events.init.add_listener
def init(environment, **kwargs):
    globals()["environment"] = environment


@events.spawning_complete.add_listener
def spawned(user_count, **kwargs):
    if state["phase"] != "spawning":
        return
    if user_count != 5:
        raise ValueError("Pilot requires exactly five users")
    phase("warmup", warmup_started_epoch=time.time())
    gevent.spawn_later(120, measured)
    gevent.spawn(control)


@events.request.add_listener
def record(request_type, name, response_time, response_length, exception,
           context, response, start_time, **kwargs):
    emit("EVENT", {"run_id": RUN, "phase": state["phase"], "started_epoch": start_time,
                   "completed_epoch": time.time(), "method": request_type, "path": name,
                   "latency_ms": response_time, "bytes": response_length,
                   "status": response.status_code if response is not None else 0,
                   "success": exception is None,
                   "error_type": type(exception).__name__ if exception else None,
                   **context})
