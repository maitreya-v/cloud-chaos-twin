"""Exercise a synthetic cart on the real Boutique and collect a bounded evidence record."""
import argparse
import datetime as dt
import http.cookiejar
import json
import secrets
import sys
import time
from pathlib import Path
from urllib import error, parse, request

from lineage import coverage, inspect_trace

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from smoke_boutique import PRODUCT, checkout_fields


def utc():
    return dt.datetime.now(dt.timezone.utc).isoformat()


class NoRedirect(request.HTTPRedirectHandler):
    def redirect_request(self, *args):
        return None


def fetch_json(url):
    with request.urlopen(url, timeout=5) as reply:
        return json.load(reply)


def query(prom, expr):
    obj = fetch_json(prom.rstrip("/") + "/api/v1/query?" + parse.urlencode({"query": expr}))
    if obj["status"] != "success":
        raise ValueError("Prometheus query failed")
    return obj["data"]["result"]


def counters(prom):
    rows = query(prom, "sum by (service_name) (cct_calls_total)")
    return {r["metric"]["service_name"]: {"value": float(r["value"][1]), "evaluation_time_s": float(r["value"][0])}
            for r in rows if "service_name" in r["metric"]}


def collector_status(prom):
    # Preserve the individual series and units. An absent counter stays absent.
    return query(prom, '{job="cct-collector",__name__=~"up|otelcol_.*(refused|failed|dropped|enqueue_failed).*"}')


def run(args, result):
    opener = request.build_opener(request.HTTPCookieProcessor(http.cookiejar.CookieJar()), NoRedirect())
    result["collector_before"] = collector_status(args.prometheus)
    result["before"] = counters(args.prometheus)
    def shop(label, path, form=None, status=200, marker=None):
        tid, parent = secrets.token_hex(16), secrets.token_hex(8)
        event = {"journey": label, "trace_id": tid, "external_parent": parent, "started_at": utc()}
        result["requests"].append(event)
        body = parse.urlencode(form).encode() if form is not None else None
        req = request.Request(args.shop.rstrip("/") + path, data=body,
                              headers={"traceparent": f"00-{tid}-{parent}-01"})
        try:
            reply = opener.open(req, timeout=12)
        except error.HTTPError as exc:
            reply = exc
        with reply:
            event["http_status"] = reply.status
            page = reply.read().decode()
            if reply.status != status or (marker and marker not in page):
                raise ValueError("unexpected Boutique response for " + label)
            if status == 302 and parse.urlparse(reply.headers.get("Location", "")).path != "/cart":
                raise ValueError("unexpected cart redirect")
        event["finished_at"] = utc()
        return event

    selected = [shop("browse", "/", marker="Online Boutique"),
                shop("cart", "/cart", {"product_id": PRODUCT, "quantity": 1}, status=302)]
    # Confirm the write with an independently traced HTTP request in the same new cookie jar.
    shop("cart-content", "/cart", marker=PRODUCT)
    selected.append(shop("checkout", "/cart/checkout", checkout_fields(), marker="Your order is complete!"))
    shop("empty-after-checkout", "/cart", marker="Your shopping cart is empty!")
    deadline = time.monotonic() + args.wait
    while True:
        for e in selected:
            if e.get("coverage", {}).get("passes_declared_path"):
                continue
            try:
                data = fetch_json(args.jaeger.rstrip("/") + "/api/traces/" + e["trace_id"]).get("data") or []
                if data:
                    e["lineage"] = inspect_trace(data[0], e["trace_id"], e["external_parent"], include_records=True)
                    e["coverage"] = coverage(e["journey"], e["lineage"])
                    e["observed_at"] = utc()
            except error.HTTPError as exc:
                if exc.code != 404:
                    raise
        result["after"] = counters(args.prometheus)
        deltas = {}
        for service,v in result["after"].items():
            old = result["before"].get(service)
            # A counter first seen after the run needs a prior sample for a measured delta.
            deltas[service] = v["value"] - old["value"] if old and v["value"] >= old["value"] else None
        result["call_deltas"] = deltas
        result["metric_activity"] = all((deltas.get(s) or 0) > 0 for s in
                                         ("frontend","productcatalogservice","checkoutservice"))
        result["trace_gate"] = all(e.get("coverage", {}).get("passes_declared_path") for e in selected)
        if (result["metric_activity"] and result["trace_gate"]) or time.monotonic() >= deadline:
            break
        time.sleep(2)
    result["collector_after"] = collector_status(args.prometheus)
    result["passed"] = result["trace_gate"] and result["metric_activity"]


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--shop", default="http://frontend.cct-app")
    p.add_argument("--prometheus", default="http://cct-prometheus:9090")
    p.add_argument("--jaeger", default="http://cct-jaeger:16686")
    p.add_argument("--wait", type=int, default=45)
    p.add_argument("--run-id", required=True)
    args = p.parse_args()
    result = {"schema_version": "1.0", "run_id": args.run_id, "source": "shopping-probe",
              "started_at": utc(), "requests": [], "passed": False}
    try:
        run(args, result)
    except (KeyError, IndexError, OSError, ValueError) as exc:
        result["collection_error"] = str(exc)
    result["finished_at"] = utc()
    print(json.dumps(result, sort_keys=True))
    sys.exit(0 if result["passed"] else 1)
