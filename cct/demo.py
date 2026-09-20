"""A small synthetic-data walkthrough, not a root-cause or forecast model."""

import csv
import html
import json
import math
from collections import defaultdict
from pathlib import Path


def load_spans(path):
    dataset = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(dataset, dict) or dataset.get("schema_version") != "1.0" or dataset.get("synthetic") is not True:
        raise ValueError("This demo requires a schema_version=1.0 synthetic dataset.")
    spans = dataset.get("spans")
    if not isinstance(spans, list) or not spans:
        raise ValueError("spans must be a non-empty list")
    unique = {}
    for span in spans:
        if not isinstance(span, dict):
            raise ValueError("Each span must be an object")
        for name in ("run_id", "trace_id", "span_id", "service", "phase"):
            if not isinstance(span.get(name), str) or not span[name].strip():
                raise ValueError("Span requires a non-empty {}".format(name))
        if span.get("phase") not in ("healthy", "fault", "recovery"):
            raise ValueError("Unknown synthetic phase")
        if span.get("status") not in ("OK", "ERROR") or span.get("kind") != "SERVER":
            raise ValueError("Expected normalized SERVER span with OK/ERROR status")
        if "parent_span_id" not in span:
            raise ValueError("parent_span_id is required; use null for a root span")
        parent = span["parent_span_id"]
        if parent is not None and (not isinstance(parent, str) or not parent.strip()):
            raise ValueError("parent_span_id must be a non-empty string or null")
        if parent == span["span_id"]:
            raise ValueError("A span cannot be its own parent")
        for name in ("start_time_ms", "duration_ms"):
            value = span.get(name)
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
                raise ValueError("{} must be a finite non-negative number".format(name))
        key = (span["run_id"], span["trace_id"], span["span_id"])
        if key in unique and span != unique[key]:
            raise ValueError("Conflicting duplicate span identity")
        unique[key] = span
    return {**dataset, "spans": list(unique.values())}


def summarize(dataset):
    spans = dataset["spans"]
    index = {(s["run_id"], s["trace_id"], s["span_id"]): s for s in spans}
    nodes = sorted({s["service"] for s in spans})
    edges = defaultdict(list)
    groups = defaultdict(list)
    missing_parents = 0
    for span in spans:
        groups[(span["run_id"], span["phase"], span["service"])].append(span)
        if span["parent_span_id"] is None:
            continue
        parent = index.get((span["run_id"], span["trace_id"], span["parent_span_id"]))
        if parent is None:
            missing_parents += 1
        elif parent["service"] != span["service"]:
            edges[(parent["service"], span["service"])].append(span)
    graph = {
        "synthetic": True,
        "edge_direction": "caller_to_callee",
        "nodes": nodes,
        "missing_parent_spans": missing_parents,
        "edges": [
            {"source": source, "target": target, "call_count": len(records)}
            for (source, target), records in sorted(edges.items())
        ],
    }
    phase_order = {"healthy": 0, "fault": 1, "recovery": 2}
    rows = []
    for (run_id, phase, service), records in sorted(groups.items(), key=lambda pair: (pair[0][0], phase_order[pair[0][1]], pair[0][2])):
        rows.append({
            "run_id": run_id,
            "phase": phase,
            "service": service,
            "request_count": len(records),
            "mean_duration_ms": round(sum(s["duration_ms"] for s in records) / len(records), 3),
            "error_fraction": round(sum(s["status"] == "ERROR" for s in records) / len(records), 3),
        })
    return graph, rows


def write_report(dataset, output):
    output = Path(output)
    graph, rows = summarize(dataset)
    output.mkdir(parents=True, exist_ok=True)
    (output / "graph.json").write_text(json.dumps(graph, indent=2) + "\n", encoding="utf-8")
    with (output / "service_summary.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    edge_rows = "".join("<tr><td>{}</td><td>{}</td><td>{}</td></tr>".format(html.escape(e["source"]), html.escape(e["target"]), e["call_count"]) for e in graph["edges"])
    headers = "".join("<th>{}</th>".format(html.escape(key.replace("_", " "))) for key in rows[0])
    values = "".join("<tr>" + "".join("<td>{}</td>".format(html.escape(str(value))) for value in row.values()) + "</tr>" for row in rows)
    page = """<!doctype html>
<html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Cloud Chaos Twin · Offline Demo</title>
<style>body{font:16px/1.6 system-ui,sans-serif;background:#f3f6fa;color:#162333;margin:0}main{max-width:1050px;margin:auto;padding:40px 24px}h1{font-size:36px;margin-bottom:8px}.notice{background:#fff4d6;border-left:4px solid #b98012;padding:14px 18px}.card{background:white;border:1px solid #dbe3ee;border-radius:12px;padding:22px;margin:24px 0;overflow:auto}table{border-collapse:collapse;width:100%;font-size:14px}th,td{text-align:left;padding:10px;border-bottom:1px solid #e2e8f0}th{text-transform:capitalize;color:#385574}a{color:#0756a3}.meta{color:#52657b}</style>
<main><p class="meta">GROUP 1 · STARTER WALKTHROUGH</p><h1>Cloud Chaos Twin</h1>
<p>Inspect the dependency graph and healthy / fault / recovery summaries.</p>
<p class="notice"><strong>Synthetic demo data.</strong> These records are invented. No live application, fault injection, root-cause diagnosis, or future-impact prediction ran. Small sample means are illustrative.</p>
<section class="card"><h2>Observed call graph</h2><p>Arrows mean caller → callee. Failure impact can travel in the opposite direction.</p>
<table><thead><tr><th>Caller</th><th>Callee</th><th>Calls</th></tr></thead><tbody>EDGES</tbody></table><p>Missing parent spans: MISSING</p></section>
<section class="card"><h2>Service summaries</h2><table><thead><tr>HEADERS</tr></thead><tbody>VALUES</tbody></table></section>
<p><a href="graph.json">Graph JSON</a> · <a href="service_summary.csv">Summary CSV</a></p>
<p class="meta">Next milestone: replace this synthetic fixture with validated, normalized telemetry from the team's testbed.</p></main></html>
"""
    page = page.replace("EDGES", edge_rows).replace("MISSING", str(graph["missing_parent_spans"])).replace("HEADERS", headers).replace("VALUES", values)
    report = output / "report.html"
    report.write_text(page, encoding="utf-8")
    return report
