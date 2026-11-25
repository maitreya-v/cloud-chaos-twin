"""Reconstruct caller -> callee from one Jaeger query trace. No clock-based joins."""
from collections import Counter


def inspect_trace(trace, trace_id, external_parent=None, include_records=False):
    if trace["traceID"] != trace_id:
        raise ValueError("unexpected trace ID")
    spans, parents, services, kinds = {}, {}, {}, {}
    duplicates = 0
    for span in trace["spans"]:
        k = span["spanID"]
        if span.get("traceID") != trace_id or not k:
            raise ValueError("span identity does not match trace")
        if k in spans:
            if span != spans[k]:
                raise ValueError("conflicting duplicate span")
            duplicates += 1
            continue
        refs = [r for r in span.get("references", []) if r.get("refType") == "CHILD_OF"]
        if len(refs) > 1 or any(r["traceID"] != trace_id for r in refs):
            raise ValueError("ambiguous or cross-trace parent")
        parents[k] = refs[0]["spanID"] if refs else None
        service = trace["processes"][span["processID"]]["serviceName"]
        if not service:
            raise ValueError("missing service.name")
        services[k] = service
        kinds[k] = next((str(t["value"]).lower() for t in span.get("tags", [])
                         if t["key"] == "span.kind"), "unknown")
        spans[k] = span
    # Resolve each ancestry once; the only permitted external reference is the probe's parent.
    nearest, rooted = {}, {}
    for k in spans:
        chain, active = [], set()
        cursor = k
        while cursor in spans and cursor not in nearest:
            if cursor in active:
                raise ValueError("cycle in trace parents")
            active.add(cursor)
            chain.append(cursor)
            cursor = parents[cursor]
        ancestor = nearest.get(cursor)
        anchor = rooted.get(cursor)
        for child in reversed(chain):
            if anchor is None and kinds[child] == "server" and services[child] == "frontend":
                if parents[child] is None or parents[child] == external_parent:
                    anchor = child
            if kinds[child] == "server":
                ancestor = child
            nearest[child], rooted[child] = ancestor, anchor
    roots = {v for v in rooted.values() if v}
    server_ids = [k for k in spans if kinds[k] == "server" and rooted[k]]
    edges, direct = Counter(), Counter()
    for k in server_ids:
        p = parents[k]
        a = nearest.get(p)
        if a and services[a] != services[k]:
            edges[services[a], services[k]] += 1
        if p in spans and kinds[p] == "server" and services[p] != services[k]:
            direct[services[p], services[k]] += 1
    missing = [k for k,p in parents.items() if p is not None and p not in spans
               and not (p == external_parent and k in roots)]
    summary = {"spans": len(spans), "duplicates": duplicates, "server_spans": len(server_ids),
            "frontend_roots": len(roots), "unlinked_spans": sum(not v for v in rooted.values()),
            "missing_parents": len(missing), "services": sorted({services[k] for k in server_ids}),
            "edges": [{"source": s, "target": t, "calls": n} for (s,t),n in sorted(edges.items())],
            "direct_parent_edges": len(direct)}

    if include_records:
        summary["server_records"] = []
        for k in sorted(server_ids):
            span = spans[k]
            tags = {t["key"]: t["value"] for t in span.get("tags", [])}
            summary["server_records"].append({
                "trace_id": trace_id, "span_id": k, "service": services[k],
                "parent_server_id": nearest.get(parents[k]), "operation": span["operationName"],
                "start_us": span["startTime"], "duration_us": span["duration"],
                "error": tags.get("error") is True or tags.get("otel.status_code") == "ERROR"})
    return summary


EXPECTED = {
    "browse": {"frontend": ["currencyservice", "productcatalogservice", "cartservice", "adservice"]},
    "cart": {"frontend": ["productcatalogservice", "cartservice"]},
    "checkout": {"frontend": ["checkoutservice", "recommendationservice", "currencyservice"],
                 "checkoutservice": ["cartservice", "productcatalogservice", "currencyservice",
                                     "shippingservice", "paymentservice", "emailservice"]},
}


def coverage(journey, stats):
    required = {(s,t) for s, ts in EXPECTED[journey].items() for t in ts}
    observed = {(e["source"],e["target"]) for e in stats["edges"]}
    missing = sorted(required - observed)
    return {"expected_edges": len(required), "matched_edges": len(required & observed),
            "missing_edges": [list(e) for e in missing],
            "passes_declared_path": bool(not missing and stats["frontend_roots"] == 1
                                         and not stats["unlinked_spans"] and not stats["missing_parents"])}
