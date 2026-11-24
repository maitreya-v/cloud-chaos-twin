"""Controlled lineage ablation on three explicitly generated RPC topologies."""
import json
from .lineage import coverage, inspect_trace

# Independent expected topology for each hand-constructed fixture; all are invented.
PATHS = {"browse": (("frontend","currencyservice"), ("frontend","productcatalogservice"),
                    ("frontend","cartservice"), ("frontend","adservice")),
         "cart": (("frontend","productcatalogservice"), ("frontend","cartservice")),
         "checkout": (("frontend","checkoutservice"), ("frontend","recommendationservice"),
                      ("frontend","currencyservice"), ("checkoutservice","cartservice"),
                      ("checkoutservice","productcatalogservice"), ("checkoutservice","currencyservice"),
                      ("checkoutservice","shippingservice"), ("checkoutservice","paymentservice"),
                      ("checkoutservice","emailservice"))}


def fixture(journey, number):
    tid = f"{number:032x}"
    spans, processes = [], {}
    def span(service, kind, parent=None):
        key = f"{len(spans)+1:016x}"
        processes[service] = {"serviceName": service}
        refs = [{"traceID": tid, "spanID": parent, "refType": "CHILD_OF"}] if parent else []
        spans.append({"traceID": tid, "spanID": key, "processID": service, "references": refs,
                      "tags": [{"key": "span.kind", "value": kind}]})
        return key
    ids = {"frontend": span("frontend", "server")}
    for caller,callee in PATHS[journey]:
        client = span(caller, "client", ids[caller])
        ids[callee] = span(callee, "server", client)
    return {"traceID": tid, "processes": processes, "spans": spans}


def benchmark():
    result = []
    for i,j in enumerate(PATHS, 1):
        trace = fixture(j,i)
        stats = inspect_trace(trace, trace["traceID"])
        gold = set(PATHS[j])
        found = {(e["source"],e["target"]) for e in stats["edges"]}
        result.append({"journey": j, "expected": len(gold), "direct_matches": stats["direct_parent_edges"],
                       "ancestor_matches": len(found & gold), "unexpected": len(found-gold),
                       "passes_declared_path": coverage(j,stats)["passes_declared_path"]})
    return {"schema_version": "1.0", "synthetic": True, "seed": None, "runs": result}


if __name__ == "__main__":
    print(json.dumps(benchmark(), indent=2))
