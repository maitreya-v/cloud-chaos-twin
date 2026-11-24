"""Write a nested, synthetic OTLP/JSON message per project signal for ingestion development."""
import argparse
import json
from pathlib import Path
from .fixture import fixture
from .signals import attributes, pod_metrics, selected_logs


def sample():
    trace = fixture("checkout", 3)
    base = 1790000000000000000
    groups = {}
    for i,s in enumerate(trace["spans"]):
        service = s["processID"]
        kind = 2 if s["tags"][0]["value"] == "server" else 3
        # All descendants start later and finish earlier than their ancestors.
        span = {"traceId":s["traceID"],"spanId":s["spanID"],"name":"fixture.rpc","kind":kind,
                "startTimeUnixNano":str(base+i*1000),"endTimeUnixNano":str(base+100000-i*1000),
                "attributes": attributes(**{"cct.run_id":"fixture-checkout","cct.synthetic":"true"})}
        if s["references"]:
            span["parentSpanId"] = s["references"][0]["spanID"]
        groups.setdefault(service,[]).append(span)
    pod = {"metadata":{"uid":"fixture-pod","labels":{"app":"frontend"}},
           "status":{"containerStatuses":[{"restartCount":0}],"conditions":[{"type":"Ready","status":"True"}]}}
    logs, _ = selected_logs(['2026-09-21T14:00:00.123456789Z {"message":"order placed"}'],
                             0,base+10**15,base+10**15,"fixture-pod/server","fixture-checkout")
    spec = {"traces":{"resourceSpans":[{"resource":{"attributes":attributes(**{"service.name":k})},
                          "scopeSpans":[{"spans":v}]} for k,v in groups.items()]},
            "metrics":{"resourceMetrics":[pod_metrics(pod,base,"fixture-checkout")]},
            "logs":{"resourceLogs":[{"resource":{"attributes":attributes(**{"service.name":"frontend"})},
                                      "scopeLogs":[{"logRecords":logs}]}]}}
    for signal, key in (("traces","resourceSpans"),("metrics","resourceMetrics"),("logs","resourceLogs")):
        for group in spec[signal][key]:
            group["resource"]["attributes"].extend(attributes(**{"cct.synthetic":"true","cct.schema_version":"1.0"}))
    return spec


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("output", type=Path)
    a = p.parse_args()
    a.output.mkdir(parents=True,exist_ok=True)
    for k,v in sample().items():
        (a.output/(k+".json")).write_text(json.dumps(v)+"\n")
