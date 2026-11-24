"""Print an application rollout for the owner. Never contact Kubernetes or change the cluster."""
import argparse
import json
import shlex
from pathlib import Path

SERVICES = ("frontend","productcatalogservice","checkoutservice","currencyservice",
            "paymentservice","emailservice","recommendationservice")
EXTRA = ("adservice", "cartservice", "shippingservice")
ADDR = "cct-collector.cct-observability.svc:4317"


def command(args):
    return " ".join(shlex.quote(v) for v in args)


def plan(images):
    kube = ["kubectl","--context","kind-cloud-chaos-twin","-n","cct-app"]
    # Emit one strategic patch per Deployment, only touching the 'server' container.
    for svc in (*SERVICES, *images):
        addr = "http://" + ADDR if svc in {"currencyservice","paymentservice"} else ADDR
        env = {"ENABLE_TRACING": "1", "COLLECTOR_SERVICE_ADDR": addr, "OTEL_SERVICE_NAME": svc}
        container = {"name":"server"}
        if svc in images:
            container.update(image=images[svc], imagePullPolicy="Never")
            env.update(OTEL_EXPORTER_OTLP_ENDPOINT="http://" + ADDR, OTEL_EXPORTER_OTLP_PROTOCOL="grpc")
        if svc == "adservice":
            env.update(JAVA_TOOL_OPTIONS="-javaagent:/opt/otel/javaagent.jar",
                       OTEL_METRICS_EXPORTER="none", OTEL_LOGS_EXPORTER="none")
        container["env"] = [{"name": k,"value": v} for k,v in env.items()]
        patch = {"spec":{"template":{"spec":{"containers":[container]}}}}
        print(command(kube + ["patch","deployment",svc,"--type=strategic","-p",json.dumps(patch)]))
        print(command(kube + ["rollout","status","deployment/"+svc,"--timeout=180s"]))


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--images", type=Path, help="JSON object containing immutable local names for all three rebuilt images")
    a = p.parse_args()
    images = json.loads(a.images.read_text()) if a.images else {}
    if a.images:
        import re
        if set(images) != set(EXTRA) or not all(re.fullmatch(r"[a-z0-9./_-]+@sha256:[a-f0-9]{64}", v) for v in images.values()):
            p.error("expected a digest reference for each of adservice, cartservice, shippingservice")
    print("set -eu")
    plan(images)
