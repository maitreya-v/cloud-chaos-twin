"""Record independent warm-up and measured runs on the shared Boutique deployment."""
from collections import Counter
import datetime as dt
import json
import os
from pathlib import Path
import re
import secrets
import subprocess
import sys
import time
from urllib.parse import urlsplit

from . import signals, stack

HERE = Path(__file__).resolve().parent
SOURCE = HERE.parent
OUT = SOURCE / "outputs" / "telemetry"


def kubectl(*args, **kwargs):
    return subprocess.run(["kubectl","--request-timeout=20s","-n",stack.NS,*args],
                          text=True, check=True, **kwargs)

def data(*args):
    return json.loads(kubectl(*args, "-o","json",stdout=subprocess.PIPE).stdout)

def save(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")

def collector_identity():
    pods = data("get","pods","-l","app=cct-collector")["items"]
    ready = [(p["metadata"]["uid"], c["containerID"],c["restartCount"])
             for p in pods for c in p["status"].get("containerStatuses",[])
             if c["name"]=="collector" and c["ready"]]
    if len(ready)!=1:
        raise RuntimeError("Expected one ready Collector")
    return ready[0]

def job(directory, resources, label):
    """Archive logs for both successful and unsuccessful Jobs before cleaning up."""
    name = resources[-1]["metadata"]["name"]
    config = directory / (label + "-manifest.json")
    save(config, {"apiVersion":"v1","kind":"List","items":resources})
    created = []
    try:
        for resource in resources:
            kubectl("create", "-f", "-", input=json.dumps(resource))
            created.append(resource["kind"].lower() + "/" + resource["metadata"]["name"])
        deadline = time.monotonic() + 340
        succeeded = False
        while time.monotonic() < deadline:
            status = data("get","job",name).get("status",{})
            if any(c.get("status")=="True" and c.get("type") in ("Complete","Failed")
                   for c in status.get("conditions",[])):
                succeeded = bool(status.get("succeeded"))
                break
            time.sleep(2)
        log = kubectl("logs","job/"+name,"--pod-running-timeout=10s",stdout=subprocess.PIPE).stdout
        raw = directory / (label+".log")
        raw.write_text(log)
        receipt = json.loads(next((line for line in reversed(log.splitlines()) if line.startswith("{")), "{}"))
        save(directory/(label+".json"),receipt)
        if not succeeded:
            print(f"{label}: Job did not complete successfully; see {raw}",flush=True)
        return receipt,succeeded
    finally:
        if created:
            subprocess.run(["kubectl", "--request-timeout=20s", "-n", stack.NS, "delete",
                            "--ignore-not-found", "--wait=false", *reversed(created)], check=False)

def targets_up(observations):
    return all(isinstance(v,(int,float)) and float(v)==1 for v in observations) and len(observations)==2

def phase(base, name, stage, revision):
    directory = base / stage
    directory.mkdir()
    run = name+"-"+stage
    print(f"Starting {stage} shopping run: {run}", flush=True)
    journeys, job_ok = job(directory, stack.probe_job(run), "journeys")
    if journeys.get("run_id")!=run or not journeys.get("finished_at"):
        raise RuntimeError(f"Missing shopping receipt in {directory}")
    coverage = {e["journey"]:e.get("coverage",{}) for e in journeys.get("requests",[])
                if e["journey"] in ("browse","cart","checkout")}
    trace = all(coverage.get(j,{}).get("passes_declared_path") is True
                and coverage[j].get("matched_edges")==n for j,n in (("browse",4),("cart",2),("checkout",9)))
    capture = signals.capture(run, journeys["started_at"].replace("+00:00","Z"),
                              journeys["finished_at"].replace("+00:00","Z"),directory/"signals")
    event_counts = {}
    stored = json.loads((directory/"signals"/"logs.json").read_text())
    start, end = (signals.nanos(t.replace("+00:00","Z")) for t in (journeys["started_at"],journeys["finished_at"]))
    ids = set()
    for group in stored["resourceLogs"]:
        service = next(a["value"]["stringValue"] for a in group["resource"]["attributes"] if a["key"]=="service.name")
        by_event = event_counts.setdefault(service,Counter())
        for scope in group["scopeLogs"]:
            for record in scope["logRecords"]:
                attrs = {a["key"]:a["value"]["stringValue"] for a in record["attributes"]}
                if (attrs["cct.run_id"] != run or not start <= int(record["timeUnixNano"]) < end
                        or attrs["cct.event_id"] in ids):
                    raise ValueError("Inconsistent event identity or timestamp in selected log capture")
                ids.add(attrs["cct.event_id"])
                by_event[record["body"]["stringValue"]] += 1
    relay, relayed = {}, False
    stable = False
    counts = capture["selected_by_service"]
    if sum(counts.values()):
        resources = stack.relay_job(run, directory/"signals")
        resources[0]["data"]["forward.py"] = (HERE/"deliver.py").read_text()
        before = collector_identity()
        relay, relayed = job(directory, resources, "delivery")
        stable = before == collector_identity()
    deltas=journeys.get("call_deltas",{})
    services=sorted(s for s in signals.APP_COMPONENTS-{"redis-cart"} if (deltas.get(s) or 0)>0)
    # The shopping probe archives the Collector job's up series before and after the run.
    health=[float(r["value"][1]) for group in ("collector_before","collector_after")
            for r in journeys.get(group,[]) if r.get("metric",{}).get("__name__")=="up"]
    log_delivery = (relayed and stable and relay.get("accepted")==["logs","metrics"]
                    and relay.get("selected")==sum(counts.values()) and relay.get("file_export_count_matches") is True)
    result={"phase":stage,"run_id":run,"source_revision":revision,"utc":[journeys["started_at"],journeys["finished_at"]],
        "journey_edges":{j:v.get("matched_edges") for j,v in coverage.items()},"trace_complete":trace,
        "positive_call_delta_services":services,"probe_metric_activity":journeys.get("metric_activity",False),
        "collector_scrape_up_before_and_after":targets_up(health),"selected_logs":counts,
        "selected_events_by_service":event_counts,"unique_selected_events":len(ids),
        "capture_complete":capture["capture_complete"],"relay_accepted":relay.get("accepted",[]),
        "file_export_delta":relay.get("delta"),"stable_collector_during_delivery":stable,
        "file_export_counts_verified":log_delivery,
        "delivery_evidence":"Collector file-exporter counter around the relay. Individual stored events were not read back.",
        "paired_overhead":"pending separate host measurements"}
    result["signals_verified"] = bool(job_ok and journeys.get("metric_activity") and capture["capture_complete"]
                                      and log_delivery and result["collector_scrape_up_before_and_after"])
    save(directory/"summary.json",result)
    print(json.dumps(result,indent=2),flush=True)
    return result

def main():
    sha=subprocess.check_output(["git","-C",str(SOURCE),"rev-parse","HEAD"],text=True).strip()
    cfg=Path.home()/".kube"/"cct-team.yaml"
    if not cfg.is_file():
        raise RuntimeError("Project kubeconfig missing: "+str(cfg))
    os.environ["KUBECONFIG"]=str(cfg)
    server=kubectl("config","view","--minify","-o","jsonpath={.clusters[0].cluster.server}",stdout=subprocess.PIPE).stdout
    if urlsplit(server).scheme!="https" or not re.fullmatch(r"cct-kubernetes(?:\.[a-z0-9-]+\.ts\.net)?",urlsplit(server).hostname or ""):
        raise RuntimeError("Project kubeconfig selected an unexpected endpoint")
    identity=data("auth","whoami")["status"]["userInfo"]["username"]
    print("Kubernetes identity:",identity,flush=True)
    name = "cct-probe-merul-"+dt.datetime.now(dt.timezone.utc).strftime("%y%m%d%H%M%S")+"-"+secrets.token_hex(4)
    base=OUT/name
    base.mkdir(parents=True, mode=0o700)
    owner=json.loads((SOURCE/"docs/evidence/CCT-03-tracing-rollout.json").read_text())
    apps=json.loads(subprocess.check_output(["kubectl","--request-timeout=20s","-n","cct-app",
                                             "get","pods","-l","app","-o","json"],text=True))["items"]
    images={}
    for pod in apps:
        service=pod["metadata"]["labels"]["app"]
        if service not in signals.APP_COMPONENTS:
            continue
        for container in pod.get("status",{}).get("containerStatuses",[]):
            image=container.get("imageID","").split("://")[-1]
            images.setdefault(service,[]).append({"ready":container["ready"],"image":image})
    save(base/"provenance.json",{"verifier_revision":sha,"kubernetes_identity":identity,"app_images":images,
         "owner_receipt_verified_at":owner["verified_at"]})
    print(f"PRIVATE EVIDENCE: {base}",flush=True)
    phase(base,name,"warmup",sha)
    result=phase(base,name,"measured",sha)
    if result["trace_complete"] and result["signals_verified"]:
        print("RECORDED VERIFICATION PASSED",flush=True)
    else:
        print("RECORDED VERIFICATION INCOMPLETE",flush=True)
    print("Measured receipt:",base/"measured"/"summary.json",flush=True)
    return 0 if result["trace_complete"] and result["signals_verified"] else 1

if __name__=="__main__":
    try:
        sys.exit(main())
    except (OSError,ValueError,KeyError,RuntimeError,subprocess.CalledProcessError) as exc:
        sys.exit("Verification interrupted: "+str(exc))
