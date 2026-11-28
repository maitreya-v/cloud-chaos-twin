"""Owner-only matched tracing-off/on benchmark with restoration and abort gates.

Uses the unchanged CCT-02 five-user workload and its 900-second acceptance gate.
Does not inject faults, add reference proxies, or alter images/resources.
"""
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

from observability.overhead import compare, load
from observability.reference import PORTS

ROOT=Path(__file__).resolve().parents[1]
KUBE=["kubectl","--kubeconfig",str(ROOT/".local/kubeconfig"),"--context","kind-cloud-chaos-twin","--request-timeout=5s"]


def kube(args,obj=None):
    result=subprocess.run(KUBE+args,input=json.dumps(obj) if obj is not None else None,capture_output=True,text=True,timeout=10)
    if result.returncode:raise RuntimeError(result.stderr.strip())
    return result.stdout


def get(args):return json.loads(kube(args) or "{}")


def save(path,value):path.write_text(json.dumps(value,indent=2,sort_keys=True)+"\n")


def disabled(original,service):
    values=[dict(e) for e in original if not (service=="adservice" and e["name"] in ("JAVA_TOOL_OPTIONS","DISABLE_TRACING"))]
    enable=next(e for e in values if e["name"]=="ENABLE_TRACING")
    enable["value"]="0"
    if service=="adservice":values.append({"name":"DISABLE_TRACING","value":"1"})
    return values


def ready(service,timeout=180):
    until=time.monotonic()+timeout
    while time.monotonic()<until:
        d=get(["-n","cct-app","get","deployment",service,"-o","json"])
        s=d.get("status",{});n=d["spec"]["replicas"]
        if s.get("observedGeneration",0)>=d["metadata"]["generation"] and all(s.get(k,0)==n for k in ("replicas","updatedReplicas","readyReplicas","availableReplicas")):
            return
        time.sleep(1)
    raise RuntimeError("Rollout did not recover: "+service)


def set_env(service,expected,target):
    d=get(["-n","cct-app","get","deployment",service,"-o","json"])
    index=next(i for i,c in enumerate(d["spec"]["template"]["spec"]["containers"]) if c["name"]=="server")
    actual=d["spec"]["template"]["spec"]["containers"][index]["env"]
    if actual==target:return
    if actual!=expected:raise RuntimeError("Environment changed outside this benchmark: "+service)
    path=f"/spec/template/spec/containers/{index}/env"
    patch=[{"op":"test","path":path,"value":expected},{"op":"replace","path":path,"value":target}]
    kube(["-n","cct-app","patch","deployment",service,"--type=json","-p",json.dumps(patch)])
    ready(service)


def website_ready():
    endpoint=os.environ.get("CCT_BOUTIQUE_URL","http://127.0.0.1:8080/")
    for _ in range(35):
        p=subprocess.run(["curl","--silent","--fail","--max-time","5","--output","/dev/null",endpoint],capture_output=True,timeout=7)
        if not p.returncode:return
        time.sleep(1)
    raise RuntimeError("Frontend endpoint did not recover after rollout")


def benchmark(directory,label,stop):
    previous=set((ROOT/"outputs/baselines").iterdir())
    log=(directory/(label+".log")).open("w")
    child=subprocess.Popen([sys.executable,"-u",str(ROOT/"scripts/baseline_boutique.py")],cwd=ROOT,stdout=log,stderr=log)
    evidence=None;name=None;start=time.monotonic();last_report=0
    try:
        while child.poll() is None:
            if stop[0] or (directory/"STOP").exists():raise RuntimeError("Stop requested")
            new=set((ROOT/"outputs/baselines").iterdir())-previous
            if len(new)>1:raise RuntimeError("Concurrent baseline detected; refusing ambiguous ownership")
            if new:
                evidence=new.pop()
                config=evidence/"run-config.json"
                if config.exists():name=json.loads(config.read_text())["pod"]["metadata"]["name"]
                samples=evidence/"resources.jsonl"
                if samples.exists() and samples.stat().st_size:
                    sample=json.loads(samples.read_text().splitlines()[-1])
                    if sample.get("error"):raise RuntimeError("Readiness/resource sample failed")
                    initial=json.loads((evidence/"initial.json").read_text())
                    def ids(s):return {p["uid"]:(p["ready"],p["phase"],{c["name"]:c["restarts"] for c in p["containers"]}) for p in s["pods"]}
                    if ids(sample)!=ids(initial):raise RuntimeError("Pod identity/readiness/restarts changed during measurement")
                    memory=int(initial["node_capacity"]["memory"].removesuffix("Ki"))*1024
                    if sample["node_usage"]["memory"]["workingSetBytes"]>.8*memory:raise RuntimeError("VM memory headroom below 20%")
                    cond=sample["node_conditions"]
                    if cond.get("Ready")!="True" or any(cond.get(k)!="False" for k in ("MemoryPressure","DiskPressure","PIDPressure")):
                        raise RuntimeError("Node pressure/readiness stop condition")
            if get(["get","networkchaos","-A","-o","json"])["items"]:raise RuntimeError("A fault appeared during the healthy benchmark")
            if name:
                p=subprocess.run(KUBE+["-n","cct-app","exec",name,"--","cat","/results/progress.json"],capture_output=True,text=True,timeout=10)
                if not p.returncode:
                    progress=json.loads(p.stdout)
                    if progress["failures"]:raise RuntimeError("Workload request failed")
                    if time.monotonic()-last_report>=30:
                        print(json.dumps({"benchmark":label,"evidence":evidence.name,"progress":progress}),flush=True)
                        last_report=time.monotonic()
            if time.monotonic()-start>1320:raise RuntimeError("Baseline deadline exceeded")
            time.sleep(5)
        if child.returncode or evidence is None:raise RuntimeError("Baseline failed; see "+label+".log")
        result=load(evidence)
        if result["passed"] is not True:raise RuntimeError("Baseline acceptance gate failed")
        return {"directory":str(evidence.relative_to(ROOT)),"summary":result}
    except BaseException:
        if child.poll() is None:
            child.terminate()
            try:child.wait(timeout=8)
            except subprocess.TimeoutExpired:child.kill();child.wait()
        if name and name.startswith("cct-baseline-"):
            # This name came from the newly created, unique baseline directory.
            try:
                save(directory/(label+"-aborted-workload.json"),{"name":name,"directory":str(evidence)})
                kube(["-n","cct-app","delete","pod",name,"--ignore-not-found","--wait=false"])
                kube(["-n","cct-app","delete","configmap",name,"--ignore-not-found","--wait=false"])
            except Exception as e:print("Workload cleanup needs attention: "+str(e),flush=True)
        raise
    finally:log.close()


def main():
    stamp=dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    directory=ROOT/"outputs/overhead"/stamp;directory.mkdir(parents=True,exist_ok=False)
    stop=[False]
    signal.signal(signal.SIGINT,lambda *_:stop.__setitem__(0,True))
    signal.signal(signal.SIGTERM,lambda *_:stop.__setitem__(0,True))
    receipt={"passed":False,"scope":"runtime tracing/export and derived spanmetrics; same instrumented images, monitoring stack retained",
             "actor":"owner-assisted paired measurement","started_at":dt.datetime.now(dt.timezone.utc).isoformat(),"runs":{},"restoration_errors":[]}
    original={};attempted=[]
    try:
        if get(["get","networkchaos","-A","-o","json"])["items"]:raise RuntimeError("Existing fault prevents a healthy benchmark")
        deployments=get(["-n","cct-app","get","deployments","-o","json"])
        save(directory/"deployments.before.json",deployments)
        for d in deployments["items"]:
            service=d["metadata"]["name"]
            if service not in PORTS:continue
            containers=d["spec"]["template"]["spec"]["containers"]
            if len(containers)!=1 or containers[0]["name"]!="server":raise RuntimeError("Unexpected sidecar/configuration in "+service)
            original[service]=containers[0]["env"]
        if set(original)!=set(PORTS):raise RuntimeError("Incomplete application roster")
        save(directory/"environment.before.json",original)
        for service in PORTS:
            if stop[0]:raise RuntimeError("Stop requested before measurement")
            print("Disabling runtime tracing: "+service,flush=True)
            attempted.append(service)
            set_env(service,original[service],disabled(original[service],service))
        website_ready()
        receipt["runs"]["off"]=benchmark(directory,"off",stop)
        for service in PORTS:
            print("Restoring runtime tracing: "+service,flush=True)
            set_env(service,disabled(original[service],service),original[service])
        website_ready()
        receipt["runs"]["on"]=benchmark(directory,"on",stop)
        receipt["comparison"]=compare(receipt["runs"]["off"]["summary"],receipt["runs"]["on"]["summary"])
        receipt["passed"]=True
    except BaseException as e:
        receipt["error"]=str(e)
        print("Paired measurement stopped: "+str(e),flush=True)
    finally:
        for service in attempted:
            try:set_env(service,disabled(original[service],service),original[service])
            except Exception as e:receipt["restoration_errors"].append(service+": "+str(e))
        try:website_ready();receipt["frontend_recovered"]=True
        except Exception as e:receipt["restoration_errors"].append(str(e))
        receipt["passed"] &= not receipt["restoration_errors"]
        receipt["finished_at"]=dt.datetime.now(dt.timezone.utc).isoformat()
        save(directory/"receipt.json",receipt)
        print(json.dumps({"passed":receipt["passed"],"error":receipt.get("error"),"restoration_errors":receipt["restoration_errors"],"evidence":str(directory)}),flush=True)
    return 0 if receipt["passed"] else 1


if __name__=="__main__":sys.exit(main())
