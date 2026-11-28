#!/usr/bin/env python3
"""Disposable HTTP/gRPC proxy check. Never patches Boutique or injects a fault."""
import datetime as dt
import json
from pathlib import Path
import socket
import secrets
import subprocess
import sys
import time
from urllib import request

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from observability.reference import configuration,sidecar

NAME="cct-reference-check"
KUBE=["kubectl","--kubeconfig",str(ROOT/".local/kubeconfig"),"--context","kind-cloud-chaos-twin"]


def run(args,obj=None):
    p=subprocess.run(KUBE+["--request-timeout=5s","-n","cct-app"]+args,
                     input=json.dumps(obj) if obj is not None else None,capture_output=True,text=True,timeout=10)
    if p.returncode:raise RuntimeError(p.stderr)
    return p.stdout


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1",0));return s.getsockname()[1]


def main():
    stamp=dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out=ROOT/"outputs/reference-checks"/stamp;out.mkdir(parents=True,exist_ok=False)
    lock=json.loads((ROOT/".local/validation/envoy.lock.json").read_text())
    receipt={"passed":False,"scope":"disposable HTTP/gRPC forwarding check","image":lock}
    processes=[];created=False
    try:
        for kind in ("pod","configmap"):
            if run(["get",kind,NAME,"--ignore-not-found","-o","json"]).strip():
                raise RuntimeError("Disposable name already exists: "+kind)
        svc=json.loads(run(["get","services","-o","json"]))["items"]
        services={x["metadata"]["name"]:x for x in svc}
        configs={};containers=[]
        for i,(key,service) in enumerate((("http","frontend"),("grpc","productcatalogservice"))):
            cfg=configuration(service)
            cfg["static_resources"]["listeners"][0]["address"]["socket_address"]["port_value"]+=i
            cfg["admin"]["address"]["socket_address"]["port_value"]+=i
            endpoint=cfg["static_resources"]["clusters"][0]["load_assignment"]["endpoints"][0]["lb_endpoints"][0]["endpoint"]["address"]["socket_address"]
            endpoint.update(address=services[service]["spec"]["clusterIP"],port_value=services[service]["spec"]["ports"][0]["port"])
            configs[key+".json"]=json.dumps(cfg)
            c=sidecar(lock["image"],NAME);c["name"]="reference-"+key
            c["args"][1]="/etc/cct-reference/"+key+".json"
            c["ports"][0]["containerPort"]+=i
            c["startupProbe"]["tcpSocket"]["port"]+=i;c["readinessProbe"]["tcpSocket"]["port"]+=i
            containers.append(c)
        md={"name":NAME,"namespace":"cct-app","annotations":{"cct.reference.check":stamp}}
        cm={"apiVersion":"v1","kind":"ConfigMap","metadata":md,"data":configs}
        pod={"apiVersion":"v1","kind":"Pod","metadata":md,"spec":{"restartPolicy":"Never",
             "automountServiceAccountToken":False,"activeDeadlineSeconds":180,"containers":containers,
             "volumes":[{"name":"cct-reference","configMap":{"name":NAME}}]}}
        created=True
        run(["create","-f","-"],cm);run(["create","-f","-"],pod)
        for _ in range(40):
            status=json.loads(run(["get","pod",NAME,"-o","json"]))["status"]
            if any(c["type"]=="Ready" and c["status"]=="True" for c in status.get("conditions",[])):break
            time.sleep(1)
        else:raise RuntimeError("Disposable proxy did not become Ready")
        ports=[free_port() for _ in range(4)]
        forward_log=(out/"forward.log").open("w")
        proc=subprocess.Popen(KUBE+["--request-timeout=0","-n","cct-app","port-forward","--address=127.0.0.1","pod/"+NAME,
                                  *[f"{local}:{remote}" for local,remote in zip(ports,(18080,18081,15000,15001))]],stdout=forward_log,stderr=forward_log)
        processes.append((proc,forward_log))
        for _ in range(50):
            try:
                with socket.create_connection(("127.0.0.1",ports[0]),timeout=.1):break
            except OSError:time.sleep(.1)
        else:raise RuntimeError("Disposable forward did not start")
        traceparents={key:"00-"+secrets.token_hex(16)+"-"+secrets.token_hex(8)+"-01" for key in ("http","grpc")}
        req=request.Request(f"http://127.0.0.1:{ports[0]}/",headers={"traceparent":traceparents["http"]})
        with request.urlopen(req,timeout=10) as response:
            if response.status!=200:raise RuntimeError("HTTP check failed")
            response.read()
        payload=out/"health-request.bin";payload.write_bytes(b"\0\0\0\0\0")
        result=subprocess.run(["curl","--silent","--show-error","--fail","--http2-prior-knowledge","--max-time","10",
            "-H","content-type: application/grpc","-H","te: trailers","-H","traceparent: "+traceparents["grpc"],
            "--data-binary","@"+str(payload),"-D",str(out/"health-headers.txt"),"-o",str(out/"health-response.bin"),
            f"http://127.0.0.1:{ports[1]}/grpc.health.v1.Health/Check"],capture_output=True,text=True,timeout=15)
        if result.returncode or (out/"health-response.bin").read_bytes()!=b"\0\0\0\0\2\10\1":
            raise RuntimeError("gRPC health response failed: "+result.stderr)
        headers=(out/"health-headers.txt").read_text().lower()
        if "http/2 200" not in headers or "grpc-status: 0" not in headers:raise RuntimeError("gRPC transport/status failed")
        time.sleep(2)
        receipt["checks"]={}
        for key,admin in zip(("http","grpc"),ports[2:]):
            raw=run(["logs",NAME,"-c","reference-"+key]);(out/(key+".log")).write_text(raw)
            logs=[]
            for line in raw.splitlines():
                try:r=json.loads(line)
                except ValueError:continue
                if r.get("cct_reference")=="v1":logs.append(r)
            if len(logs)!=1 or logs[0]["traceparent"]!=traceparents[key]:
                raise RuntimeError("Request log count or trace-context preservation failed: "+key)
            if not isinstance(logs[0]["duration_us"],(int,float)) or logs[0]["duration_us"]<0:
                raise RuntimeError("Reference duration missing")
            with request.urlopen(f"http://127.0.0.1:{admin}/stats?format=json",timeout=5) as response:stats=json.load(response)
            counters={s["name"]:s["value"] for s in stats["stats"] if "name" in s}
            (out/(key+"-stats.json")).write_text(json.dumps(stats,indent=2)+"\n")
            if counters.get("http.cct_reference.downstream_rq_total")!=1 or counters.get("http.cct_reference.downstream_rq_completed")!=1:
                raise RuntimeError("Independent completion counters differ from logs")
            receipt["checks"][key]={"log":logs[0],"completion_counter":1,"trace_context_preserved":True}
        receipt["passed"]=True
    except Exception as e:receipt["error"]=str(e)
    finally:
        for proc,log in processes:
            proc.terminate()
            try:proc.wait(timeout=5)
            except subprocess.TimeoutExpired:proc.kill();proc.wait()
            log.close()
        receipt["cleanup_errors"]=[]
        if created:
            for kind in ("pod","configmap"):
                try:
                    obj=json.loads(run(["get",kind,NAME,"--ignore-not-found","-o","json"]) or "{}")
                    if obj and obj["metadata"].get("annotations",{}).get("cct.reference.check")==stamp:
                        run(["delete",kind,NAME,"--wait=false"])
                    elif obj:raise RuntimeError("Ownership changed; refusing deletion")
                except Exception as e:receipt["cleanup_errors"].append(str(e))
        receipt["passed"] &= not receipt["cleanup_errors"]
        (out/"receipt.json").write_text(json.dumps(receipt,indent=2)+"\n")
        print(json.dumps({"passed":receipt["passed"],"error":receipt.get("error"),"evidence":str(out)}))
    return 0 if receipt["passed"] else 1


if __name__=="__main__":sys.exit(main())
