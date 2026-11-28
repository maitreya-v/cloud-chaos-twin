"""Owner-controlled temporary reference proxies, with durable originals/restore.

Construct only after paired.py finishes restoring tracing. Install is explicit.
No call is made to the cluster merely by importing this module.
"""
import copy
import hashlib
import json
from pathlib import Path
import socket
import subprocess
import threading
import time
from urllib import request

from observability.paired import KUBE, ROOT, get, kube, ready, save
from observability.reference import PORTS, DATA_PORT, ADMIN_PORT, configuration, sidecar


class ReferenceSession:
    def __init__(self,directory,session_id,image):
        self.directory=Path(directory);self.session_id=session_id;self.image=image
        self.original={};self.services={};self.expected={};self.attempted=[];self.changed_services=[]
        self.processes=[];self.threads=[];self.logs=None;self.log_lock=threading.Lock()
        self.log_counts={s:0 for s in PORTS};self.pods={};self.admin={};self.errors=[]

    def state(self):
        save(self.directory/"reference-state.json",{"session_id":self.session_id,"image":self.image,
             "original_deployments":self.original,"original_services":self.services,
             "expected_templates":self.expected,"attempted":self.attempted,"changed_services":self.changed_services})

    def replace_template(self,service,target):
        d=get(["-n","cct-app","get","deployment",service,"-o","json"])
        actual=d["spec"]["template"]
        expected=self.expected.get(service,self.original[service]["spec"]["template"])
        if actual!=expected:raise RuntimeError("Application template changed outside reference session: "+service)
        patch=[{"op":"test","path":"/spec/template","value":actual},
               {"op":"replace","path":"/spec/template","value":target}]
        # Record the proposed template before a potentially ambiguous API timeout.
        self.expected[service]=target;self.state()
        result=json.loads(kube(["-n","cct-app","patch","deployment",service,"--type=json","-p",json.dumps(patch),"-o","json"]))
        self.expected[service]=result["spec"]["template"];self.state();ready(service)

    def install(self):
        if get(["get","networkchaos","-A","-o","json"])["items"]:raise RuntimeError("Existing fault prevents reference installation")
        deployments=get(["-n","cct-app","get","deployments","-o","json"])["items"]
        for d in deployments:
            service=d["metadata"]["name"]
            if service not in PORTS:continue
            containers=d["spec"]["template"]["spec"]["containers"]
            if len(containers)!=1 or containers[0]["name"]!="server" or d["spec"]["replicas"]!=1:
                raise RuntimeError("Unexpected application replica/container roster")
            env={e["name"]:e.get("value") for e in containers[0].get("env",[])}
            if env.get("ENABLE_TRACING")!="1":raise RuntimeError("Tracing is not restored: "+service)
            self.original[service]=d
        if set(self.original)!=set(PORTS):raise RuntimeError("Incomplete application roster")
        pods=get(["-n","cct-app","get","pods","-o","json"])["items"]
        if any(p["metadata"]["name"].startswith("cct-baseline-") for p in pods):
            raise RuntimeError("A paired benchmark is still active")
        for s in get(["-n","cct-app","get","services","-o","json"])["items"]:
            if s["spec"].get("selector",{}).get("app") in PORTS:self.services[s["metadata"]["name"]]=s
        self.state()
        for service in PORTS:
            name="cct-ref-"+service
            if get(["-n","cct-app","get","configmap",name,"--ignore-not-found","-o","json"]):
                raise RuntimeError("Reference configuration name already exists")
            obj={"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":name,"namespace":"cct-app",
                 "annotations":{"cct.reference.session":self.session_id}},"data":{"envoy.json":json.dumps(configuration(service))}}
            self.attempted.append(service);self.state()
            kube(["create","-f","-"],obj)
            template=copy.deepcopy(self.original[service]["spec"]["template"])
            template.setdefault("metadata",{}).setdefault("annotations",{})["cct.reference.session"]=self.session_id
            template["spec"]["containers"].append(sidecar(self.image,name))
            template["spec"].setdefault("volumes",[]).append({"name":"cct-reference","configMap":{"name":name}})
            print("Installing independent reference: "+service,flush=True)
            self.replace_template(service,template)
        for name,s in self.services.items():
            ports=copy.deepcopy(s["spec"]["ports"])
            for port in ports:port["targetPort"]=DATA_PORT
            patch=[{"op":"test","path":"/metadata/uid","value":s["metadata"]["uid"]},
                   {"op":"test","path":"/spec/ports","value":s["spec"]["ports"]},
                   {"op":"replace","path":"/spec/ports","value":ports}]
            self.changed_services.append(name);self.state()
            kube(["-n","cct-app","patch","service",name,"--type=json","-p",json.dumps(patch)])
        # Existing gRPC channels can bypass a changed Service targetPort until
        # reconnect. Replace each backend once after all routes use the proxies.
        for service in PORTS:
            template=copy.deepcopy(self.expected[service])
            template["metadata"]["annotations"]["cct.reference.connections_reset"]=self.session_id
            print("Resetting established channels: "+service,flush=True)
            self.replace_template(service,template)
        for service in PORTS:
            items=get(["-n","cct-app","get","pods","-l","app="+service,"-o","json"])["items"]
            if len(items)!=1:raise RuntimeError("Unexpected stable reference pod roster")
            p=items[0];self.pods[service]={"name":p["metadata"]["name"],"uid_sha256":hashlib.sha256(p["metadata"]["uid"].encode()).hexdigest()}
        save(self.directory/"reference-pods.json",self.pods)

    def connect(self):
        self.logs=(self.directory/"reference.jsonl").open("x",buffering=1)
        for service,pod in self.pods.items():
            with socket.socket() as s:s.bind(("127.0.0.1",0));port=s.getsockname()[1]
            log=(self.directory/(service+"-admin-forward.log")).open("w")
            proc=subprocess.Popen([a for a in KUBE if not a.startswith("--request-timeout=")]+[
                "--request-timeout=0","-n","cct-app","port-forward","--address=127.0.0.1",
                "pod/"+pod["name"],f"{port}:{ADMIN_PORT}"],stdout=log,stderr=log)
            self.processes.append((proc,log))
            self.admin[service]=f"http://127.0.0.1:{port}"
            for _ in range(50):
                try:
                    with request.urlopen(self.admin[service]+"/ready",timeout=.2) as r:
                        if r.status==200:break
                except OSError:time.sleep(.1)
            else:raise RuntimeError("Reference admin forward failed: "+service)
            proc=subprocess.Popen([a for a in KUBE if not a.startswith("--request-timeout=")]+[
                "--request-timeout=0","-n","cct-app","logs","-f",pod["name"],"-c","cct-reference","--timestamps"],
                stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True)
            log=(self.directory/(service+"-reference.log")).open("w")
            self.processes.append((proc,log))
            def reader(service=service,pod=pod,proc=proc,log=log):
                for line in proc.stdout:
                    log.write(line);log.flush()
                    try:
                        stamp,text=line.split(" ",1);row=json.loads(text)
                        if row.get("cct_reference")!="v1":continue
                    except ValueError:continue
                    if row.get("service")!=service:
                        self.errors.append("Reference service mismatch");continue
                    with self.log_lock:
                        self.log_counts[service]+=1
                        self.logs.write(json.dumps({"service":service,"pod_uid_sha256":pod["uid_sha256"],
                            "sequence":self.log_counts[service],"kube_log_time":stamp,
                            "seen_ms":(time.time_ns()+999999)//1000000,"record":row})+"\n")
                # Normal stop is handled by close(); a caller checks PIDs during capture.
            thread=threading.Thread(target=reader,daemon=True);self.threads.append(thread);thread.start()

    def counters(self):
        out={}
        for service,base in self.admin.items():
            started=time.time_ns()//1000000
            with request.urlopen(base+"/stats?format=json",timeout=3) as response:raw=json.load(response)
            values={s["name"]:s["value"] for s in raw["stats"] if "name" in s and
                    (s["name"].startswith("http.cct_reference.") or s["name"].startswith("filesystem."))}
            out[service]={"started_ms":started,"seen_ms":(time.time_ns()+999999)//1000000,
                          "pod_uid_sha256":self.pods[service]["uid_sha256"],"counters":values,
                          "received_log_records":self.log_counts[service]}
        return out

    def close(self):
        for proc,_ in self.processes:
            if proc.poll() is None:proc.terminate()
        for proc,log in self.processes:
            try:proc.wait(timeout=5)
            except subprocess.TimeoutExpired:proc.kill();proc.wait()
        for thread in self.threads:thread.join(timeout=3)
        for _,log in self.processes:
            if log:log.close()
        if self.logs:self.logs.close();self.logs=None
        self.processes=[]

    def restore(self):
        self.close();errors=[]
        for name in self.changed_services:
            original=self.services[name]
            try:
                current=get(["-n","cct-app","get","service",name,"-o","json"])
                if current["metadata"]["uid"]!=original["metadata"]["uid"]:raise RuntimeError("Service identity changed")
                old=current["spec"]["ports"]
                if old!=original["spec"]["ports"]:
                    if any(p["targetPort"]!=DATA_PORT for p in old):raise RuntimeError("Service ports changed outside session")
                    patch=[{"op":"test","path":"/spec/ports","value":old},
                           {"op":"replace","path":"/spec/ports","value":original["spec"]["ports"]}]
                    kube(["-n","cct-app","patch","service",name,"--type=json","-p",json.dumps(patch)])
            except Exception as e:errors.append(name+": "+str(e))
        for service in self.attempted:
            try:
                current=get(["-n","cct-app","get","deployment",service,"-o","json"])["spec"]["template"]
                original=self.original[service]["spec"]["template"]
                if current!=original:self.replace_template(service,original)
                name="cct-ref-"+service
                obj=get(["-n","cct-app","get","configmap",name,"--ignore-not-found","-o","json"])
                if obj:
                    if obj["metadata"].get("annotations",{}).get("cct.reference.session")!=self.session_id:raise RuntimeError("Reference config ownership changed")
                    kube(["-n","cct-app","delete","configmap",name,"--wait=false"])
            except Exception as e:errors.append(service+": "+str(e))
        save(self.directory/"reference-restoration.json",{"errors":errors,"restored":not errors})
        return errors
