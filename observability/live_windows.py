"""Causal closed-window checks and prospective direct-query decisions.

The workload drains between fixed groups. Admit a frame only after independent
logs/counters reconcile and every older workload trace has arrived. Unknown or
late frames remain unscored. Full post-run review remains a separate check.
"""
from collections import defaultdict
import json
import math
import threading
import time

from analytics.forecast import predict
from observability.coverage import reconcile,proofs_for_windows
from observability.lineage import inspect_trace
from observability.reference import PORTS


class LiveWindows:
    def __init__(self,calibration=None, *, near_limit=False):
        self.near_limit = near_limit
        self.calibration=calibration;self.stop_event=threading.Event();self.thread=None
        self.frames=[];self.predictions=[];self.errors=[];self.windows=[];self.deadlines={}
        self.caches={"reference":{},"trace":{}}

    def bind(self,archive,reference,directory,run,context,pod_path,join_helper):
        self.archive=archive;self.reference=reference;self.directory=directory;self.run=run
        self.context=context;self.pod_path=pod_path;self.join_helper=join_helper
        self.before=json.loads((directory/"reference-before.json").read_text())
        self.thread=threading.Thread(target=self.loop,daemon=True);self.thread.start()

    def native_rows(self,entries,cutoff):
        rows=[]
        for trace_id,entry in entries.items():
            trace=entry.get("trace")
            if not trace:continue
            if max(s["startTime"]+s["duration"] for s in trace["spans"])<(cutoff-60000)*1000:continue
            summary=inspect_trace(trace,trace_id,entry["client"]["external_parent"],include_records=True)
            if summary["missing_parents"] or summary["unlinked_spans"]:continue
            availability=max(entry["span_observed_ms"].get(s["spanID"],cutoff+1) for s in trace["spans"])
            for span in summary["server_records"]:
                end_us=span["start_us"]+span["duration_us"]
                rows.append({"run_id":self.run,"synthetic":False,"trace_id":trace_id,"span_id":span["span_id"],
                    "parent_span_id":span["parent_server_id"],"service":span["service"],"kind":"SERVER","operation":span["operation"],
                    "start_ns":span["start_us"]*1000,"end_ns":end_us*1000,"event_ns":end_us*1000,
                    "duration_ms":span["duration_us"]/1000,"error":span["error"],
                    "available_ms":max(availability,math.ceil(end_us/1000))})
        return rows

    def attempt(self,end):
        after=self.reference.counters()
        ledger=self.reference.slice_observations(self.before,after)
        begins=list(self.context["begins"].copy().values())
        entries=self.archive.entries.copy()
        pairs=reconcile(entries,begins,self.before,after,ledger,trace_cutoff_ms=end,caches=self.caches)
        groups=defaultdict(list)
        for pair in pairs:
            if (end-5000)*1000<=pair["native_end_us"]<end*1000:groups[pair["service"]].append(pair)
        rows=[]
        for service,values in sorted(groups.items()):
            durations=sorted(v["native_duration_us"] for v in values)
            rows.append({"run_id":self.run,"service":service,"start_ms":end-5000,"end_ms":end,
                "requests":len(values),"errors":sum(v["error"] for v in values),
                "p95_ms":durations[math.ceil(.95*len(values))-1]/1000,
                "available_ms":max(end,max(v["reference_seen_ms"] for v in values)),
                "ready":None,"restart_delta":None,"complete":False})
        proofs=proofs_for_windows(self.run,rows,pairs,"live-frame-proofs.jsonl")
        samples=[]
        with self.pod_path.open() as source:
            for line in source:
                try:row=json.loads(line)
                except ValueError:continue
                if row["seen_ms"]>=end-8000:samples.append(row)
        joined=self.join_helper.join(rows,samples,proofs,self.run)
        if set(r["service"] for r in joined)!=set(PORTS) or not all(r["complete"] for r in joined):
            raise ValueError("window readiness/coverage incomplete")
        now=(time.time_ns()+999999)//1000000
        if now>=end+4900:raise ValueError("window proof missed its decision deadline")
        for row in joined:row["available_ms"]=max(row["available_ms"],now)
        frame={"end_ms":end,"verified_at_ms":now,"counter_snapshot":after,"paired_completions":len(pairs),
               "window_rows":joined,"source":"live independent reference reconciliation","teammate_signoff_recorded":False}
        self.frames.append(frame);self.windows.extend(joined)
        with (self.directory/"live-frame-proofs.jsonl").open("a") as out:out.write(json.dumps(frame)+"\n")
        if self.calibration is not None:
            while time.time_ns()//1000000<max(r["available_ms"] for r in joined):time.sleep(.001)
            cutoff=time.time_ns()//1000000
            if cutoff//5000*5000!=end:return
            prediction=predict(self.native_rows(entries,cutoff),self.windows,self.run,list(PORTS),cutoff,self.calibration,
                               near_limit=self.near_limit)
            prediction.update(mode="live-direct-observation",observation_frame_end_ms=end,
                observation_verified_at_ms=now,scope="prospective owner-assisted direct-query candidate; historical validity flag retained")
            self.predictions.append(prediction)
            with (self.directory/"prospective-decisions.jsonl").open("a") as out:out.write(json.dumps(prediction)+"\n")

    def loop(self):
        done=set()
        while not self.stop_event.is_set():
            start=self.context.get("measurement_start")
            if start is None:time.sleep(.2);continue
            now=time.time_ns()//1000000;end=now//5000*5000
            if end-5000<math.ceil(start*1000) or end in done or now<end+3200:
                time.sleep(.1);continue
            if now>=end+4800:
                done.add(end);self.deadlines.setdefault(end,"No verified frame before deadline");continue
            try:
                self.attempt(end);done.add(end)
                print(json.dumps({"live_window_end_ms":end,"verified_frames":len(self.frames),"prospective_decisions":len(self.predictions)}),flush=True)
            except (ValueError,KeyError,OSError) as e:self.deadlines[end]=str(e)
            except Exception as e:
                self.errors.append(type(e).__name__+": "+str(e));self.stop_event.set()
            time.sleep(.15)

    def close(self):
        self.stop_event.set()
        if self.thread:self.thread.join(timeout=35)
        if self.thread and self.thread.is_alive():self.errors.append("Prospective observer did not stop within its bounded query deadline")
        result={"verified_frames":len(self.frames),"prospective_decisions":len(self.predictions),
            "errors":self.errors,"missed_or_retried_windows":self.deadlines,"historical_live_forecast_valid":False}
        (self.directory/"live-observation-summary.json").write_text(json.dumps(result,indent=2,sort_keys=True)+"\n")
        if not self.thread or not self.thread.is_alive():
            self.caches.clear();self.archive=None
        return result
