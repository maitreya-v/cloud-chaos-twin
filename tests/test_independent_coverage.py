import copy
import hashlib
import unittest

from observability.coverage import reconcile, proofs_for_windows, closed_counters
from observability.fixture import fixture
from observability.reference import PORTS


class IndependentCoverageTests(unittest.TestCase):
    def data(self):
        trace=fixture("cart",1);trace_id=trace["traceID"]
        before={};after={};ledger=[]
        by_service={}
        for span in trace["spans"]:
            span.update(startTime=1000000,duration=100,operationName="hipstershop.Test/Call")
            tags={t["key"]:t["value"] for t in span.get("tags",[])}
            if tags.get("span.kind")!="server":continue
            service=trace["processes"][span["processID"]]["serviceName"]
            if service=="frontend":
                span.setdefault("tags",[]).extend([{"key":"http.request.method","value":"POST"},{"key":"url.path","value":"/cart"}])
                parent="e"*16;span["references"]=[{"refType":"CHILD_OF","traceID":trace_id,"spanID":parent}]
                path="/cart";code=302;grpc=None
            else:
                parent=next(r["spanID"] for r in span["references"] if r["refType"]=="CHILD_OF")
                path="/hipstershop.Test/Call";code=200;grpc=0
            by_service[service]=(parent,path,code,grpc)
        for service in PORTS:
            uid=hashlib.sha256(service.encode()).hexdigest()
            def counters(n):
                return {"pod_uid_sha256":uid,"received_log_records":2*n,
                    "received_events":{"DownstreamStart":n,"DownstreamEnd":n},
                    "counters":{"http.cct_reference.downstream_rq_total":n,
                                "http.cct_reference.downstream_rq_completed":n,
                                "http.cct_reference.downstream_rq_active":0,
                                "filesystem.write_failed":0,"filesystem.reopen_failed":0,"filesystem.write_total_buffered":0}}
            before[service]=counters(0);after[service]=counters(int(service in by_service))
            if service not in by_service:continue
            parent,path,code,grpc=by_service[service]
            for sequence,event in enumerate(("DownstreamStart","DownstreamEnd"),1):
                ledger.append({"service":service,"sequence":sequence,"pod_uid_sha256":uid,"seen_ms":2000,
                    "record":{"event":event,"traceparent":f"00-{trace_id}-{parent}-01","start_epoch_s":"0.999990",
                              "method":"POST","path":path,"duration_us":120,"upstream_end_us":120,
                              "http_status":code,"grpc_status":grpc,"flags":"-"}})
        entries={trace_id:{"trace":trace,"client":{"external_parent":"e"*16}}}
        requests=[{"trace_id":trace_id}]
        return entries,requests,before,after,ledger

    def test_independent_completions_reconcile_and_reviews_are_not_backdated(self):
        pairs=reconcile(*self.data())
        self.assertEqual(len(pairs),3)
        windows=[{"run_id":"fixture","service":p["service"],"start_ms":0,"end_ms":5000,"requests":1,"errors":0} for p in pairs]
        proofs=proofs_for_windows("fixture",windows,pairs,"fixture-pairs.jsonl")
        self.assertTrue(all(p["verified"] and p["records_reconciled"] for p in proofs))
        self.assertTrue(all(p["available_ms"]>=5000 for p in proofs))

    def test_missing_reference_record_does_not_become_complete(self):
        data=list(self.data());data[-1]=data[-1][:-1]
        with self.assertRaisesRegex(ValueError,"lost records"):
            reconcile(*data)

    def test_missing_native_completion_is_detected_by_the_independent_channel(self):
        data=list(self.data());trace=next(iter(data[0].values()))["trace"]
        target=next(s for s in trace["spans"] if trace["processes"][s["processID"]]["serviceName"]=="cartservice" and any(t["key"]=="span.kind" and t["value"]=="server" for t in s.get("tags",[])))
        trace["spans"].remove(target)
        with self.assertRaises(ValueError):reconcile(*data)

    def test_unknown_background_context_prevents_a_closed_world_claim(self):
        data=list(self.data());data[-1][0]["record"]["traceparent"]="00-"+"f"*32+"-"+"e"*16+"-01"
        with self.assertRaisesRegex(ValueError,"background or broken-propagation"):
            reconcile(*data)

    def test_pod_replacement_and_output_failure_are_rejected(self):
        data=list(self.data());data[3]["frontend"]["pod_uid_sha256"]="replaced"
        with self.assertRaisesRegex(ValueError,"pod replaced"):reconcile(*data)
        data=list(self.data());data[3]["frontend"]["counters"]["filesystem.write_failed"]=1
        with self.assertRaisesRegex(ValueError,"output failure"):reconcile(*data)

    def test_enclosure_check_rejects_an_unrelated_request_timing(self):
        data=list(self.data());trace=next(iter(data[0].values()))["trace"]
        for span in trace["spans"]:span["startTime"]+=10000
        with self.assertRaisesRegex(ValueError,"not enclosed"):reconcile(*data)

    def test_aspnet_grpc_server_uses_http_path_instead_of_decorated_span_name(self):
        data=list(self.data());trace=next(iter(data[0].values()))["trace"]
        for span in trace["spans"]:
            if trace["processes"][span["processID"]]["serviceName"]=="cartservice" and any(t["key"]=="span.kind" and t["value"]=="server" for t in span.get("tags",[])):
                span["operationName"]="POST /hipstershop.Test/Call"
                span["tags"].append({"key":"url.path","value":"/hipstershop.Test/Call"})
        self.assertEqual(len(reconcile(*data)),3)

    def test_a_live_prefix_never_requires_unarrived_future_traces(self):
        data=list(self.data());data[1][0]['started_epoch']=.9
        data[1].append({'trace_id':'f'*32,'started_epoch':5.1})
        caches={'reference':{},'trace':{}}
        first=reconcile(*data,trace_cutoff_ms=1000,caches=caches)
        self.assertEqual(len(first),3)
        self.assertEqual(reconcile(*data,trace_cutoff_ms=1000,caches=caches),first)
        data[1][-1]['started_epoch']=.95
        with self.assertRaisesRegex(ValueError,'fully archived'):
            reconcile(*data,trace_cutoff_ms=1000,caches=caches)

    def test_a_prior_reset_does_not_hide_a_new_reset_or_poison_idle_boundaries(self):
        entries,requests,before,after,ledger=self.data()
        for snapshot in (before,after):
            row=snapshot['frontend'];row['counters']['http.cct_reference.downstream_rq_total']+=2
            row['counters']['http.cct_reference.downstream_rq_completed']+=1
            row['received_log_records']+=4
            for event in row['received_events']:row['received_events'][event]+=2
        for row in ledger:
            if row['service']=='frontend':row['sequence']+=4
        closed_counters(before,before)
        self.assertEqual(len(reconcile(entries,requests,before,after,ledger)),3)
        after['frontend']['counters']['http.cct_reference.downstream_rq_completed']-=1
        with self.assertRaisesRegex(ValueError,'reset during capture'):
            reconcile(entries,requests,before,after,ledger)


if __name__=="__main__":unittest.main()
