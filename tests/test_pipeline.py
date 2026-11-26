import copy
import json
import tempfile
import unittest
from pathlib import Path
from collections import Counter
from ingestion.contract import TOPICS, envelope, read_archive
from processing.normalize import normalize, trace_edges, metric_deltas
from processing.benchmark import records, interval
from processing.replay import validate_snapshot
from observability.export_sample import sample
from observability.fixture import PATHS
from ingestion.recorded import adapt


class PipelineTests(unittest.TestCase):
    def rows(self):
        return [(t,r) for e in records(1,'test') for t,r in normalize(e,1800000000000,TOPICS[e['signal']])]

    def test_fixture_counts_and_edges(self):
        rows=self.rows()
        self.assertEqual(Counter(t for t,r in rows), {'spans':19,'metrics':2,'logs':1})
        edges=list(trace_edges(r for t,r in rows if t=='spans'))
        self.assertEqual({(r['source'],r['target']) for r in edges if r['lineage_status']=='linked'},set(PATHS['checkout']))
        self.assertEqual(sum(r['lineage_status']=='root' for r in edges),1)

    def test_repeat_identity_and_run_isolation(self):
        a=self.rows(); b=self.rows()
        self.assertEqual(a,b)
        r=[r['identity'] for e in records(2,'test') for _,r in normalize(e,1800000000000,TOPICS[e['signal']])]
        self.assertEqual(len(r),len(set(r)))

    def test_availability_and_topic_guard(self):
        e=envelope('fixture-checkout','traces',sample()['traces'],True,1800000000000000001)
        rows=list(normalize(e,1790000000000,TOPICS['traces']))
        self.assertTrue(all(r['available_ms']==1800000000001 for _,r in rows))
        with self.assertRaises(ValueError): list(normalize(e,1800000000000,TOPICS['logs']))

    def test_missing_parent(self):
        spans=[r for t,r in self.rows() if t=='spans']
        server=next(r for r in spans if r['kind']=='SERVER' and r['parent_span_id'])
        server['parent_span_id']='f'*16
        self.assertEqual(next(r for r in trace_edges(spans) if r['span_id']==server['span_id'])['lineage_status'],'missing_parent')

    def test_counter_reset_and_no_baseline(self):
        r=next(r for t,r in self.rows() if t=='metrics')
        r.update(metric_type='sum',temporality='CUMULATIVE',monotonic=True,start_ns=1,value=10)
        r2=dict(r,event_ns=r['event_ns']+100,value=13)
        r3=dict(r2,event_ns=r2['event_ns']+100,value=1)
        d=list(metric_deltas([r3,r,r2]))
        self.assertEqual([x['delta_state'] for x in d],['unknown_baseline','differenced','reset_or_invalid'])
        self.assertEqual([x['value'] for x in d],[None,3,None])

    def test_experiments_are_not_features(self):
        e=envelope('test','experiments',{'run_id':'test','target_service':'frontend'},True)
        self.assertEqual(list(normalize(e,1800000000000,TOPICS['experiments'])),[])

    def test_snapshot_interval(self):
        offsets={t:{'0':0} for t in TOPICS.values()}
        before={'endingOffsets':offsets}; after={'schema_version':'1.0','bootstrap':'localhost:9092','endingOffsets':{t:{'0':3} for t in TOPICS.values()}}
        s=interval(before,after)
        self.assertEqual(validate_snapshot(s),12)
        s['message_count']=11
        with self.assertRaises(ValueError): validate_snapshot(s)

    def test_archive_roundtrip(self):
        with tempfile.TemporaryDirectory() as tmp:
            for s,p in sample().items(): (Path(tmp)/f'{s}.json').write_text(json.dumps(p))
            rows=[(t,r) for e in read_archive(tmp,'fixture-checkout',True) for t,r in normalize(e,1800000000000,TOPICS[e['signal']])]
            self.assertEqual(Counter(t for t,r in rows),{'spans':19,'metrics':2,'logs':1})

    def test_summary_alone_is_not_archive(self):
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp); (p/'summary.json').write_text('{}')
            with self.assertRaises(FileNotFoundError): adapt(p,p/'out')
            self.assertFalse((p/'out').exists())

    def test_recorded_adapter_projection(self):
        from observability.fixture import fixture
        from observability.lineage import inspect_trace
        stamp='2026-11-25T05:21:55.162467+00:00'
        req=[]
        for i,j in enumerate(PATHS,1):
            trace=fixture(j,i)
            for span in trace['spans']:
                span.update(operationName='rpc',startTime=1790000000000000,duration=10)
            if j == 'checkout':
                # Live checkout can include extra legitimate RPC edges.
                root = trace['spans'][0]['spanID']
                extra = dict(trace['spans'][-1], spanID='ffffffffffffffff', processID='productcatalogservice',
                             references=[{'traceID':trace['traceID'],'spanID':root,'refType':'CHILD_OF'}])
                trace['spans'].append(extra)
            lineage=inspect_trace(trace,trace['traceID'],include_records=True)
            req.append({'journey':j,'trace_id':trace['traceID'],'observed_at':stamp,'coverage':{'passes_declared_path':True},'lineage':lineage})
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp); (p/'signals').mkdir()
            (p/'journeys.json').write_text(json.dumps({'run_id':'fixture-checkout','requests':req,'before':{},'after':{}}))
            (p/'summary.json').write_text(json.dumps({'run_id':'fixture-checkout','phase':'measured','trace_complete':True,'signals_verified':True,'unique_selected_events':1,'journey_edges':{j:len(v) for j,v in PATHS.items()},'source_revision':'fixture-only'}))
            for s in ('logs','metrics'): (p/'signals'/f'{s}.json').write_text(json.dumps(sample()[s]))
            meta=adapt(p,p/'out')
            self.assertEqual(meta['counts'],{'spans':19,'metrics':2,'logs':1})
            self.assertFalse(meta['historical_live_forecast_valid'])

if __name__=='__main__': unittest.main()
