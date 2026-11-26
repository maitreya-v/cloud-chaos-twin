"""Convert Merul's measured receipt directory to a replay archive (server-span projection)."""
import argparse
import json
from pathlib import Path
from collections import defaultdict
from observability.signals import attributes, nanos
from ingestion.contract import digest, read_archive
from processing.normalize import normalize
from ingestion.contract import TOPICS


def adapt(source, output):
    source, output = Path(source), Path(output)
    receipt = json.loads((source/'journeys.json').read_text())
    summary = json.loads((source/'summary.json').read_text())
    run = receipt['run_id']
    if summary['run_id'] != run or summary['phase'] != 'measured' or not summary['trace_complete'] or not summary['signals_verified']:
        raise ValueError('expected matching verified measured run')
    groups = defaultdict(list)
    observed = []
    selected = set()
    for request in receipt['requests']:
        journey = request['journey']
        if journey not in ('browse','cart','checkout'):
            continue
        if journey in selected or not request['coverage']['passes_declared_path']:
            raise ValueError('duplicate or incomplete journey')
        selected.add(journey)
        observed.append(nanos(request['observed_at'].replace('+00:00','Z')))
        records = request['lineage']['server_records']
        ids = {r['span_id'] for r in records}
        if not records or any(r['trace_id'] != request['trace_id'] or (r['parent_server_id'] and r['parent_server_id'] not in ids) for r in records):
            raise ValueError('invalid server projection')
        for r in records:
            span = dict(traceId=r['trace_id'], spanId=r['span_id'], name=r['operation'], kind=2,
                        startTimeUnixNano=str(r['start_us']*1000), endTimeUnixNano=str((r['start_us']+r['duration_us'])*1000),
                        status={'code':2 if r['error'] else 0})
            if r['parent_server_id']:
                span['parentSpanId'] = r['parent_server_id']
            groups[r['service']].append(span)
    if selected != {'browse','cart','checkout'}:
        raise ValueError('missing journey')
    payloads = {'traces': {'resourceSpans':[{'resource':{'attributes':attributes(**{'service.name':s,'cct.run_id':run})},'scopeSpans':[{'spans':v}]} for s,v in sorted(groups.items())]}}
    for name in ('metrics','logs'):
        payloads[name] = json.loads((source/'signals'/f'{name}.json').read_text())
    # Archive the actual Prometheus counter observations as cumulative-value gauges.
    # No fabricated counter epoch, interpolation or per-window requests.
    by_service = defaultdict(list)
    for label in ('before','after'):
        for service, point in receipt[label].items():
            by_service[service].append({'timeUnixNano':str(round(point['evaluation_time_s']*1e9)), 'asDouble':point['value']})
    for service, points in sorted(by_service.items()):
        payloads['metrics']['resourceMetrics'].append({'resource':{'attributes':attributes(**{'service.name':service,'cct.run_id':run})},'scopeMetrics':[{'metrics':[{'name':'cct.probe.observed_call_counter','unit':'1','gauge':{'dataPoints':points}}]}]})
    from ingestion.contract import envelope
    from collections import Counter
    counts = Counter()
    from processing.normalize import trace_edges
    from observability.lineage import EXPECTED
    spans = []
    for signal,payload in payloads.items():
        for table,row in normalize(envelope(run,signal,payload,False,max(observed)),max(observed)//1000000,TOPICS[signal]):
            counts[table]+=1
            if table=='spans': spans.append(row)
    if counts['logs'] != summary['unique_selected_events']:
        raise ValueError('selected log count differs from summary')
    # Reconcile per-trace distinct service edges, not global unique pairs.
    for request in receipt['requests']:
        if request['journey'] not in selected: continue
        found={(r['source'],r['target']) for r in trace_edges([s for s in spans if s['trace_id']==request['trace_id']]) if r['lineage_status']=='linked' and r['source']!=r['target']}
        expected={(e['source'],e['target']) for e in request['lineage']['edges']}
        required = {(s,t) for s, targets in EXPECTED[request['journey']].items() for t in targets}
        if found != expected or not required <= found or len(required) != summary['journey_edges'][request['journey']]:
            raise ValueError('trace edge reconciliation failed')
    output.mkdir(parents=True,exist_ok=False)
    for name,payload in payloads.items():
        (output/f'{name}.json').write_text(json.dumps(payload,indent=2)+'\n')
    metadata={'run_id':run,'synthetic':False,'observed_ns':max(observed),'counts':dict(counts),'source_revision':summary['source_revision'],'source_summary_sha256':digest(summary),'trace_representation':'Jaeger server projection; client/internal spans not available','historical_live_forecast_valid':False}
    (output/'archive.json').write_text(json.dumps(metadata,indent=2)+'\n')
    return metadata


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__); p.add_argument('source'); p.add_argument('output'); a=p.parse_args()
    print(json.dumps(adapt(a.source,a.output),indent=2))
