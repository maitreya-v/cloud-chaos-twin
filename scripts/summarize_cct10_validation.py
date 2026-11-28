#!/usr/bin/env python3
"""Package a finished validation without upgrading failed/unknown evidence."""
import argparse
from collections import Counter,defaultdict
import hashlib
import json
from pathlib import Path
import sys
import zipfile

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from analytics.health import fit
from analytics.evaluate import control_alarms
from ingestion.contract import digest
from observability.reference import PORTS
from observability.signals import nanos


def load(path):return json.loads(path.read_text())


def qualified(rows):
    result=Counter()
    for r in rows:
        if (r['complete'] and r['ready'] is True and r['restart_delta']==0 and
            r['requests'] is not None and r['requests']>=20 and r['errors'] is not None and
            r['errors']/r['requests']<.05 and r['p95_ms'] is not None and r['p95_ms']>0):
            result[r['service']]+=1
    return dict(sorted(result.items()))


def summarize(directory):
    receipt=load(directory/'receipt.json')
    frozen=load(directory/'calibration.frozen.json') if (directory/'calibration.frozen.json').exists() else None
    out={'execution_receipt':receipt,'runs':{},'teammate_signoff_recorded':False,
         'historical_live_forecast_valid':False,'calibration_reproduces':False,
         'scope':'one healthy training run and three reserved repetitions of one bounded delay scenario',
         'limits':['Owner-assisted technical validation does not record teammate personal acceptance.',
                   'Three trials on one target do not establish cross-target or cross-fault generalization.',
                   'Retrospective label agreement and prospective decision scoring are separate.',
                   'Missed or late independent frames remain unknown; no availability times are backdated.']}
    for name in ('retained-environment-review','session-source','outcome-evaluation-plan.v2','final-shared-health'):
        if (directory/(name+'.json')).exists():out[name]=load(directory/(name+'.json'))
    for run in receipt['runs']:
        folder=directory/'runs'/run;r=load(folder/'receipt.json')
        result={k:r.get(k) for k in ('passed','error','source_revision','mode','protocol','measurement_interval_utc',
            'workload_report','fault','capture','coverage_review','live_observations','health_unchanged',
            'remaining_fault','remaining_workload','cleanup_errors')}
        for key,name in (('native','service-windows.reviewed.json'),('independent_reference','reference-service-windows.reviewed.json')):
            if (folder/name).exists():
                rows=load(folder/name);result[key]={'rows':len(rows),'complete':sum(r['complete'] for r in rows),
                    'qualifying_by_service':qualified(rows),'sha256':digest(rows)}
                if frozen and r.get('fault',{}).get('request_started_ms'):
                    start,end=[nanos(v)//1000000 for v in r['measurement_interval_utc']]
                    fault=r['fault']
                    intervals={'before_injection':((start+4999)//5000*5000,fault['request_started_ms']//5000*5000),
                        'after_recovery_plus_30s':((fault['recovered_observed_ms']+30000+4999)//5000*5000,end//5000*5000)}
                    view=[dict(row,available_ms=row['end_ms']) for row in rows]
                    result[key]['retrospective_no_fault_control_alarms']={phase:control_alarms(
                        view,run,PORTS,a,b,frozen[key]) for phase,(a,b) in intervals.items() if a<b}
        for name in ('heldout-label-validation','prospective-summary','phase-latency'):
            path=folder/(name+'.json')
            if path.exists():
                data=load(path)
                if name=='heldout-label-validation':data={k:v for k,v in data.items() if k!='timeline'}
                result[name]=data
        if (folder/'fault.recovered.json').exists():
            controller=load(folder/'fault.recovered.json')
            result['controller_events']=[r.get('events',[]) for r in controller.get('status',{}).get('experiment',{}).get('containerRecords',[])]
            result['controller_timestamp_precision_ms']=1000
        if (folder/'live-observation-summary.json').exists():
            live=load(folder/'live-observation-summary.json')
            result['live_deadline_reasons']=dict(Counter(live['missed_or_retried_windows'].values()))
        supplementary=folder/'confirmed-outcome-evaluations.supplementary.json'
        if supplementary.exists():result['supplementary_confirmed_outcomes']={k:v for k,v in load(supplementary).items() if k!='evaluations'}
        out['runs'][run]=result
    frozen_path=directory/'calibration.frozen.json'
    if frozen_path.exists():
        out['calibration']=frozen
        training=receipt['split']['train']
        if len(training)!=1:raise ValueError('Expected the declared one-run training protocol')
        folder=directory/'runs'/training[0];bounds=load(directory/'healthy-intervals.json')
        for key,name in (('native','service-windows.reviewed.json'),('independent_reference','reference-service-windows.reviewed.json')):
            rebuilt=fit(load(folder/name),receipt['split'],bounds)
            if rebuilt!=frozen[key]:raise ValueError('Frozen calibration does not reproduce: '+key)
        out['calibration_reproduces']=True
    out['artifact_hashes']={str(p.relative_to(directory)):hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(directory.rglob('*')) if p.is_file() and p.name!='validation-summary.json'}
    (directory/'validation-summary.json').write_text(json.dumps(out,indent=2,sort_keys=True)+'\n')
    bundle=directory.with_name(directory.name+'-handoff.zip')
    with zipfile.ZipFile(bundle,'w',zipfile.ZIP_DEFLATED) as z:
        for p in sorted(directory.rglob('*')):
            if p.is_file():z.write(p,str(p.relative_to(directory.parent)))
    return out,{'path':str(bundle.relative_to(ROOT)),'bytes':bundle.stat().st_size,
                'sha256':hashlib.sha256(bundle.read_bytes()).hexdigest()}


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('directory',type=Path)
    p.add_argument('--public-evidence',type=Path)
    a=p.parse_args();a.directory=a.directory.resolve()
    out,bundle=summarize(a.directory)
    if a.public_evidence:
        # Only curated receipts leave ignored raw output. Pod/configuration
        # snapshots, reference trace IDs and detailed timelines stay local.
        evidence={k:v for k,v in out.items() if k!='artifact_hashes'}
        evidence['artifact_hashes']=out['artifact_hashes'];evidence['raw_bundle']=bundle
        a.public_evidence.write_text(json.dumps(evidence,indent=2,sort_keys=True)+'\n')
    print(json.dumps({'passed':out['execution_receipt']['passed'],
        'calibration_reproduces':out['calibration_reproduces'],'runs':len(out['runs']),'raw_bundle':bundle}))


if __name__=='__main__':main()
