#!/usr/bin/env python3
"""Supplementary, versioned outcome scoring; never rewrites primary results."""
import argparse
import hashlib
import json
from pathlib import Path
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from analytics.evaluate import evaluate,aggregate


def load(path):return json.loads(path.read_text())


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('campaign',type=Path)
    a=parser.parse_args();folder=a.campaign
    plan_path=folder/'outcome-evaluation-plan.v2.json';plan=load(plan_path)
    frozen=load(folder/'calibration.frozen.json');summary={}
    for name,expected in plan['implementation_sha256'].items():
        if hashlib.sha256((ROOT/name).read_bytes()).hexdigest()!=expected:
            raise ValueError('Supplementary implementation changed after its recorded freeze: '+name)
    for run in frozen['native']['split']['validation']+frozen['native']['split']['test']:
        directory=folder/'runs'/run;receipt=load(directory/'receipt.json')
        heldout=run in plan['reserved_test_runs']
        if heldout and max(plan['frozen_at_ms'],plan['scope_amended_at_ms'],plan['implementation_bound_at_ms'])>=receipt['fault']['request_started_ms']:
            raise ValueError('Supplementary protocol was not fixed before this fault')
        path=directory/'prospective-decisions.jsonl';source_hash=hashlib.sha256(path.read_bytes()).hexdigest()
        predictions=[json.loads(line) for line in path.read_text().splitlines()]
        outcomes=load(directory/'reference-service-windows.reviewed.json')
        primary=load(directory/'prospective-evaluations.json');scores=[]
        for prediction,original in zip(predictions,primary):
            strict=evaluate(prediction,outcomes,frozen['native'],
                original['outcome_source'],'productcatalogservice',frozen['independent_reference'])
            strict.pop('outcome_unknown_policy',None)
            comparison=dict(original);comparison.pop('outcome_unknown_policy',None)
            if strict!=comparison:raise ValueError('Original conservative scoring changed')
            scores.append(evaluate(prediction,outcomes,frozen['native'],
                original['outcome_source'],'productcatalogservice',frozen['independent_reference'],True))
        if len(predictions)!=len(primary) or not scores:raise ValueError('Decision/evaluation counts differ or are empty')
        result={'run_id':run,'scope':'held-out supplementary test' if heldout else 'exploratory supplementary analysis',
            'plan_sha256':hashlib.sha256(plan_path.read_bytes()).hexdigest(),
            'prediction_source_sha256':source_hash,'original_conservative_scores_reproduce':True,
            'outcome_unknown_policy':'observed-pending-v2','historical_live_forecast_valid':False,
            'summary':aggregate(scores),'evaluations':scores}
        (directory/'confirmed-outcome-evaluations.supplementary.json').write_text(json.dumps(result,indent=2)+'\n')
        if hashlib.sha256(path.read_bytes()).hexdigest()!=source_hash:raise ValueError('Predictions changed during supplementary scoring')
        summary[run]={k:v for k,v in result.items() if k!='evaluations'}
    (folder/'confirmed-outcome-summary.supplementary.json').write_text(json.dumps(summary,indent=2)+'\n')
    print(json.dumps({'runs':len(summary),'held_out_runs':plan['reserved_test_runs'],'primary_scores_preserved':True}))


if __name__=='__main__':main()
