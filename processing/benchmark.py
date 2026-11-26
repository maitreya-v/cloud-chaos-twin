"""Run isolated synthetic replay scales; retain receipts, no fabricated benchmark results."""
import argparse
import json
from pathlib import Path
from ingestion.contract import envelope, TOPICS
from ingestion.kafka_io import initialize, publish, snapshot, write_new
from observability.export_sample import sample
from processing.replay import run


def replace(value, run_id):
    if isinstance(value, dict):
        return {k:replace(v,run_id) for k,v in value.items()}
    if isinstance(value,list): return [replace(v,run_id) for v in value]
    return run_id if value=='fixture-checkout' else value


def records(n, prefix):
    for i in range(n):
        rid=f'{prefix}-{i}'
        for signal,payload in sample().items():
            from ingestion.contract import SIGNALS
            for group in replace(payload,rid)[SIGNALS[signal]]:
                yield envelope(rid,signal,{SIGNALS[signal]:[group]},True)


def interval(before, after):
    result=dict(after,startingOffsets=before['endingOffsets'])
    result['message_count']=sum(end-result['startingOffsets'][t][p] for t,parts in result['endingOffsets'].items() for p,end in parts.items())
    return result


def main():
    p=argparse.ArgumentParser(description=__doc__); p.add_argument('output',type=Path); p.add_argument('--scales',type=int,nargs='+',default=[1,10,100]); p.add_argument('--master',default='local[2]'); p.add_argument('--bootstrap',default='localhost:9092'); a=p.parse_args()
    if any(n<=0 for n in a.scales) or len(set(a.scales))!=len(a.scales): p.error('scales must be distinct positive integers')
    a.output.mkdir(parents=True,exist_ok=False)
    initialize(a.bootstrap)
    results=[]
    for n in a.scales:
        prefix=f'benchmark-{a.output.name}-{n}'
        before=snapshot(a.bootstrap)
        # Include deliberate duplicate replay to validate identity-based deduplication.
        def repeated():
            yield from records(n,prefix)
            yield from records(1,prefix)
        pub=publish(a.bootstrap,repeated()); snap=interval(before,snapshot(a.bootstrap))
        if sum(pub['acknowledged'].values())!=snap['message_count']: raise RuntimeError('broker not isolated or count mismatch')
        write_new(a.output/f'{n}-snapshot.json',snap); write_new(a.output/f'{n}-publish.json',pub)
        path=a.output/f'{n}-parquet'; receipt=run(snap,path,a.master)
        # Compare physical persisted counts to independent fixture expectations.
        expected={'spans':19*n,'metrics':2*n,'logs':n,'edge_windows':9*n}
        for t,c in expected.items():
            if receipt['table_counts'][t]!=c: raise AssertionError((t,c,receipt['table_counts'][t]))
        if receipt['duplicates_removed']!={'spans':19,'metrics':2,'logs':1}: raise AssertionError(receipt['duplicates_removed'])
        if run(snap,path,a.master)!=receipt: raise AssertionError('restart changed receipt')
        # Full recomputation from identical frozen offsets at smallest scale.
        if n==min(a.scales):
            rebuilt=run(snap,a.output/f'{n}-rebuilt',a.master)
            if rebuilt['table_counts']!=receipt['table_counts']: raise AssertionError('rebuild changed counts')
        results.append({'fixture_runs':n,'producer_seconds':pub['seconds'],'replay_seconds':receipt['elapsed_seconds'],'messages':snap['message_count'],'messages_per_second':receipt['messages_per_second'],'parquet_bytes':sum(receipt['parquet_bytes'].values()),'table_counts':receipt['table_counts'],'restart_count_check':True})
        write_new(a.output/f'{n}-result.json',results[-1])
    write_new(a.output/'benchmark.json',{'synthetic':True,'master':a.master,'results':results})
    print(json.dumps(results,indent=2))

if __name__=='__main__': main()
