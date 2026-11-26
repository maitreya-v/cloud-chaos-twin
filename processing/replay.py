"""Finite Kafka -> Spark -> Parquet replay; immutable offset snapshots are the restart checkpoint."""
import argparse
import json
import os
import shutil
import time
import uuid
from pathlib import Path
from ingestion.contract import TOPICS, digest
from processing.normalize import normalize, trace_edges, metric_deltas

COMMON = 'run_id string, synthetic boolean, broker_ms long, source_observed_ns long, identity string, content_hash string, available_ms long'
SCHEMAS = {
    'spans': COMMON + ', trace_id string, span_id string, parent_span_id string, service string, kind string, operation string, start_ns long, end_ns long, event_ns long, duration_ms double, error boolean',
    'logs': COMMON + ', service string, pod_uid string, event_id string, event_ns long, observed_ns long, body string',
    'metrics': COMMON + ', service string, pod_uid string, metric string, unit string, metric_type string, temporality string, monotonic boolean, series_id string, resource_json string, scope_json string, attributes_json string, start_ns long, event_ns long, value double, count long, sum_value double, bounds array<double>, buckets array<long>, missing boolean',
}
EDGE_SCHEMA = 'run_id string, synthetic boolean, trace_id string, span_id string, source string, target string, lineage_status string, event_ns long, duration_ms double, error boolean, available_ms long'
DELTA_SCHEMA = 'run_id string, synthetic boolean, service string, series_id string, metric string, unit string, pod_uid string, event_ns long, interval_start_ns long, available_ms long, delta_state string, value double, count long, sum_value double, bounds array<double>, buckets array<long>'
CONNECTOR = 'org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1'


def parse_message(row):
    return normalize(json.loads(row.value), row.broker_ms, row.topic)


def validate_snapshot(s):
    if s['schema_version']!='1.0' or not s['bootstrap']:
        raise ValueError('invalid snapshot')
    total = 0
    for offsets in ('startingOffsets','endingOffsets'):
        if set(s[offsets])!=set(TOPICS.values()):
            raise ValueError('snapshot does not include all four topics')
    for topic in TOPICS.values():
        a,b=s['startingOffsets'][topic],s['endingOffsets'][topic]
        if set(a)!=set(b) or not a:
            raise ValueError('incompatible snapshot partitions')
        for p in a:
            if not p.isdigit() or type(a[p]) is not int or type(b[p]) is not int or not 0<=a[p]<=b[p]:
                raise ValueError('invalid snapshot offset')
            total += b[p]-a[p]
    if total!=s['message_count'] or total==0:
        raise ValueError('empty or inconsistent snapshot')
    return total


def windowed(df, width):
    from pyspark.sql import functions as F
    return (df.withColumn('start_ms', F.expr(f'event_ns DIV {width*1000000}')*width)
            .withColumn('end_ms', F.col('start_ms')+width))


def with_availability(df):
    from pyspark.sql import functions as F
    return df.withColumn('available_ms', F.greatest('available_ms','end_ms'))


def run(snapshot, output, master='local[2]', width=5000, min_requests=20, cutoff=None):
    from pyspark.sql import SparkSession, Window, functions as F
    if width<=0 or min_requests<=0 or cutoff is not None and cutoff<0:
        raise ValueError('invalid feature settings')
    expected=validate_snapshot(snapshot)
    output=Path(output).resolve()
    settings={'snapshot_sha256':digest(snapshot),'window_ms':width,'min_requests':min_requests,
              'cutoff_ms':cutoff,'pipeline_version':'1.0','master':master}
    if output.exists():
        receipt=json.loads((output/'receipt.json').read_text())
        if receipt['settings']!=settings or any(not (output/t/'_SUCCESS').is_file() for t in receipt['table_counts']):
            raise ValueError('existing output differs or is incomplete; select a new destination')
        return receipt  # committed output; no append or duplicate counting
    output.parent.mkdir(parents=True,exist_ok=True)
    staging=output.parent/('.'+output.name+'.staging-'+uuid.uuid4().hex)
    staging.mkdir(mode=0o700)
    started=time.perf_counter()
    spark=None
    try:
        spark=(SparkSession.builder.appName('CloudChaosTwinReplay').master(master)
               .config('spark.jars.packages',CONNECTOR)
               .config('spark.sql.session.timeZone','UTC')
               .config('spark.sql.shuffle.partitions','8')
               .config('spark.ui.enabled','false').getOrCreate())
        spark.sparkContext.setLogLevel('WARN')
        raw=(spark.read.format('kafka').option('kafka.bootstrap.servers',snapshot['bootstrap'])
             .option('subscribe',','.join(TOPICS.values()))
             .option('startingOffsets',json.dumps(snapshot['startingOffsets']))
             .option('endingOffsets',json.dumps(snapshot['endingOffsets']))
             .option('failOnDataLoss','true').load()
             .select('topic','partition','offset','timestampType',F.col('key').cast('string').alias('key'),
                     F.col('value').cast('string').alias('value'), F.unix_millis('timestamp').alias('broker_ms')).cache())
        count=raw.count()
        if count!=expected:
            raise RuntimeError(f'offset reconciliation failed: expected {expected}, read {count}')
        # Kafka timestampType=1 denotes broker LogAppendTime.
        if raw.filter(F.col('timestampType')!=1).count():
            raise RuntimeError('broker timestamp is not LogAppendTime')
        observed={(r.topic,str(r.partition)):(r.n,r.lo,r.hi) for r in raw.groupBy('topic','partition').agg(
            F.count('*').alias('n'),F.min('offset').alias('lo'),F.max('offset').alias('hi')).collect()}
        for t, parts in snapshot['startingOffsets'].items():
            for p, lo in parts.items():
                hi=snapshot['endingOffsets'][t][p]
                if hi>lo and observed.get((t,p))!=(hi-lo,lo,hi-1):
                    raise RuntimeError(f'partition offset reconciliation failed: {t}/{p}')
        counts={}; bytes_by_table={}; duplicates={}
        def save(name,df):
            target=staging/name
            df.write.mode('errorifexists').parquet(str(target))
            # Verify the physically persisted count, not just a pre-write dataframe count.
            counts[name]=spark.read.parquet(str(target)).count()
            bytes_by_table[name]=sum(p.stat().st_size for p in target.rglob('*.parquet'))
        save('raw_telemetry',raw.filter(F.col('topic')!=TOPICS['experiments']))
        save('raw_experiments',raw.filter(F.col('topic')==TOPICS['experiments']))
        tagged=raw.select('value','broker_ms','topic').rdd.flatMap(parse_message).persist()
        normalized={}
        for name,schema in SCHEMAS.items():
            df=spark.createDataFrame(tagged.filter(lambda x: x[0]==name).map(lambda x:x[1]),schema).cache()
            before=df.count()
            if df.groupBy('identity').agg(F.countDistinct('content_hash').alias('variants')).filter('variants > 1').limit(1).count():
                raise ValueError(f'{name}: conflicting records with same identity; inspect archive')
            w=Window.partitionBy('identity').orderBy('available_ms','broker_ms',F.col('source_observed_ns').asc_nulls_first())
            dedup=df.withColumn('_rank',F.row_number().over(w)).filter('_rank = 1').drop('_rank')
            save(name,dedup)
            duplicates[name]=before-counts[name]
            df.unpersist()
            eligible=spark.read.parquet(str(staging/name))
            if cutoff is not None:
                eligible=eligible.filter((F.col('available_ms')<=cutoff)&(F.col('event_ns')<=cutoff*1000000))
            normalized[name]=eligible
        tagged.unpersist(); raw.unpersist()
        spans=normalized['spans']
        lineage=spark.createDataFrame(spans.rdd.map(lambda r: r.asDict()).map(
            lambda r: ((r['run_id'],r['trace_id']),r)).groupByKey().flatMap(lambda pair:trace_edges(pair[1])),EDGE_SCHEMA)
        save('lineage',lineage)
        edges=spark.read.parquet(str(staging/'lineage')).filter('lineage_status = "linked" AND source != target')
        edge_windows=windowed(edges,width).groupBy('run_id','synthetic','source','target','start_ms','end_ms').agg(
            F.count('*').alias('calls'),F.sum(F.col('error').cast('long')).alias('errors'),
            F.expr('percentile_approx(duration_ms, 0.95, 10000)').alias('p95_ms'),F.max('available_ms').alias('available_ms'))
        service_windows=windowed(spans.filter(F.col('kind')=='SERVER'),width).groupBy('run_id','synthetic','service','start_ms','end_ms').agg(
            F.count('*').alias('requests'), F.sum(F.col('error').cast('long')).alias('errors'),
            F.avg('duration_ms').alias('mean_ms'),F.expr('percentile_approx(duration_ms, 0.95, 10000)').alias('p95_ms'),
            F.max('available_ms').alias('available_ms')).withColumn('low_count',F.col('requests')<min_requests)
        # No independent coverage/roster/freshness data: deliberately do not assert complete or healthy.
        service_windows=(service_windows.withColumn('complete',F.lit(False))
                         .withColumn('ready',F.lit(None).cast('boolean')).withColumn('restart_delta',F.lit(None).cast('long'))
                         .withColumn('error_rate',F.col('errors')/F.col('requests')))
        log_windows=windowed(normalized['logs'],width).groupBy('run_id','synthetic','service','start_ms','end_ms').agg(
            F.count('*').alias('selected_logs'),F.max('available_ms').alias('available_ms'))
        deltas=spark.createDataFrame(normalized['metrics'].rdd.map(lambda r:r.asDict()).map(
            lambda r:(r['series_id'],r)).groupByKey().flatMap(lambda pair:metric_deltas(pair[1])),DELTA_SCHEMA)
        save('metric_deltas',deltas)
        metric_points=windowed(spark.read.parquet(str(staging/'metric_deltas')),width)
        # Last point per series in each window; interval deltas are not assumed to lie wholly in a window.
        w=Window.partitionBy('series_id','start_ms').orderBy(F.col('event_ns').desc(),F.col('available_ms').desc())
        metric_windows=metric_points.withColumn('_rank',F.row_number().over(w)).filter('_rank=1').drop('_rank')
        for name,df in [('service_windows',service_windows),('edge_windows',edge_windows),('log_windows',log_windows),('metric_windows',metric_windows)]:
            df=with_availability(df)
            if cutoff is not None:
                df=df.filter(F.col('available_ms')<=cutoff)
            save(name,df)
        receipt={'settings':settings,'snapshot':snapshot,'table_counts':counts,'duplicates_removed':duplicates,
                 'parquet_bytes':bytes_by_table,'elapsed_seconds':time.perf_counter()-started,
                 'broker_messages':count,'spark_version':spark.version,'feature_emitted_ms':time.time_ns()//1000000,
                 'mode':'offline-replay','historical_live_forecast_valid':False}
        receipt['messages_per_second']=count/receipt['elapsed_seconds']
        (staging/'receipt.json').write_text(json.dumps(receipt,indent=2,sort_keys=True)+'\n')
        if output.exists():
            raise RuntimeError('concurrent writer committed output')
        staging.rename(output)  # same filesystem; only completed data visible at destination
        return receipt
    finally:
        if spark is not None:
            spark.stop()
        if staging.exists():
            shutil.rmtree(staging)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('snapshot',type=Path); p.add_argument('output',type=Path)
    p.add_argument('--master',default='local[2]'); p.add_argument('--window-ms',type=int,default=5000)
    p.add_argument('--min-requests',type=int,default=20); p.add_argument('--cutoff-ms',type=int)
    a=p.parse_args()
    print(json.dumps(run(json.loads(a.snapshot.read_text()),a.output,a.master,a.window_ms,a.min_requests,a.cutoff_ms),indent=2))


if __name__=='__main__':
    main()
