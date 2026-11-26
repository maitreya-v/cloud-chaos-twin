"""Pure normalization shared by Spark executors and offline contract tests."""
import json
import math
from ingestion.contract import canonical, digest, validate, TOPICS


def attrs(items):
    out = {}
    for item in items:
        key, val = item['key'], item['value']
        if key in out or len(val) != 1:
            raise ValueError('ambiguous OTLP attribute')
        # Preserve nested AnyValue type information for identity.
        out[key] = val.get('stringValue', val)
    return out


def ns(value):
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise ValueError('expected integer nanoseconds')
    result = int(value)
    if not 0 <= result < 2**63:
        raise ValueError('timestamp outside signed 64-bit range')
    return result


def number(value):
    result = float(value)
    if not math.isfinite(result):
        raise ValueError('nonfinite metric')
    return result


def ident(value, name):
    if not isinstance(value, str) or not value:
        raise ValueError('missing ' + name)
    return value


def hex_id(value, width):
    value = ident(value, 'span/trace ID').lower()
    if len(value) != width or any(c not in '0123456789abcdef' for c in value) or not int(value, 16):
        raise ValueError('invalid span/trace ID')
    return value


def check_run(run, *attributes):
    if any(a.get('cct.run_id', run) != run for a in attributes):
        raise ValueError('run identity mismatch')


def normalize(e, broker_ms, topic):
    """Return (table, row) pairs. Fail on malformed/unsupported data, never silently drop it."""
    validate(e)
    signal, run, payload = e['signal'], e['run_id'], e['payload']
    if TOPICS[signal] != topic:
        raise ValueError('signal/topic mismatch')
    if broker_ms is None or broker_ms <= 0:
        raise ValueError('missing broker append time')
    base = dict(run_id=run, synthetic=e['synthetic'], broker_ms=broker_ms,
                source_observed_ns=e['source_observed_ns'])
    if signal == 'experiments':
        # Deliberately never returned to feature extraction.
        return
    keys = {'traces': ('resourceSpans', 'scopeSpans', 'spans'),
            'metrics': ('resourceMetrics', 'scopeMetrics', 'metrics'),
            'logs': ('resourceLogs', 'scopeLogs', 'logRecords')}
    group_key, scope_key, item_key = keys[signal]
    for group in payload[group_key]:
        resource = attrs(group.get('resource', {}).get('attributes', []))
        service = ident(resource.get('service.name'), 'service.name')
        check_run(run, resource)
        for scope in group[scope_key]:
            scope_identity = canonical({'scope': scope.get('scope', {}), 'schemaUrl': scope.get('schemaUrl'),
                                        'resourceSchemaUrl': group.get('schemaUrl')})
            for item in scope[item_key]:
                a = attrs(item.get('attributes', []))
                check_run(run, a)
                if signal == 'traces':
                    start, end = ns(item['startTimeUnixNano']), ns(item['endTimeUnixNano'])
                    if end < start:
                        raise ValueError('negative span duration')
                    trace, span = hex_id(item['traceId'], 32), hex_id(item['spanId'], 16)
                    parent = item.get('parentSpanId')
                    parent = hex_id(parent, 16) if parent else None
                    kind = str(item.get('kind', 0)).replace('SPAN_KIND_', '')
                    kind = {'0':'UNSPECIFIED','1':'INTERNAL','2':'SERVER','3':'CLIENT','4':'PRODUCER','5':'CONSUMER'}.get(kind, kind)
                    if kind not in ('UNSPECIFIED','INTERNAL','SERVER','CLIENT','PRODUCER','CONSUMER'):
                        raise ValueError('unknown span kind')
                    status = str(item.get('status', {}).get('code', 0)).replace('STATUS_CODE_', '')
                    if status not in ('0','1','2','UNSET','OK','ERROR'):
                        raise ValueError('unknown span status')
                    error = status in ('2','ERROR')
                    # Some instrumentation only sets HTTP status or an error flag.
                    for key in ('http.status_code', 'http.response.status_code', 'rpc.grpc.status_code'):
                        if key in a:
                            v = a[key]
                            v = int(next(iter(v.values())) if isinstance(v, dict) else v)
                            error |= v != 0 if key.startswith('rpc.') else v >= 500
                    error |= a.get('error') in ('true', {'boolValue': True})
                    row = dict(trace_id=trace, span_id=span, parent_span_id=parent, service=service,
                               kind=kind, operation=str(item.get('name','')), start_ns=start, end_ns=end,
                               event_ns=end, duration_ms=(end-start)/1e6, error=bool(error))
                    key = [run, trace, span]
                    yield 'spans', finish(base, row, key)
                elif signal == 'logs':
                    event_id = ident(a.get('cct.event_id'), 'cct.event_id')
                    event = ns(item['timeUnixNano'])
                    observed = ns(item['observedTimeUnixNano']) if item.get('observedTimeUnixNano') else None
                    row = dict(service=service, pod_uid=resource.get('k8s.pod.uid'), event_id=event_id,
                               event_ns=event, observed_ns=observed, body=canonical(item.get('body', {})))
                    yield 'logs', finish(base, row, [run, event_id])
                else:
                    types = [t for t in ('gauge','sum','histogram','exponentialHistogram','summary') if t in item]
                    if len(types) != 1 or types[0] not in ('gauge','sum','histogram'):
                        raise ValueError('unsupported or ambiguous metric type')
                    typ = types[0]; block = item[typ]
                    temporality = str(block.get('aggregationTemporality','0')).replace('AGGREGATION_TEMPORALITY_', '')
                    temporality = {'0':'UNSPECIFIED','1':'DELTA','2':'CUMULATIVE'}.get(temporality, temporality)
                    if typ != 'gauge' and temporality not in ('DELTA','CUMULATIVE'):
                        raise ValueError('metric temporality required')
                    for point in block['dataPoints']:
                        pa = attrs(point.get('attributes', [])); check_run(run, pa)
                        stamp = ns(point['timeUnixNano'])
                        start = ns(point['startTimeUnixNano']) if point.get('startTimeUnixNano') else None
                        if start is not None and start > stamp:
                            raise ValueError('metric start after event time')
                        name = ident(item['name'], 'metric name')
                        series = digest([run, resource, scope_identity, name, item.get('unit',''), typ, pa])
                        value = None; count = None; total = None; bounds = None; buckets = None
                        # OTLP NO_RECORDED_VALUE flag: preserve missing point without fabricating zero.
                        missing = bool(int(point.get('flags',0)) & 1)
                        if not missing and typ == 'histogram':
                            count = ns(point['count']); total = number(point['sum']) if 'sum' in point else None
                            bounds = [number(x) for x in point.get('explicitBounds', [])]
                            buckets = [ns(x) for x in point.get('bucketCounts', [])]
                            if len(buckets) != len(bounds)+1 or sum(buckets) != count or any(x>=y for x,y in zip(bounds,bounds[1:])):
                                raise ValueError('invalid histogram buckets')
                        elif not missing:
                            value = number(point['asInt'] if 'asInt' in point else point['asDouble'])
                        row = dict(service=service, pod_uid=resource.get('k8s.pod.uid'), metric=name,
                                   unit=item.get('unit',''), metric_type=typ, temporality=temporality,
                                   monotonic=bool(block.get('isMonotonic',False)), series_id=series,
                                   resource_json=canonical(resource), scope_json=scope_identity,
                                   attributes_json=canonical(pa), start_ns=start, event_ns=stamp,
                                   value=value, count=count, sum_value=total, bounds=bounds, buckets=buckets,
                                   missing=missing)
                        yield 'metrics', finish(base, row, [series, start, stamp])


def finish(base, row, key):
    # Timestamp is a conservative lower bound on replay availability, not historical live delivery.
    observed = max(row['event_ns'], base['source_observed_ns'] or 0, row.get('observed_ns') or 0)
    return dict(base, **row, identity=digest(key), content_hash=digest([base['synthetic'], row]),
                available_ms=max(base['broker_ms'], (observed+999999)//1000000))


def trace_edges(rows):
    """Resolve nearest SERVER ancestor through CLIENT/INTERNAL spans; flag missing/cyclic links."""
    rows = list(rows)
    if len(rows) > 100000:
        raise ValueError('trace exceeds replay safety limit')
    lookup = {r['span_id']: r for r in rows}
    for r in rows:
        if r['kind'] != 'SERVER':
            continue
        cursor = r['parent_span_id']; seen = {r['span_id']}; path = [r]
        source = None; status = 'root'
        while cursor:
            if cursor in seen:
                status = 'cycle'; break
            seen.add(cursor)
            p = lookup.get(cursor)
            if p is None:
                status = 'missing_parent'; break
            path.append(p)
            if p['kind'] == 'SERVER':
                source = p['service']; status = 'linked'; break
            cursor = p['parent_span_id']
        yield dict(run_id=r['run_id'], synthetic=r['synthetic'], trace_id=r['trace_id'], span_id=r['span_id'],
                   source=source, target=r['service'], lineage_status=status, event_ns=r['event_ns'],
                   duration_ms=r['duration_ms'], error=r['error'], available_ms=max(x['available_ms'] for x in path))


def metric_deltas(rows):
    """Difference cumulative points only within a compatible series/epoch; no assumed zero baseline."""
    previous = None
    for r in sorted(rows, key=lambda x: (x['event_ns'], x['start_ns'] or 0)):
        value, count, total, buckets = None, None, None, None
        state = 'gauge' if r['metric_type']=='gauge' else 'unknown_baseline'
        available = r['available_ms']
        if r['missing']:
            state = 'missing'
        elif r['metric_type']=='gauge':
            value = r['value']
        elif r['temporality']=='DELTA':
            value, count, total, buckets = (r[k] for k in ('value','count','sum_value','buckets'))
            state = 'reported_delta'
        elif previous and not previous['missing']:
            compatible = (r['start_ns'] is not None and r['start_ns']==previous['start_ns']
                          and r['event_ns']>previous['event_ns'] and r['bounds']==previous['bounds']
                          and r['temporality']==previous['temporality'] and r['monotonic']==previous['monotonic'])
            if compatible:
                available = max(available, previous['available_ms'])
                if r['metric_type']=='sum':
                    delta = r['value']-previous['value']
                    if not r['monotonic'] or delta>=0:
                        value, state = delta, 'differenced'
                    else:
                        state = 'reset_or_invalid'
                else:
                    diffs = [a-b for a,b in zip(r['buckets'],previous['buckets'])]
                    if min(diffs)>=0 and r['count']>=previous['count']:
                        count, buckets, state = r['count']-previous['count'], diffs, 'differenced'
                        if r['sum_value'] is not None and previous['sum_value'] is not None:
                            total = r['sum_value']-previous['sum_value']
                    else:
                        state = 'reset_or_invalid'
            else:
                state = 'changed_epoch_or_schema'
        yield dict(run_id=r['run_id'], synthetic=r['synthetic'], service=r['service'], series_id=r['series_id'],
                   metric=r['metric'], unit=r['unit'], pod_uid=r['pod_uid'], event_ns=r['event_ns'],
                   interval_start_ns=(previous['event_ns'] if state=='differenced' else r['start_ns']),
                   available_ms=available, delta_state=state, value=value, count=count, sum_value=total,
                   bounds=r['bounds'], buckets=buckets)
        previous = r
