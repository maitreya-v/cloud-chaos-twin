"""Observed service graphs from normalized spans, without cross-trace joins."""
from collections import Counter, defaultdict
from math import ceil, isfinite

from ingestion.contract import digest
from processing.normalize import trace_edges


def percentile(values, fraction=0.95):
    values = sorted(values)
    return values[max(0, ceil(len(values) * fraction) - 1)] if values else None


def build_graph(spans, run_id, cutoff_ms=None):
    """Count each downstream SERVER once; retain lineage gaps and deduplicate retries."""
    unique, duplicates = {}, 0
    for row in spans:
        if row['run_id'] != run_id:
            continue
        if (not isfinite(row['duration_ms']) or row['duration_ms'] < 0
                or row['end_ns'] < row['start_ns']
                or row['available_ms'] * 1000000 < row['end_ns']
                or type(row['synthetic']) is not bool):
            raise ValueError('invalid span timing or provenance')
        # Eligibility precedes deduplication: future conflicting records cannot affect a prediction.
        if cutoff_ms is not None and (row['available_ms'] > cutoff_ms or row['end_ns'] > cutoff_ms * 1000000):
            continue
        key = row['trace_id'], row['span_id']
        content = {k: row[k] for k in ('parent_span_id', 'service', 'kind', 'start_ns',
                                      'end_ns', 'duration_ms', 'error', 'synthetic')}
        content['operation'] = row.get('operation')
        if key in unique:
            previous = unique[key]
            if content != {k: previous.get(k) for k in content}:
                raise ValueError('conflicting span identity')
            duplicates += 1
            if previous['available_ms'] <= row['available_ms']:
                continue
        unique[key] = dict(row, event_ns=row['end_ns'])
    if not unique:
        raise ValueError('no eligible spans for run')
    if len({r['synthetic'] for r in unique.values()}) != 1:
        raise ValueError('mixed synthetic and measured spans')
    traces, services = defaultdict(list), defaultdict(list)
    for r in unique.values():
        traces[r['trace_id']].append(r)
        if r['kind'] == 'SERVER':
            services[r['service']].append(r)
    # Detect cycles across SERVER nodes too (the upstream nearest-server helper stops there).
    for rows in traces.values():
        parents = {r['span_id']: r['parent_span_id'] for r in rows}
        resolved = set()
        for span_id in parents:
            visiting, cursor = set(), span_id
            while cursor in parents and cursor not in resolved:
                if cursor in visiting:
                    raise ValueError('cycle in trace ancestry')
                visiting.add(cursor)
                cursor = parents[cursor]
            resolved.update(visiting)
    lineage = [r for rows in traces.values() for r in trace_edges(rows)]
    edge_groups = defaultdict(list)
    for row in lineage:
        if row['lineage_status'] == 'linked' and row['source'] != row['target']:
            edge_groups[row['source'], row['target']].append(row)

    def summary(rows):
        return dict(call_count=len(rows), errors=sum(r['error'] for r in rows),
                    error_fraction=sum(r['error'] for r in rows) / len(rows),
                    mean_ms=sum(r['duration_ms'] for r in rows) / len(rows),
                    p95_ms=percentile([r['duration_ms'] for r in rows]),
                    low_count=len(rows) < 20)

    graph = dict(schema_version='1.0', run_id=run_id,
                 synthetic=next(iter(unique.values()))['synthetic'], cutoff_ms=cutoff_ms,
                 edge_direction='caller_to_callee', nodes=sorted(services),
                 span_count=len(unique), duplicate_spans=duplicates, trace_count=len(traces),
                 lineage_counts=dict(Counter(r['lineage_status'] for r in lineage)),
                 missing_parent_spans=sum(r['lineage_status'] == 'missing_parent' for r in lineage),
                 edges=[dict(source=s, target=t, **summary(rows),
                             available_ms=max(r['available_ms'] for r in rows))
                        for (s, t), rows in sorted(edge_groups.items())],
                 service_metrics=[dict(service=s, **summary(rows)) for s, rows in sorted(services.items())])
    graph['input_sha256'] = digest(sorted(unique.values(), key=lambda r: (r['trace_id'], r['span_id'])))
    return graph
