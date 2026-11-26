"""Local-only adapters for the existing pipeline and measured shopping receipts."""
import json
from pathlib import Path

from ingestion.contract import TOPICS, envelope
from observability.signals import nanos
from processing.normalize import normalize
from .graph import build_graph


def read_json(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + '\n', encoding='utf-8')


def table(path):
    path = Path(path)
    if path.is_dir() or path.suffix == '.parquet':
        try:
            import pyarrow.parquet as pq
        except ImportError as exc:
            raise ValueError('Parquet input needs pyarrow; JSON input uses only the standard library') from exc
        return pq.read_table(path).to_pylist()
    if path.suffix == '.jsonl':
        return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    value = read_json(path)
    if not isinstance(value, list):
        raise ValueError('expected a JSON array, JSONL records or a Parquet table')
    return value


def archive_spans(directory):
    """Use source-observation time as conservative availability; never invent broker receipts."""
    directory = Path(directory)
    meta = read_json(directory / 'archive.json')
    payload = read_json(directory / 'traces.json')
    observed = meta['observed_ns']
    rows = [r for _, r in normalize(envelope(meta['run_id'], 'traces', payload,
                                            meta['synthetic'], observed),
                                   (observed + 999999) // 1000000, TOPICS['traces'])]
    for row in rows:
        row['broker_ms'] = None
    return rows, meta


def receipt_graph(path):
    """Reconstruct and reconcile every saved core journey from its server projection."""
    from observability.lineage import EXPECTED
    receipt = read_json(path)
    rows, checks, journeys = [], [], set()
    for request in receipt['requests']:
        journey = request['journey']
        if journey not in EXPECTED:
            continue
        if journey in journeys:
            raise ValueError('duplicate core journey')
        journeys.add(journey)
        available = (nanos(request['observed_at'].replace('+00:00', 'Z')) + 999999) // 1000000
        projected = []
        for r in request['lineage']['server_records']:
            if r['trace_id'] != request['trace_id']:
                raise ValueError('receipt trace identity mismatch')
            projected.append(dict(run_id=receipt['run_id'], synthetic=False, trace_id=r['trace_id'],
                                  span_id=r['span_id'], parent_span_id=r['parent_server_id'],
                                  service=r['service'], operation=r['operation'], kind='SERVER',
                                  start_ns=r['start_us'] * 1000,
                                  end_ns=(r['start_us'] + r['duration_us']) * 1000,
                                  duration_ms=r['duration_us'] / 1000, error=r['error'], available_ms=available))
        graph = build_graph(projected, receipt['run_id'])
        found = {(e['source'], e['target']): e['call_count'] for e in graph['edges']}
        declared = {(e['source'], e['target']): e['calls'] for e in request['lineage']['edges']}
        required = {(s, t) for s, targets in EXPECTED[journey].items() for t in targets}
        passed = (found == declared and required <= set(found) and not graph['missing_parent_spans']
                  and graph['lineage_counts'].get('root') == 1
                  and request['coverage']['passes_declared_path'] is True)
        checks.append(dict(journey=journey, expected=len(required), matched=len(required & set(found)),
                           matches_receipt=found == declared, passed=passed))
        if not passed:
            raise ValueError('receipt graph reconciliation failed: ' + journey)
        rows.extend(projected)
    if journeys != set(EXPECTED):
        raise ValueError('receipt must include browse, cart and checkout')
    graph = build_graph(rows, receipt['run_id'])
    graph.update(journey_checks=checks, representation='recorded nearest-SERVER projection',
                 historical_live_forecast_valid=False)
    return graph
