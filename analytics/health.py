"""Training-only calibration and reuse of Merul's CCT-10 health rule."""
from math import isfinite
from statistics import median

from ingestion.contract import digest
from observability.health import state


def validate_windows(rows, width_ms=5000):
    for r in rows:
        if (not r['run_id'] or not r['service'] or r['start_ms'] % width_ms
                or r['end_ms'] - r['start_ms'] != width_ms or r['available_ms'] < r['end_ms']):
            raise ValueError('invalid service-window identity or timestamps')
        for key in ('start_ms', 'end_ms', 'available_ms'):
            if type(r[key]) is not int or r[key] < 0:
                raise ValueError('window times must be nonnegative integer milliseconds')
        for key in ('requests', 'errors', 'restart_delta'):
            if r.get(key) is not None and (type(r[key]) is not int or r[key] < 0):
                raise ValueError('invalid count: ' + key)
        if r.get('p95_ms') is not None and (not isfinite(r['p95_ms']) or r['p95_ms'] < 0):
            raise ValueError('invalid p95')
        if r.get('errors') is not None and r.get('requests') is not None and r['errors'] > r['requests']:
            raise ValueError('errors exceed requests')
        if type(r.get('complete')) is not bool or r.get('ready') is not None and type(r['ready']) is not bool:
            raise ValueError('invalid coverage/readiness')
    return rows


def latest(rows, cutoff_ms):
    result = {}
    for r in rows:
        if r['end_ms'] > cutoff_ms or r['available_ms'] > cutoff_ms:
            continue
        key = r['run_id'], r['service'], r['end_ms']
        prev = result.get(key)
        if prev and prev['available_ms'] == r['available_ms'] and prev != r:
            raise ValueError('conflicting window revision')
        if prev is None or prev['available_ms'] < r['available_ms']:
            result[key] = r
    return list(result.values())


def validate_split(split):
    if set(split) != {'train', 'validation', 'test'} or any(not v for v in split.values()):
        raise ValueError('declare nonempty train, validation and test run lists')
    all_runs = [r for part in split.values() for r in part]
    if any(not isinstance(r, str) or not r for r in all_runs) or len(all_runs) != len(set(all_runs)):
        raise ValueError('whole-run partitions overlap or contain invalid IDs')


def fit(rows, split, healthy_intervals, min_windows=3, min_requests=20, width_ms=5000):
    validate_split(split)
    if min_windows < 1 or min_requests < 1 or width_ms < 1:
        raise ValueError('invalid calibration settings')
    validate_windows(rows, width_ms)
    if set(healthy_intervals) != set(split['train']):
        raise ValueError('declare a healthy interval for every training run only')
    for start, end in healthy_intervals.values():
        if type(start) is not int or type(end) is not int or start < 0 or start >= end:
            raise ValueError('invalid healthy training interval')
    samples, used = {}, []
    training = [r for r in rows if r['run_id'] in split['train']]
    for r in latest(training, max((r['available_ms'] for r in training), default=0)):
        start, end = healthy_intervals[r['run_id']]
        if (start <= r['start_ms'] and r['end_ms'] <= end and r['complete']
                and r['ready'] is True and r['restart_delta'] == 0
                and r['requests'] is not None and r['requests'] >= min_requests
                and r['errors'] is not None and r['errors'] / r['requests'] < 0.05
                and r['p95_ms'] is not None and r['p95_ms'] > 0):
            samples.setdefault(r['service'], []).append(r['p95_ms'])
            used.append(r)
    limits = {s: 2 * median(v) for s, v in sorted(samples.items()) if len(v) >= min_windows}
    if not limits:
        raise ValueError('no service has sufficient healthy training windows')
    return dict(schema_version='1.0', method='twice-median-healthy-p95', width_ms=width_ms,
                min_requests=min_requests, min_windows=min_windows, error_limit=0.05,
                latency_limits_ms=limits, sample_counts={s: len(v) for s, v in samples.items()},
                split=split, healthy_intervals=healthy_intervals,
                calibration_sha256=digest(sorted(used, key=lambda r: (r['run_id'], r['service'], r['end_ms']))))


def classify(rows, run_id, services, cutoff_ms, calibration):
    result = {}
    for service in services:
        limit = calibration['latency_limits_ms'].get(service)
        # Readiness/restarts remain usable even without a latency baseline.
        value = state(rows, run_id, service, cutoff_ms, latency_limit_ms=limit or 1e300,
                      error_limit=calibration['error_limit'], min_requests=calibration['min_requests'],
                      width_ms=calibration['width_ms'])
        if limit is None:
            end = cutoff_ms // calibration['width_ms'] * calibration['width_ms']
            hard = any(r['run_id'] == run_id and r['service'] == service and r['end_ms'] == end
                       and (r.get('ready') is False or (r.get('restart_delta') or 0) > 0)
                       for r in latest(rows, cutoff_ms))
            value = 'affected' if hard else 'unknown'
        result[service] = value
    return result
