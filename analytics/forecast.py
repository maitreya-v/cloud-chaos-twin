"""Cutoff-only root ranking and explicit, uncalibrated cascade baselines."""
from collections import defaultdict, deque
import time

from . import VERSION
from .graph import build_graph
from .health import classify, latest, validate_windows
from ingestion.contract import digest


def predict(spans, windows, run_id, services, cutoff_ms, calibration, horizons=(10000, 30000, 60000)):
    if run_id in calibration['split']['train']:
        raise ValueError('prediction run overlaps calibration training runs')
    if (type(cutoff_ms) is not int or cutoff_ms < 0 or not horizons
            or any(type(h) is not int or h <= 0 or h % calibration['width_ms'] for h in horizons)):
        raise ValueError('invalid cutoff or horizons')
    if not services or len(set(services)) != len(services):
        raise ValueError('declare a unique service roster')
    validate_windows(windows, calibration['width_ms'])
    history = [r for r in windows if r['run_id'] == run_id and r['available_ms'] <= cutoff_ms]
    rows = latest(history, cutoff_ms)
    graph = build_graph(spans, run_id, cutoff_ms)
    if not set(graph['nodes']) <= set(services):
        raise ValueError('graph contains services outside declared roster')
    states = classify(rows, run_id, services, cutoff_ms, calibration)
    affected = {s for s in services if states[s] == 'affected'}
    eligible = {s for s in services if states[s] == 'healthy'}
    end = cutoff_ms // calibration['width_ms'] * calibration['width_ms']
    indexed = {(r['service'], r['end_ms']): r for r in rows}
    first, anomaly, trend = {}, {}, {}
    for at in sorted({r['available_ms'] for r in history} | {cutoff_ms}):
        # Do not assign an onset earlier than that window's actual availability.
        observed = classify(history, run_id, services, at, calibration)
        for s in affected:
            if observed[s] == 'affected':
                first.setdefault(s, at)
    for s in services:
        r = indexed.get((s, end))
        limit = calibration['latency_limits_ms'].get(s)
        score = 0.0
        if r and states[s] != 'unknown':
            if limit and r.get('p95_ms') is not None:
                score = max(score, r['p95_ms'] / limit)
            if r.get('requests') and r.get('errors') is not None:
                score = max(score, r['errors'] / r['requests'] / calibration['error_limit'])
            if r.get('ready') is False or (r.get('restart_delta') or 0) > 0:
                score = max(score, 1.0)
        anomaly[s] = score
        previous = indexed.get((s, end - calibration['width_ms']))
        trend[s] = (max(0.0, (r['p95_ms'] - previous['p95_ms']) / limit)
                    if limit and r and previous and r.get('p95_ms') is not None
                    and previous.get('p95_ms') is not None and r['complete'] and previous['complete'] else 0.0)
    earliest = sorted(affected, key=lambda s: (first.get(s, cutoff_ms), s))
    largest = sorted(affected, key=lambda s: (-anomaly[s], s))
    reverse, totals = defaultdict(list), defaultdict(int)
    for edge in graph['edges']:
        totals[edge['source']] += edge['call_count']
    for edge in graph['edges']:
        reverse[edge['target']].append((edge['source'], edge['call_count'] / totals[edge['source']]))
    distance = {s: 0 for s in affected}
    weights = {s: 1.0 for s in affected}
    queue = deque(sorted(affected))
    while queue:
        target = queue.popleft()
        for caller, weight in reverse[target]:
            if caller not in distance:
                distance[caller] = distance[target] + 1
                weights[caller] = weights[target] * weight
                queue.append(caller)
            elif distance[caller] == distance[target] + 1:
                weights[caller] = max(weights[caller], weights[target] * weight)
    methods = {
        'no_additional': {},
        'reverse_one_hop': {s: 1.0 for s in eligible if distance.get(s) == 1},
        'reverse_reachable': {s: 1.0 for s in eligible if s in distance},
        'distance_weighted': {s: weights[s] / distance[s] for s in eligible if s in distance},
        # A predeclared edge-risk rule: recent symptom strength and positive latency trend.
        'temporal_risk': {s: weights[s] / distance[s] * (1 + anomaly[s] + trend[s])
                          for s in eligible if s in distance},
    }
    forecasts = []
    for horizon in sorted(set(horizons)):
        for method, scores in methods.items():
            threshold = 0.5 if method in ('distance_weighted', 'temporal_risk') else 0.0
            predicted = sorted(s for s, score in scores.items() if score >= threshold)
            forecasts.append(dict(method=method, horizon_ms=horizon, predicted_new=predicted,
                                  scores=dict(sorted(scores.items())), score_threshold=threshold,
                                  predicted_total_affected=len(affected) + len(predicted),
                                  predicted_total_fraction=(len(affected) + len(predicted)) / len(services),
                                  estimated_time_to_impact_ms=None))
    return dict(schema_version='1.0', method_version=VERSION, run_id=run_id,
                mode='offline-replay', historical_live_forecast_valid=False,
                synthetic=graph['synthetic'], cutoff_ms=cutoff_ms,
                emitted_at_ms=time.time_ns() // 1000000, services=sorted(services),
                calibration_sha256=digest(calibration), graph=graph,
                feature_sha256=digest(sorted(rows, key=lambda r: (r['service'], r['end_ms']))),
                states=states, eligible=sorted(eligible), already_affected=sorted(affected),
                unknown_at_cutoff=sorted(s for s in services if states[s] == 'unknown'),
                roots={'earliest_anomaly': earliest, 'largest_anomaly': largest},
                root_scores=anomaly, first_observed_affected_ms=first,
                forecasts=forecasts)
