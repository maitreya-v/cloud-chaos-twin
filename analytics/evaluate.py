"""Score frozen predictions against a separate, finalized outcome channel."""
from statistics import mean

from .health import classify, latest, validate_windows
from ingestion.contract import digest


def control_alarms(windows, run_id, services, start_ms, end_ms, calibration):
    """Count incident starts during an independently declared healthy control interval."""
    width = calibration['width_ms']
    if start_ms % width or end_ms % width or start_ms >= end_ms:
        raise ValueError('control interval must be aligned and nonempty')
    validate_windows(windows, width)
    rows = [r for r in windows if r['run_id'] == run_id]
    previous, incidents, observed, unknown = 'healthy', 0, 0, 0
    for at in range(start_ms + width, end_ms + 1, width):
        states = classify(rows, run_id, services, at, calibration)
        current = ('affected' if 'affected' in states.values() else
                   'unknown' if 'unknown' in states.values() else 'healthy')
        if current == 'unknown':
            unknown += 1
        else:
            observed += 1
            if current == 'affected' and previous == 'healthy':
                incidents += 1
            previous = current
    hours = observed * width / 3600000
    return dict(run_id=run_id, start_ms=start_ms, end_ms=end_ms,
                alarm_incidents=incidents, has_false_alarm=bool(incidents),
                observed_hours=hours, unknown_windows=unknown,
                false_alarms_per_observed_hour=incidents / hours if hours else None)


def evaluate(prediction, outcome_rows, calibration, outcome_source, root_target=None, outcome_calibration=None):
    if not isinstance(outcome_source, str) or not outcome_source.strip():
        raise ValueError('identify the independently collected outcome source')
    if prediction['calibration_sha256'] != digest(calibration):
        raise ValueError('outcomes must use the frozen prediction calibration')
    if prediction['run_id'] not in calibration['split']['validation'] + calibration['split']['test']:
        raise ValueError('evaluation run is absent from validation/test partition')
    if root_target is not None and root_target not in prediction['services']:
        raise ValueError('root target is outside declared roster')
    width, cutoff = calibration['width_ms'], prediction['cutoff_ms']
    outcome_calibration=outcome_calibration or calibration
    if any(outcome_calibration[k]!=calibration[k] for k in ('split','width_ms','min_requests','min_windows','error_limit')):
        raise ValueError('independent outcome calibration must use the same frozen protocol and run split')
    validate_windows(outcome_rows, width)
    rows = [r for r in outcome_rows if r['run_id'] == prediction['run_id']]
    # Finalized outcomes may arrive later. Select the final revision, then evaluate event time.
    final = latest(rows, max((r['available_ms'] for r in rows), default=0))
    event_rows = [dict(r, available_ms=r['end_ms']) for r in final]
    baseline = classify(event_rows, prediction['run_id'], prediction['eligible'], cutoff, outcome_calibration)
    result = []
    for forecast in prediction['forecasts']:
        onset, censored = {}, set()
        for s in prediction['eligible']:
            if baseline[s] != 'healthy':
                censored.add(s)
                continue
            # Any unknown interval before onset prevents an exact new-onset label.
            for at in range((cutoff // width + 1) * width, cutoff + forecast['horizon_ms'] + 1, width):
                status = classify(event_rows, prediction['run_id'], [s], at, outcome_calibration)[s]
                if status == 'unknown':
                    censored.add(s)
                    break
                if status == 'affected':
                    onset[s] = at
                    break
        eligible = set(prediction['eligible']) - censored
        actual = set(onset)
        guessed = set(forecast['predicted_new']) & eligible
        tp, fp, fn = len(guessed & actual), len(guessed - actual), len(actual - guessed)
        empty = not (actual | guessed)
        precision = tp / (tp + fp) if tp + fp else (0.0 if actual else None)
        recall = tp / (tp + fn) if tp + fn else (0.0 if guessed else None)
        f1 = 2 * tp / (2 * tp + fp + fn) if not empty else None
        # Always report signed emission-relative timing, but retrospective replay is not live evidence.
        lead = {s: onset[s] - prediction['emitted_at_ms'] for s in sorted(guessed & actual)}
        result.append(dict(method=forecast['method'], horizon_ms=forecast['horizon_ms'],
                           eligible_count=len(prediction['eligible']), scorable_count=len(eligible),
                           coverage=len(eligible) / len(prediction['eligible']) if prediction['eligible'] else None,
                           censored=sorted(censored), actual_new=sorted(actual),
                           predicted_new=sorted(guessed), unscored_predictions=sorted(set(forecast['predicted_new']) & censored),
                           missed_impacts=sorted(actual - guessed), tp=tp, fp=fp, fn=fn,
                           precision=precision, recall=recall, f1=f1, empty_empty=empty,
                           jaccard=tp / len(actual | guessed) if actual | guessed else None,
                           affected_count_absolute_error=abs(len(guessed) - len(actual)),
                           affected_fraction_absolute_error=abs(len(guessed) - len(actual)) / len(prediction['services']),
                           impact_error_scope='already affected plus scorable cutoff-healthy services',
                           onset_ms=onset, signed_warning_ms=lead,
                           positive_warning_count=sum(v > 0 for v in lead.values()),
                           late_warning_count=sum(v <= 0 for v in lead.values()),
                           time_to_impact_mae_ms=None))
    roots = {}
    for method, ranking in prediction['roots'].items():
        rank = ranking.index(root_target) + 1 if root_target in ranking else None
        roots[method] = dict(target=root_target, rank=rank,
                             top1=int(rank == 1) if root_target else None,
                             top3=int(rank is not None and rank <= 3) if root_target else None,
                             reciprocal_rank=(1 / rank if rank else 0.0) if root_target else None)
    return dict(schema_version='1.0', run_id=prediction['run_id'], cutoff_ms=cutoff,
                synthetic=prediction['synthetic'], outcome_source=outcome_source,
                calibration_sha256=prediction['calibration_sha256'],
                outcome_calibration_sha256=digest(outcome_calibration),
                partition='test' if prediction['run_id'] in calibration['split']['test'] else 'validation',
                outcome_sha256=digest(sorted(final, key=lambda r: (r['service'], r['end_ms']))),
                historical_live_forecast_valid=False, roots=roots, forecasts=result)


def aggregate(evaluations):
    """Average cutoffs within a run first, so long runs do not dominate the result."""
    if not evaluations:
        raise ValueError('no evaluations to aggregate')
    if len({(e['synthetic'], e['partition'], e['calibration_sha256'],e.get('outcome_calibration_sha256',e['calibration_sha256'])) for e in evaluations}) != 1:
        raise ValueError('aggregate only matching provenance, partition and frozen calibration')
    groups, seen = {}, set()
    for e in evaluations:
        key = e['run_id'], e['cutoff_ms']
        if key in seen:
            raise ValueError('duplicate evaluation run/cutoff')
        seen.add(key)
        for f in e['forecasts']:
            groups.setdefault((f['method'], f['horizon_ms']), {}).setdefault(e['run_id'], []).append(f)
    results = []
    for (method, horizon), runs in sorted(groups.items()):
        entry = dict(method=method, horizon_ms=horizon, runs=len(runs),
                     empty_empty=sum(f['empty_empty'] for fs in runs.values() for f in fs),
                     censored_services=sum(len(f['censored']) for fs in runs.values() for f in fs))
        for metric in ('precision', 'recall', 'f1', 'jaccard', 'coverage', 'affected_count_absolute_error'):
            per_run = {run: mean(f[metric] for f in fs if f[metric] is not None)
                       for run, fs in runs.items() if any(f[metric] is not None for f in fs)}
            entry[metric + '_by_run'] = per_run
            entry[metric + '_run_mean'] = mean(per_run.values()) if per_run else None
        results.append(entry)
    return results
