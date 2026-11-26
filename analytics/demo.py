"""Deterministic invented cascade with disjoint training, validation and test runs."""
from pathlib import Path

from .evaluate import aggregate, control_alarms, evaluate
from .forecast import predict
from .health import classify, fit
from .io import write_json
from .report import write_report


BASE = 1790000000000
SERVICES = ['frontend', 'checkoutservice', 'paymentservice']


def fixture():
    split = {'train': ['synthetic-train'], 'validation': ['synthetic-validation'], 'test': ['synthetic-test']}
    windows, spans = [], []
    for number, run in enumerate((r for rs in split.values() for r in rs), 1):
        for i, (service, kind, parent) in enumerate([
            ('frontend', 'SERVER', None), ('frontend', 'CLIENT', 1),
            ('checkoutservice', 'SERVER', 2), ('checkoutservice', 'CLIENT', 3),
            ('paymentservice', 'SERVER', 4)], 1):
            spans.append(dict(run_id=run, synthetic=True, trace_id=f'{number:032x}', span_id=f'{i:016x}',
                              parent_span_id=f'{parent:016x}' if parent else None, service=service, kind=kind,
                              start_ns=BASE * 1000000 + i * 1000,
                              end_ns=BASE * 1000000 + 1000000 - i * 1000,
                              duration_ms=(1000000 - i * 2000) / 1000000,
                              error=False, available_ms=BASE + 1000))
        for end in range(5000, 100001, 5000):
            for service in SERVICES:
                onset = {'paymentservice': 30000, 'checkoutservice': 40000, 'frontend': 50000}[service]
                affected = run == 'synthetic-test' and onset <= end < 80000
                windows.append(dict(run_id=run, service=service, start_ms=BASE + end - 5000,
                                    end_ms=BASE + end, available_ms=BASE + end,
                                    complete=True, ready=not affected, restart_delta=0,
                                    requests=30, errors=6 if affected else 0, p95_ms=80.0 if affected else 10.0))
    return spans, windows, split


def run(output):
    output = Path(output)
    spans, windows, split = fixture()
    calibration = fit(windows, split, {'synthetic-train': [BASE, BASE + 30000]})
    prediction = predict(spans, windows, 'synthetic-test', SERVICES, BASE + 30000, calibration)
    evaluation = evaluate(prediction, windows, calibration,
                          'synthetic independent-probe fixture; invented outcomes', 'paymentservice')
    timeline = [dict(end_ms=at, **classify(windows, 'synthetic-test', SERVICES, at, calibration))
                for at in range(BASE + 5000, BASE + 100001, 5000)]
    for name, value in [('spans', spans), ('windows', windows), ('outcomes', windows),
                        ('split', split), ('calibration', calibration), ('services', SERVICES),
                        ('healthy_intervals', {'synthetic-train': [BASE, BASE + 30000]}),
                        ('aggregate', aggregate([evaluation])),
                        ('control', control_alarms(windows, 'synthetic-validation', SERVICES,
                                                   BASE, BASE + 100000, calibration))]:
        write_json(output / (name + '.json'), value)
    result = write_report(output, prediction['graph'], prediction, evaluation, timeline)
    write_json(output / 'receipt.json', dict(schema_version='1.0', synthetic=True,
               run_id='synthetic-test', graph_nodes=len(prediction['graph']['nodes']),
               graph_edges=len(prediction['graph']['edges']), methods=5, horizons_ms=[10000, 30000, 60000],
               calibration_training_runs=calibration['split']['train'],
               historical_live_forecast_valid=False, report=str(result)))
    return result
