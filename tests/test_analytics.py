import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from analytics.demo import BASE, SERVICES, fixture, run
from analytics.evaluate import aggregate, control_alarms, evaluate
from analytics.benchmark import benchmark
from analytics.forecast import predict
from analytics.graph import build_graph
from analytics.health import classify, fit, latest, validate_split
from analytics.io import receipt_graph, table
from ingestion.contract import TOPICS, envelope
from observability.export_sample import sample
from processing.normalize import normalize


class AnalyticsTests(unittest.TestCase):
    def setUp(self):
        self.spans, self.windows, self.split = fixture()
        self.calibration = fit(self.windows, self.split, {'synthetic-train': [BASE, BASE + 30000]})

    def prediction(self, spans=None, windows=None):
        with patch('analytics.forecast.time.time_ns', return_value=(BASE + 31000) * 1000000):
            return predict(self.spans if spans is None else spans,
                           self.windows if windows is None else windows,
                           'synthetic-test', SERVICES, BASE + 30000, self.calibration)

    def test_graph_walks_client_spans_and_deduplicates(self):
        graph = build_graph(self.spans + self.spans, 'synthetic-test', BASE + 30000)
        self.assertEqual(graph['duplicate_spans'], 5)
        self.assertEqual([(r['source'], r['target'], r['call_count']) for r in graph['edges']],
                         [('checkoutservice', 'paymentservice', 1), ('frontend', 'checkoutservice', 1)])
        self.assertEqual(sum(r['call_count'] for r in graph['service_metrics']), 3)

    def test_nested_otlp_pipeline_contract(self):
        e = envelope('fixture-checkout', 'traces', sample()['traces'], True)
        rows = [r for _, r in normalize(e, BASE + 2000, TOPICS['traces'])]
        graph = build_graph(rows, 'fixture-checkout')
        self.assertEqual((graph['span_count'], len(graph['edges'])), (19, 9))

    def test_graph_missing_parent_conflict_and_server_cycle(self):
        spans = copy.deepcopy(self.spans)
        spans[-1]['parent_span_id'] = 'f' * 16
        self.assertEqual(build_graph(spans, 'synthetic-test')['missing_parent_spans'], 1)
        with self.assertRaisesRegex(ValueError, 'conflicting'):
            build_graph(self.spans + [dict(self.spans[-1], service='other')], 'synthetic-test')
        spans = copy.deepcopy(self.spans)
        spans[-5]['parent_span_id'] = spans[-1]['span_id']
        with self.assertRaisesRegex(ValueError, 'cycle'):
            build_graph(spans, 'synthetic-test')

    def test_calibration_uses_training_intervals_only(self):
        altered = [dict(r, p95_ms=99999) if r['run_id'] != 'synthetic-train' or r['end_ms'] > BASE + 30000 else r
                   for r in self.windows]
        fitted = fit(altered, self.split, {'synthetic-train': [BASE, BASE + 30000]})
        self.assertEqual(fitted, self.calibration)
        self.assertEqual(set(fitted['latency_limits_ms'].values()), {20.0})
        bad = dict(self.split, test=['synthetic-train'])
        with self.assertRaises(ValueError):
            validate_split(bad)

    def test_cutoff_predictions_ignore_future_and_late_records(self):
        original = self.prediction()
        altered = [dict(r, p95_ms=9999, ready=False) if r['end_ms'] > BASE + 30000 else r for r in self.windows]
        late = dict(next(r for r in self.windows if r['run_id'] == 'synthetic-test' and r['service'] == 'frontend'
                         and r['end_ms'] == BASE + 30000), available_ms=BASE + 31000, ready=False)
        future_edge = dict(self.spans[-1], span_id='f' * 16, available_ms=BASE + 31000)
        self.assertEqual(original, self.prediction(self.spans + [future_edge], altered + [late]))

    def test_prediction_direction_roots_and_new_eligibility(self):
        p = self.prediction()
        self.assertEqual(p['already_affected'], ['paymentservice'])
        self.assertEqual(p['roots']['earliest_anomaly'], ['paymentservice'])
        methods = {f['method']: f['predicted_new'] for f in p['forecasts']}
        self.assertEqual(methods['no_additional'], [])
        self.assertEqual(methods['reverse_one_hop'], ['checkoutservice'])
        self.assertEqual(methods['reverse_reachable'], ['checkoutservice', 'frontend'])
        self.assertTrue(all('paymentservice' not in f['predicted_new'] for f in p['forecasts']))

    def test_incomplete_pipeline_windows_remain_unknown(self):
        rows = [dict(r, complete=False, ready=None, restart_delta=None) for r in self.windows]
        p = self.prediction(windows=rows)
        self.assertEqual(p['eligible'], [])
        self.assertEqual(len(p['unknown_at_cutoff']), 3)
        self.assertTrue(all(not f['predicted_new'] for f in p['forecasts']))

    def test_uncalibrated_services_only_allow_hard_failure(self):
        c = dict(self.calibration, latency_limits_ms={})
        states = classify(self.windows, 'synthetic-test', SERVICES, BASE + 30000, c)
        self.assertEqual(states, {'frontend': 'unknown', 'checkoutservice': 'unknown', 'paymentservice': 'affected'})

    def test_revision_conflicts_and_persistence(self):
        row = next(r for r in self.windows if r['run_id'] == 'synthetic-test' and r['end_ms'] == BASE + 25000)
        with self.assertRaises(ValueError):
            latest([row, dict(row, p95_ms=55)], BASE + 30000)
        rows = [dict(r, p95_ms=50) if r['service'] == 'frontend' and r['end_ms'] >= BASE + 25000 else r for r in self.windows]
        self.assertEqual(classify(rows, 'synthetic-test', ['frontend'], BASE + 25000, self.calibration)['frontend'], 'unknown')
        self.assertEqual(classify(rows, 'synthetic-test', ['frontend'], BASE + 30000, self.calibration)['frontend'], 'affected')

    def test_scores_and_signed_warning(self):
        e = evaluate(self.prediction(), self.windows, self.calibration, 'invented-probes', 'paymentservice')
        one = next(f for f in e['forecasts'] if f['method'] == 'reverse_one_hop' and f['horizon_ms'] == 10000)
        self.assertEqual((one['tp'], one['fp'], one['fn'], one['f1']), (1, 0, 0, 1.0))
        self.assertEqual(one['signed_warning_ms'], {'checkoutservice': 9000})
        self.assertFalse(e['historical_live_forecast_valid'])
        self.assertEqual(e['roots']['largest_anomaly']['reciprocal_rank'], 1.0)
        none = next(f for f in e['forecasts'] if f['method'] == 'no_additional' and f['horizon_ms'] == 30000)
        self.assertEqual((none['fn'], none['f1']), (2, 0.0))

    def test_missing_future_and_baseline_outcomes_are_censored(self):
        rows = [r for r in self.windows if not (r['run_id'] == 'synthetic-test' and r['service'] == 'frontend'
                                                and r['end_ms'] == BASE + 35000)]
        e = evaluate(self.prediction(), rows, self.calibration, 'invented-probes')
        f = next(f for f in e['forecasts'] if f['method'] == 'reverse_reachable' and f['horizon_ms'] == 30000)
        self.assertEqual(f['censored'], ['frontend'])
        self.assertEqual(f['unscored_predictions'], ['frontend'])
        self.assertEqual(f['coverage'], 0.5)

    def test_empty_sets_and_run_weighting(self):
        p = self.prediction()
        rows = [dict(r, ready=True, errors=0, p95_ms=10) for r in self.windows]
        e = evaluate(p, rows, self.calibration, 'synthetic-no-impact')
        f = next(f for f in e['forecasts'] if f['method'] == 'no_additional')
        self.assertTrue(f['empty_empty'])
        self.assertIsNone(f['f1'])
        self.assertEqual(len(aggregate([e])), 15)
        with self.assertRaises(ValueError):
            aggregate([e, e])

    def test_outcome_calibration_mismatch_rejected(self):
        with self.assertRaisesRegex(ValueError, 'frozen'):
            evaluate(self.prediction(), self.windows, dict(self.calibration, error_limit=0.1), 'probes')

    def test_independent_latency_boundary_uses_its_own_frozen_thresholds(self):
        prediction=self.prediction()
        outcomes=[dict(r,ready=True,errors=0) for r in self.windows]
        reference=copy.deepcopy(self.calibration)
        reference['latency_limits_ms']={s:1000.0 for s in SERVICES}
        result=evaluate(prediction,outcomes,self.calibration,'synthetic proxy latency',outcome_calibration=reference)
        one=next(f for f in result['forecasts'] if f['method']=='reverse_one_hop' and f['horizon_ms']==10000)
        self.assertEqual((one['tp'],one['fp'],one['fn']),(0,1,0))
        self.assertNotEqual(result['calibration_sha256'],result['outcome_calibration_sha256'])
        altered=copy.deepcopy(reference);altered['split']['test']=['other-test']
        with self.assertRaisesRegex(ValueError,'same frozen protocol'):
            evaluate(prediction,outcomes,self.calibration,'synthetic proxy latency',outcome_calibration=altered)

    def test_mixed_independent_outcome_calibrations_are_not_aggregated(self):
        first=evaluate(self.prediction(),self.windows,self.calibration,'synthetic probes')
        other=copy.deepcopy(first);other['cutoff_ms']+=5000;other['outcome_calibration_sha256']='different-reference'
        with self.assertRaisesRegex(ValueError,'matching'):
            aggregate([first,other])

    def test_observed_pending_does_not_backdate_confirmed_onset_or_hide_real_gaps(self):
        rows=[dict(r,ready=True,errors=0,p95_ms=50 if r['service']=='checkoutservice' and r['end_ms']>=BASE+35000 else 10) for r in self.windows]
        def score(data,pending):
            result=evaluate(self.prediction(),data,self.calibration,'synthetic observed latency',allow_observed_pending=pending)
            return result,next(f for f in result['forecasts'] if f['method']=='reverse_one_hop' and f['horizon_ms']==10000)
        strict,old=score(rows,False);supplement,new=score(rows,True)
        self.assertIn('checkoutservice',old['censored'])
        self.assertEqual(new['onset_ms'],{'checkoutservice':BASE+40000})
        self.assertEqual(new['signed_warning_ms'],{'checkoutservice':9000})
        self.assertEqual((new['tp'],new['fp'],new['fn']),(1,0,0))
        gap=[r for r in rows if not (r['service']=='checkoutservice' and r['run_id']=='synthetic-test' and r['end_ms']==BASE+35000)]
        self.assertIn('checkoutservice',score(gap,True)[1]['censored'])
        low=[dict(r,requests=19) if r['service']=='checkoutservice' and r['end_ms']==BASE+35000 else r for r in rows]
        self.assertIn('checkoutservice',score(low,True)[1]['censored'])
        supplement['cutoff_ms']+=1
        with self.assertRaisesRegex(ValueError,'matching'):aggregate([strict,supplement])

    def test_aggregation_weights_runs_and_rejects_mixed_evidence(self):
        e = evaluate(self.prediction(), self.windows, self.calibration, 'synthetic-probes')
        second_cutoff = copy.deepcopy(e)
        second_cutoff['cutoff_ms'] += 5000
        second_run = copy.deepcopy(e)
        second_run['run_id'] = 'another-test-run'
        for f in second_run['forecasts']:
            f['f1'] = 0.0
        results = aggregate([e, second_cutoff, second_run])
        one = next(r for r in results if r['method'] == 'reverse_one_hop' and r['horizon_ms'] == 10000)
        self.assertEqual(one['f1_run_mean'], 0.5)
        for override in ({'synthetic': False}, {'partition': 'validation'}, {'calibration_sha256': 'different'}):
            with self.assertRaisesRegex(ValueError, 'matching'):
                aggregate([e, dict(second_run, **override)])

    def test_control_alarms_and_graph_scaling(self):
        result = control_alarms(self.windows, 'synthetic-validation', SERVICES, BASE, BASE + 100000, self.calibration)
        self.assertEqual(result['alarm_incidents'], 0)
        self.assertEqual(result['unknown_windows'], 0)
        changed = [dict(r, ready=False) if r['service'] == 'frontend' and BASE + 20000 <= r['end_ms'] <= BASE + 30000 else r
                   for r in self.windows]
        result = control_alarms(changed, 'synthetic-validation', SERVICES, BASE, BASE + 100000, self.calibration)
        self.assertEqual(result['alarm_incidents'], 1)
        self.assertEqual(benchmark([10], 1)['results'][0]['edges'], 9)

    def test_demo_artifacts_and_json_reader(self):
        with tempfile.TemporaryDirectory() as tmp:
            report = run(tmp)
            self.assertIn('SYNTHETIC FIXTURE', report.read_text())
            self.assertEqual(len(table(Path(tmp) / 'spans.json')), 15)
            self.assertEqual(json.loads((Path(tmp) / 'receipt.json').read_text())['graph_edges'], 2)

    def test_real_receipt_projection_contract(self):
        from observability.fixture import PATHS, fixture as journey_fixture
        from observability.lineage import inspect_trace
        requests = []
        for i, journey in enumerate(PATHS, 1):
            trace = journey_fixture(journey, i)
            for span in trace['spans']:
                span.update(operationName='test-only', startTime=BASE * 1000, duration=10)
            requests.append(dict(journey=journey, trace_id=trace['traceID'],
                                 observed_at='2026-11-25T05:21:55.162467+00:00',
                                 coverage={'passes_declared_path': True},
                                 lineage=inspect_trace(trace, trace['traceID'], include_records=True)))
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'journeys.json'
            path.write_text(json.dumps(dict(run_id='adapter-test-only', requests=requests)))
            graph = receipt_graph(path)
            self.assertEqual(sum(c['matched'] for c in graph['journey_checks']), 15)
            requests[0]['lineage']['edges'][0]['calls'] += 1
            path.write_text(json.dumps(dict(run_id='adapter-test-only', requests=requests)))
            with self.assertRaisesRegex(ValueError, 'reconciliation'):
                receipt_graph(path)


if __name__ == '__main__':
    unittest.main()
