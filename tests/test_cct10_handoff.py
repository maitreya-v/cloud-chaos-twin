"""Synthetic safety checks for Merul's supplied coverage/pod join; no live evidence."""
import importlib.util
from pathlib import Path
import unittest


SOURCE = Path(__file__).resolve().parents[1] / 'CCT-10-Merul-Handoff-2026-11-26/join_windows.py'
SPEC = importlib.util.spec_from_file_location('cct10_join', SOURCE)
JOIN = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(JOIN)


class HandoffTests(unittest.TestCase):
    def setUp(self):
        self.row = dict(run_id='synthetic-test', service='frontend', start_ms=5000, end_ms=10000,
                        available_ms=10000, requests=25, errors=0, p95_ms=10)
        self.samples = [dict(run_id='synthetic-test', started_ms=t, seen_ms=t+10,
                             services={'frontend': {'pod-a': dict(ready=True, restarts={'server': 0}, deleting=False)}})
                        for t in range(4000, 11000, 1000)]
        self.proof = dict(self.row, verified=True, records_reconciled=True, duration_count=25,
                          independent_source='synthetic-independent-probe', evidence_ref='fixture-only',
                          reviewer='test', available_ms=12000)

    def joined(self, proofs=()):
        return JOIN.join([self.row], self.samples, list(proofs), 'synthetic-test')[0]

    def test_pods_without_coverage_do_not_assert_complete(self):
        row = self.joined()
        self.assertTrue(row['ready'])
        self.assertEqual(row['restart_delta'], 0)
        self.assertFalse(row['complete'])
        self.assertEqual(row['available_ms'], 10010)

    def test_reviewed_matching_coverage_uses_review_availability(self):
        row = self.joined([self.proof])
        self.assertTrue(row['complete'])
        self.assertEqual(row['available_ms'], 12000)
        self.assertFalse(self.joined([dict(self.proof, requests=26)])['complete'])

    def test_missing_samples_and_replaced_pods_remain_unknown(self):
        original = self.samples
        self.samples = [s for s in original if s['started_ms'] not in (7000, 8000)]
        row = self.joined([self.proof])
        self.assertEqual((row['ready'], row['restart_delta'], row['complete']), (None, None, False))
        self.samples = original
        last = self.samples[-1]['services']['frontend']
        last['replacement-pod'] = last.pop('pod-a')
        self.assertFalse(self.joined([self.proof])['complete'])

    def test_restart_outside_window_is_not_attributed_to_window(self):
        for sample in self.samples[1:]:
            sample['services']['frontend']['pod-a']['restarts']['server'] = 1
        row = self.joined([self.proof])
        self.assertIsNone(row['restart_delta'])
        self.assertFalse(row['complete'])


if __name__ == '__main__':
    unittest.main()
