"""Ensure the telemetry review fails on silent corruption of a replayed capture."""
import unittest

from observability.review_recorded import check_replay_table


class RecordedReplayReviewTests(unittest.TestCase):
    def setUp(self):
        self.source = dict(identity="sample", service="checkoutservice",
                           duration_ms=2.4, broker_ms=1, available_ms=5100)
        self.replay = dict(self.source, broker_ms=12000, available_ms=12000)

    def test_uses_later_of_source_and_broker_time(self):
        check_replay_table([self.source], [self.replay])
        check_replay_table([self.source], [dict(self.source, broker_ms=5000)])
        for bad_time in (5100, 11999, 12001):
            with self.subTest(bad_time=bad_time), self.assertRaisesRegex(ValueError, "availability"):
                check_replay_table([self.source], [dict(self.replay, available_ms=bad_time)])

    def test_rejects_changed_payload_even_when_identity_matches(self):
        with self.assertRaisesRegex(ValueError, "normalized source record"):
            check_replay_table([self.source], [dict(self.replay, duration_ms=2.5)])

    def test_rejects_missing_duplicate_and_substituted_records(self):
        for replay in ([], [self.replay, self.replay], [dict(self.replay, identity="other")]):
            with self.subTest(replay=replay), self.assertRaises(ValueError):
                check_replay_table([self.source], replay)
