"""Regression checks against false success in the live acceptance gate."""

import copy
import unittest

from scripts.baseline_boutique import evaluate


class BaselineGateTests(unittest.TestCase):
    def setUp(self):
        apps = ["adservice", "shippingservice", "checkoutservice", "currencyservice", "emailservice",
                "productcatalogservice", "frontend", "recommendationservice", "cartservice", "redis-cart", "paymentservice"]
        self.initial = {"node_capacity": {"memory": "1024000Ki", "cpu": "10"},
                        "pods": [{"namespace": "cct-app", "app": name, "name": name, "uid": name,
                                  "ready": True, "phase": "Running",
                                  "containers": [{"restarts": 0, "state": {}, "last_state": {}}]} for name in apps]}
        self.samples = [{**copy.deepcopy(self.initial), "sample_started_epoch": t,
                         "node_conditions": {"Ready": "True", "MemoryPressure": "False",
                                             "DiskPressure": "False", "PIDPressure": "False"},
                         "node_usage": {"memory": {"workingSetBytes": 500 * 1024**2},
                                        "cpu": {"usageNanoCores": 1000000000}}} for t in range(0, 901, 5)]
        self.measurement = {"measurement_started_epoch": 0, "measurement_ended_epoch": 900,
                            "measured_seconds": 900, "users": 5, "users_at_end": 5,
                            "requests": 1000, "failures": 0, "locust_errors": 0,
                            "endpoints": [{"method": m, "name": p, "requests": 10}
                                          for m, p in [("GET", "/"), ("GET", "/product/test"),
                                                       ("POST", "/cart"), ("POST", "/cart/checkout")]]}

    def result(self):
        return evaluate(self.measurement, self.samples, self.initial, 0)

    def test_complete_healthy_run_passes(self):
        self.assertTrue(self.result()["passed"])

    def test_unready_pod_and_replacement_fail_even_with_zero_http_errors(self):
        self.samples[30]["pods"][0]["ready"] = False
        self.assertFalse(self.result()["passed"])
        self.samples[30]["pods"][0]["ready"] = True
        self.samples[30]["pods"][0]["uid"] = "replacement"
        self.assertFalse(self.result()["passed"])

    def test_restart_fails(self):
        self.samples[50]["pods"][0]["containers"][0]["restarts"] = 1
        self.assertFalse(self.result()["passed"])

    def test_missing_samples_fail(self):
        self.samples = self.samples[:20] + self.samples[30:]
        self.assertFalse(self.result()["passed"])

    def test_short_window_or_request_failure_fails(self):
        self.measurement["measured_seconds"] = 899
        self.assertFalse(self.result()["passed"])
        self.measurement["measured_seconds"] = 900
        self.measurement["failures"] = 1
        self.assertFalse(self.result()["passed"])

    def test_timer_precision_tolerance_does_not_allow_a_short_run(self):
        self.measurement["measured_seconds"] = 899.998938286
        self.assertTrue(self.result()["passed"])
        self.measurement["measured_seconds"] = 899.98
        self.assertFalse(self.result()["passed"])

    def test_insufficient_memory_headroom_fails(self):
        self.samples[50]["node_usage"]["memory"]["workingSetBytes"] = 900 * 1024**2
        self.assertFalse(self.result()["passed"])


if __name__ == "__main__":
    unittest.main()
