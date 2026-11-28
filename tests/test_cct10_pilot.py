import copy
import importlib.util
import unittest
from pathlib import Path
from unittest.mock import patch


spec = importlib.util.spec_from_file_location("pilot", Path(__file__).resolve().parents[1]/"scripts/cct10_pilot.py")
pilot = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pilot)


class PilotSafetyTests(unittest.TestCase):
    def test_streams_survive_beyond_the_short_read_timeout(self):
        args = pilot.stream_command(["logs","-f","test"])
        self.assertEqual([a for a in args if a.startswith("--request-timeout=")],
                         ["--request-timeout=0"])
        self.assertIn("--request-timeout=4s",pilot.KUBE)

    def test_repeated_poll_is_not_a_second_failed_scrape(self):
        previous = {}
        rows = [{"labels":{"job":job},"health":"up","lastScrape":"one"}
                for job in ("cct-service","cct-collector")]
        self.assertFalse(pilot.target_failures(rows,previous))
        rows[0].update(health="down",lastScrape="two")
        self.assertFalse(pilot.target_failures(rows,previous))
        self.assertFalse(pilot.target_failures(rows,previous))
        rows[0]["lastScrape"] = "three"
        self.assertTrue(pilot.target_failures(rows,previous))
        rows[0].update(health="up",lastScrape="four")
        self.assertFalse(pilot.target_failures(rows,previous))

    def test_missing_target_is_a_stop_condition(self):
        with self.assertRaises(RuntimeError):
            pilot.target_failures([], {})

    def test_cleanup_refuses_another_runs_fault(self):
        obj = {"metadata":{"annotations":{"cct.run_id":"another-run"}}}
        with patch.object(pilot,"kjson",return_value=obj), patch.object(pilot,"kube") as mutate:
            with self.assertRaisesRegex(RuntimeError,"another run"):
                pilot.owned_delete("networkchaos",pilot.FAULT,"our-run")
            mutate.assert_not_called()

    def test_cleanup_preserves_controller_finalizers(self):
        obj = {"metadata":{"annotations":{"cct.run_id":"our-run"},"finalizers":["controller"]}}
        with patch.object(pilot,"kjson",return_value=obj), patch.object(pilot,"kube") as mutate:
            pilot.owned_delete("networkchaos",pilot.FAULT,"our-run")
            self.assertEqual(mutate.call_args.args[0],
                             ["-n","cct-app","delete","networkchaos",pilot.FAULT,"--wait=false"])

    def test_workload_is_unprivileged_and_has_no_application_label(self):
        _, pod = pilot.manifests("cct-probe-check")
        self.assertNotIn("app",pod["metadata"]["labels"])
        self.assertFalse(pod["spec"]["automountServiceAccountToken"])
        self.assertEqual(pod["spec"]["activeDeadlineSeconds"],700)


if __name__ == "__main__":
    unittest.main()
