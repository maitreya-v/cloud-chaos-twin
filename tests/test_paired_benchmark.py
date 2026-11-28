import copy
import unittest
from unittest.mock import patch

from observability import paired


class PairedStabilityTests(unittest.TestCase):
    def test_a_ready_but_terminating_old_pod_cannot_start_measurement(self):
        new={"metadata":{"uid":"new"},"status":{"phase":"Running","conditions":[{"type":"Ready","status":"True"}]}}
        old=copy.deepcopy(new);old["metadata"].update(uid="old",deletionTimestamp="2026-11-28T00:00:00Z")
        rolling={"items":[old,new]};stable={"items":[new]}
        with patch.object(paired,"get",side_effect=[rolling,rolling,stable,stable]) as get,patch.object(paired.time,"sleep"):
            paired.quiescent()
        self.assertEqual(get.call_count,4)

    def test_trace_switch_does_not_mutate_the_restore_snapshot(self):
        original=[{"name":"ENABLE_TRACING","value":"1"},{"name":"JAVA_TOOL_OPTIONS","value":"-javaagent:agent.jar"}]
        saved=copy.deepcopy(original)
        off=paired.disabled(original,"adservice")
        self.assertEqual(original,saved)
        values={e["name"]:e["value"] for e in off}
        self.assertEqual(values["ENABLE_TRACING"],"0")
        self.assertEqual(values["DISABLE_TRACING"],"1")
        self.assertNotIn("JAVA_TOOL_OPTIONS",values)


if __name__=="__main__":unittest.main()
