# Fault experiments · Komalika

First output: one controlled fault with verified effect, observed start/end timestamps, cleanup, and recovery, repeated three times. `network-delay.example.yaml` is a scoped 30-second starting template, not a validated experiment or an automatically applied fault.

Confirm Chaos Mesh permissions, container runtime/socket settings, target labels, and healthy workload first. The selector must match only the intended testbed pod. Inspect which traffic the chosen NetworkChaos settings affect and record that mechanism. Tune severity during the pilot rather than assuming 100 ms produces a cascade.

The experiment runner should record requested and actual timestamps separately, monitor the fault status, clean up on failure/interruption, and wait for recovery. Start with one fault at a time. Preserve no-impact and invalid runs with reasons.

Reference: [Network faults](https://chaos-mesh.org/docs/simulate-network-chaos-on-kubernetes/).
