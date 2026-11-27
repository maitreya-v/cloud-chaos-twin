# Fault experiments · Komalika

The host's Chaos Mesh 2.8.8 installation and Komalika's initial bounded NetworkChaos permissions are ready. Runtime injection/recovery passed on an isolated canary. Follow the [setup record and pilot commands](../docs/CCT-07_CHAOS_SETUP.md). The three recorded Boutique trials and the experiment runner remain to be completed.

First output: one controlled fault with verified effect, observed start/end timestamps, cleanup, and recovery, repeated three times. `network-delay.example.yaml` is the admitted 30-second starting template. It has not been automatically applied to Boutique; calibrate and record its actual effect in the reserved test window.

Confirm Chaos Mesh permissions, container runtime/socket settings, target labels, and healthy workload first. The selector must match only the intended testbed pod. Inspect which traffic the chosen NetworkChaos settings affect and record that mechanism. Tune severity during the pilot rather than assuming 100 ms produces a cascade.

The experiment runner should record requested and actual timestamps separately, monitor the fault status, clean up on failure/interruption, and wait for recovery. Start with one fault at a time. Preserve no-impact and invalid runs with reasons.

Reference: [Network faults](https://chaos-mesh.org/docs/simulate-network-chaos-on-kubernetes/).
