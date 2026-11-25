# Workload · Maitreya

Start from Online Boutique's supplied Locust workload. Record user count, traffic mix, think time, offered/achieved rate, random seed, and run duration. Validate browsing, cart, and checkout before injecting faults. A constant virtual-user count does not ensure a constant arrival rate under slow responses.

The CCT-02 runner is implemented in `scripts/baseline_boutique.py` with the workload wrapper in `workload/baseline_locust.py`. It uses the pinned upstream task mix, five users, one user spawned per second, seed 42, and 1–10 second think times. After all users spawn, it warms up for 120 seconds and then measures a separate 900-second interval.

Run the shopping check first, then the baseline, following the [deployment record](../docs/CCT-02_LOCAL_DEPLOYMENT.md). Results include per-request events, endpoint counts/latency, achieved request rate, workload configuration, image versions, and sampled readiness/resources. Captures stay under `outputs/baselines/`, outside Git. The runner stops and removes its temporary load pod after preserving results; Boutique remains deployed with its continuous load generator disabled.

Do not send other traffic or change the cluster during a measured run. This initial low-load baseline establishes local health only. Add two calibrated workload levels after the pilot; assess headroom again when telemetry or other components are added.
