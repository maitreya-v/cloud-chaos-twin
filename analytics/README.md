# Analytics · Mithil

First deliverable: a healthy service dependency graph verified against real traces. The starter `cct/demo.py` defines a minimal graph JSON interface for a simplified normalized SERVER-span fixture; build an OTLP normalization stage before using real traces.

Next implement independently evaluated earliest-anomaly/largest-anomaly root-cause baselines, no-additional-impact and graph-distance cascade baselines, and then a temporal method. Record cutoff/emission time and exclude already affected services from the newly affected target. Split whole experiments before fitting preprocessing or thresholds.

Known injected origin does not establish the exact causal propagation path. Keep observed dependencies, plausible propagation, and future predictions separate in results.
