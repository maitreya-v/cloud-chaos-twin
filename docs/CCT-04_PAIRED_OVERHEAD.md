# Matched runtime tracing measurements — November 28, 2026

The owner completed one matched tracing-off/on pair using the unchanged CCT-02
five-user workload: seed 42, one user spawned per second, 120 seconds warm-up
and 900 seconds measured per run. Both runs passed the workload, stable-pod,
monitoring and node-headroom gates, with zero failed requests. Tracing was
restored and the frontend recovered. See the [receipt and source hashes](evidence/CCT-04-paired-overhead-20261128.json).

| Measurement | Tracing off | Tracing on | Observed change |
| --- | ---: | ---: | ---: |
| Successful requests | 996 | 1,000 | +4 |
| Request p95 | 21 ms | 32 ms | +11 ms (+52.38%) |
| Mean Linux VM CPU | 0.256 cores | 0.306 cores | +0.050 cores (+19.53%) |
| Peak Linux VM working set | 3,673,444,352 bytes | 3,856,375,808 bytes | +4.98% |
| Throughput | 1.1067 requests/s | 1.1111 requests/s | +0.40% |

The same instrumented ARM64 images and resource settings were used in both
runs. The off phase disabled runtime tracing and export in all ten application
services; it also removed the ads Java agent. Collector, Jaeger, Prometheus and
existing selected-log collection remained running. No reference proxies, Kafka,
Spark or fault experiments ran during this pair. The scope is runtime tracing,
export and derived spanmetrics, rather than the cost of the entire monitoring
stack or instrumented binaries. CPU/memory describe the Linux VM.

This is a single sequential pair with a closed-loop workload. It does not
establish statistical significance, long-term overhead, or an identical arrival
sequence. Keep the absolute 11 ms difference alongside its relative percentage.
An earlier attempt stopped before measurement on an old terminating pod; its
separate local receipt is retained and its results are excluded.

To reproduce on the prepared owner host, load `.local/env` and run
`python3 -m observability.paired`. The runner saves original settings, waits for
old pods to finish terminating before each measurement, aborts on unhealthy
conditions, and restores settings in its cleanup path. Keep the host awake and
online; a `STOP` file in the generated output directory requests cleanup.

The measured pair is under `outputs/overhead/20261128T043415Z/`; its two baseline
directories and a hashed local handoff archive are listed in the evidence.
Raw pod/configuration data stays outside Git.
