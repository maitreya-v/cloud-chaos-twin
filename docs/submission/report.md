# Cloud Chaos Twin

**AMS 560.01 / CSE 542.01 · Group 1 · Stony Brook University · Fall 2026**

Maitreya Vaghulade, Merul Shah, Param Shah, Komalika Acharya and Mithil Bavishi · November 28, 2026

## Abstract

When checkout slows down, an end-to-end timer tells the operator little about where the delay began or whether another service will become unhealthy. We instrumented Online Boutique, joined requests to service graphs and recorded bounded fault and recovery intervals. Three trials of a 100 ms catalog network delay passed the effect and recovery check. Native and independent health labels agreed on 1,548 scorable service-windows. The five forecasts all scored F1 0.0 at 30 seconds in the final supplementary held-out trial. The data support retrospective reconstruction. An improvement in advance warning remains unproven.

## Introduction and contributions

When a payment RPC slows down, both checkout and the storefront can accumulate latency. A request timer measures what the shopper experienced. Diagnosing the cause requires evidence about which services called one another and when their behavior changed. A forecast has a further requirement. It must reach the operator before the service it names becomes affected.

We contribute a trace verifier that preserves caller ancestry, a dataset with independently checked five-second health windows, and a saved-run demonstration linking the fault and its recovery to measured RPC latency. We compare graph and temporal forecasting baselines on information available at the original cutoff. The comparison exposes a practical limit: accurate retrospective labels can coexist with missed warnings.

---

# Tracing and early warning

## Background and related work

Dapper established distributed tracing as a way to follow individual requests through large services [3]. W3C Trace Context specifies how a request carries a trace identifier and parent relationship [4]. We use both properties to associate a controlled shopping request with its distributed trace. They cannot establish the health of a service that never emitted a span.

MicroRCA uses observed relationships to localize performance problems in microservices [5]. Seer uses patterns in tracing data to anticipate quality-of-service violations [6]. These systems motivate distinguishing retrospective localization from prospective warning. A shared benchmark would be needed for a quantitative comparison. The contribution here is the combination of request-linked measurements, explicit coverage checks and a reproducible evaluation protocol for one pinned Boutique deployment.

## Problem formulation and objectives

Define a directed graph with an edge from A to B when A calls B. Delay originating in B can propagate toward A. The experiment manifest identifies the intervention and its verified effective interval. That intervention supplies a root label. Independently defined health observations supply affected-service labels. Observability alone rarely establishes the exact internal causal path.

At cutoff t, a predictor receives only observations whose event and availability times are both at or before t. Its proposed outputs are a ranked list of possible roots and a set of services healthy at t that first become affected within a horizon H. Record the actual emission time to determine whether any useful warning remained. Exclude services with insufficient observations from the relevant denominator and report their number.

---

# From requests to a service graph

## System architecture and design rationale

| Stage | Execution path and rationale |
| --- | --- |
| Shopping workload | A fresh cookie jar isolates a synthetic shopper. Distinct W3C trace IDs identify each HTTP request. The verifier selects browse, add to cart, and checkout. |
| Distributed traces | Boutique sends OTLP to the Collector. Full trace data goes to Jaeger. A private ClusterIP and bounded in-memory backend fit a short experiment [7, 8]. |
| Service metrics | A second trace pipeline selects SERVER spans. The span-metrics connector computes call counts and duration histograms. Prometheus scrapes service and Collector metrics every five seconds [9]. |
| Selected logs and pod state | A Kubernetes reader uses the caller’s project permissions. It captures allowlisted frontend/checkout events and explicit pod readiness/restart gauges. It preserves event and observation times. |
| Analysis handoff | Optional Kafka exporters encode nested OTLP JSON. Param owns ingestion and Spark/Parquet replay. Mithil receives connected service edges. Komalika owns independent fault timing and recovery. |

## Algorithms and methods

**Reconstructing an RPC edge.** Suppose a frontend SERVER span f starts a CLIENT span c before it calls checkout. The checkout SERVER span s identifies c as its parent. Filtering out client spans before joining leaves s disconnected. Our verifier indexes all spans within the exact trace and follows CHILD_OF references to find the nearest server ancestor. It counts the call from frontend to checkout once. Resolved ancestors are cached, making traversal linear in spans and parent references before output sorting.

The verifier rejects cycles, contradictory duplicates and cross-trace parents. It checks reachability from a single frontend SERVER span and allows the known external parent supplied by the probe. A client call without a matching downstream SERVER span leaves the journey edge unverified. The declared whitelist covers specific journey paths. It does not cover every optional Boutique feature.

---

# Health labels and implementation

## Five-second health rules

Thresholds were fitted on a separate healthy run with 59 qualifying windows for each of ten services. Each channel uses twice its own median training-window p95. An unready pod or a positive restart delta is a hard failure. A latency breach or an error fraction of at least 5% requires persistence across two windows. Incomplete observations, low traffic and an unconfirmed first breach can produce an unknown label. The prospective evaluator retains actual first availability and decision-emission times.

## Source walkthrough

| Source | Execution path |
| --- | --- |
| observability/probe.py and lineage.py | Match each shopping request to its exact trace and walk span parents to obtain verified service pairs. |
| observability/coverage.py and live_windows.py | Reconcile native spans with independent proxy observations and counters. Publish a frame only when the evidence closes before the deadline. |
| ingestion/ and processing/ | Replay recorded OTLP and experiment metadata through Kafka. Spark normalizes events, builds windows and writes Parquet with preserved observation times. |
| analytics/health.py, forecast.py and evaluate.py | Fit thresholds on training only, rank eligible services at cutoff and score against a separate outcome channel. |
| analytics/saved_run.py | Join the published same-run journey checks, controller times, latency phases, health summaries and forecast scores into an offline demo. |

**Ownership.** Maitreya owns the testbed and deployment. Merul owns tracing, metrics, selected logs and telemetry verification. Param owns Kafka/Spark processing, Komalika owns controlled faults and recovery, and Mithil owns graph analysis and prediction. Merul and Mithil share responsibility for the health rules and forecast review.

---

# Experiment and reproduction

**Archived baseline.** Maitreya ran Online Boutique v0.10.7 on an Apple M5 host with 16 GiB, using a native linux/arm64 kind cluster with Kubernetes 1.36.4. Ten application microservices and Redis ran in the application namespace. The continuous load generator was disabled. A temporary workload pod ran five closed-loop users with seed 42, upstream weighted shopping tasks, a 120-second warm-up and a nominal 900-second measurement. Latency was measured inside the cluster [1].

The coverage campaign used twelve workers, seed 42 and five browse/cart/checkout journeys per second. Each five-second interval starts 25 journeys over 2.8 seconds and drains. Healthy measurement lasted 300 seconds after a 120-second warm-up. Each fault run had 120 seconds of healthy measurement, a 30-second delay with 10 ms jitter and at least 120 seconds after confirmed recovery. The untraced Envoy 1.40.2 channel supplied independent request counts, timings and outcomes. Versions for the application, observer and fault controller are pinned under configs/.

One healthy run trained the rules. One validation run and two original test runs used the same bounded catalog delay. The supplementary outcome rule was fixed before the final test. The first two fault runs were exploratory for that rule. The single tracing-cost pair uses five closed-loop users and retains the monitoring stack and instrumented binaries.

**Reproduction.** Use the Git revision containing this report and record git rev-parse HEAD. The versioned campaign inputs are frozen at 6499bd8cee286c6b485418a4f8091de2919de470 [10]. Run these commands from the repository root. The demo needs only Python 3.9+. The replay audit requires PyArrow.

```sh
python3 -m cct check
python3 -m unittest discover -s tests
python3 -m analytics.saved_run
python3 -m observability.fixture
python3 -m pip install -r processing/requirements-verify.txt
python3 -m observability.review_recorded
```

The saved-run command rebuilds the final trial shown on the next page. The fixture command reproduces the graph ablation. The replay audit recomputes the paired overhead values and compares the healthy source records with Parquet. The CCT-11 guide explains how to verify the private five-second timeline and how outputs differ across runs.

---

# Measured results

## Effect and recovery

| Catalog-delay trial | Healthy | Delay active | Recovered |
| --- | --- | --- | --- |
| Reserved validation | 0.263 | 104.243 | 0.263 |
| First test | 0.268 | 106.002 | 0.261 |
| Final test | 0.274 | 106.930 | 0.282 |

Table 1. Median caller RPC latency in milliseconds. Only calls wholly within each phase contribute. The final test includes 5,415, 1,350 and 5,400 calls in the three phases. Across healthy training and all three trials, the independent ledger reconciled 265,212 SERVER calls including warm-up. All 2,190 measured service-windows passed coverage checks [10].

## Health labels and warnings

Across the three reserved trials, native and independent rules agreed on 46 affected and 1,502 healthy service-windows. An additional 52 were unknown or censored. In the final supplementary test, each of five forecasting methods scored 0.0 mean F1 at 30 seconds with 86.7% mean scorable coverage. Of 53 decisions, 42 had empty predictions and outcomes under the no-additional baseline. The other 11 had a scorable new impact. The original conservative scoring policy censored pending first breaches and gave undefined F1 for this baseline [10].

## Tracing cost and graph ablation

In one matched pair, runtime tracing increased p95 from 21 to 32 ms (+52.38%). Throughput changed by +0.40%, mean VM CPU by +19.53%, and peak VM working set by +4.98%. Both runs had zero failed requests [11]. In the three synthetic journey fixtures, a direct SERVER-parent join recovered 0 of 15 expected links. Following all span parents recovered 15. The separate three live journey examples also verified 4 browse, 2 cart and 9 checkout relationships.

---

# Limits and next experiments

## Interpretation and threats to validity

Three repetitions on one ARM64 host and one fault type constrain generalization. The intervention delayed outgoing catalog traffic. Recorded caller latency and the controller confirm effect and recovery; they do not identify the exact causal path of every health label. Native service spans and the independent proxy time different boundaries. Both use the same persistence rule, which limits what retrospective agreement can establish.

Seven final-run frame ends had a retry or miss because requests were in flight; six fell within the observed delay. A retry can later succeed. Aggregate counts cannot attribute individual forecast errors. Overlapping horizons may count the same onset repeatedly. One independent-reference control alarm occurred before the last injection. Short controls cannot estimate a long-run false-alarm rate.

The off/on pair is sequential and has no replication. The pipeline also passed recorded fault-data replay: 3,683 Kafka messages, 11 Parquet tables and identical logical hashes on rebuild [12]. Replay elapsed time includes local processing and does not measure continuous end-to-end ingestion delay.

## Conclusion and next test

The shared testbed can show a measured fault, its verified request paths, the latency change and recovery in one saved run. The 30-second baselines missed the scorable onsets in the final test. A new prospective comparator warns when a still-healthy service reaches 80% of its latency or error limit, even if no graph neighbor is already affected. It awaits fresh runs and comparison on identical cutoffs. Proposed next tests vary the target and fault type, preserve missing evidence, and record lead time and false alarms alongside F1.

---

# References

1. [Group 1. CCT-02 measured baseline and testbed evidence](https://github.com/maitreya-v/cloud-chaos-twin/blob/df96c8c821fbc10dcbaad0ef518eb2b74334f664/docs/evidence/CCT-02-maitreya.json).

2. [Group 1. Frozen Cloud Chaos Twin telemetry implementation, tests and runbook](https://github.com/maitreya-v/cloud-chaos-twin/tree/cfab3d0ce1af576aaa3bbba72276322c8c361c66).

3. [Sigelman et al. *Dapper, a Large-Scale Distributed Systems Tracing Infrastructure*. Google, 2010](https://research.google/pubs/dapper-a-large-scale-distributed-systems-tracing-infrastructure/).

4. [W3C. *Trace Context*. Recommendation, 2021](https://www.w3.org/TR/trace-context/).

5. [Wu et al. *MicroRCA: Root Cause Localization of Performance Issues in Microservices*. NOMS, 2020](https://ieeexplore.ieee.org/document/9110353/).

6. [Gan et al. *Seer: Leveraging Big Data to Navigate the Complexity of Performance Debugging in Cloud Microservices*. ASPLOS, 2019](https://www.csl.cornell.edu/~delimitrou/papers/2019.asplos.seer.pdf).

7. [OpenTelemetry Protocol specification](https://opentelemetry.io/docs/specs/otlp/).

8. [Jaeger 1.76 deployment and storage](https://www.jaegertracing.io/docs/1.76/deployment/).

9. [Prometheus HTTP API](https://prometheus.io/docs/prometheus/latest/querying/api/).

10. [Group 1. Fixed campaign receipt, independent coverage, held-out evaluations and artifact hashes](https://github.com/maitreya-v/cloud-chaos-twin/blob/6499bd8cee286c6b485418a4f8091de2919de470/docs/evidence/CCT-10-validation-20261128.json).

11. [Group 1. Matched tracing overhead, measured summaries and limitations](https://github.com/maitreya-v/cloud-chaos-twin/blob/6499bd8cee286c6b485418a4f8091de2919de470/docs/CCT-04_PAIRED_OVERHEAD.md).

12. [Group 1. Recorded Kafka/Spark fault replay, rebuild checks and versions](https://github.com/maitreya-v/cloud-chaos-twin/blob/6499bd8cee286c6b485418a4f8091de2919de470/docs/CCT-10_PIPELINE_REPLAY.md).
