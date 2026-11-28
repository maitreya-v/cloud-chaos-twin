# CCT-10 status — November 28, 2026

CCT-09 is complete. Owner-run independent coverage and healthy calibration now
pass for all ten services. Reserved fault validation is in progress; teammate
personal acceptance has not been recorded. No general forecasting-accuracy or
advance-warning claim is established.

## Independent healthy capture and frozen thresholds

Campaign `cct-validation-20261128-03` completed healthy training run
`cct-probe-cct10-train-20261128b-01`: 1,950 browse/cart/checkout journeys,
5,850 successful HTTP operations and zero failures. Independent untraced proxy
start/end logs and separate request counters reconciled one-to-one with all
70,200 native SERVER calls. Full uncached review accepted 590 measured service
windows with fresh readiness and zero restart deltas. Each of the ten services
has **59 qualifying healthy five-second windows**, each with at least 20 requests.
The live observer separately recorded 58 timely verified frames.

The candidate twice-median-healthy-p95 rule was fitted separately to native and
independent duration channels and frozen before the reserved fault runs. Native
calibration digest: `b41cbab8ed071da132b09aaba89964d88c764e14727a5c22d20959f1b4f568cd`;
independent digest: `afe19f2b198ca0733b87c37ca63b180d4c7aea42ab6c9c8890e3b80434ed6cf1`.
These observations apply to the reference-augmented, grouped five-journey/second
validation workload. They do not revise historical incomplete pilot rows or
establish performance under other workloads. See the [validation protocol](CCT-10_VALIDATION.md).

The [matched tracing pair](CCT-04_PAIRED_OVERHEAD.md) also passed: both unchanged
five-user, 900-second measurements had zero failed requests. Runtime tracing-on
p95 was 32 ms versus 21 ms off. This single descriptive pair retains the
monitoring stack and instrumented images; it does not measure every collection
component or establish statistical significance.

## Validated handoff

Merul supplied the [handoff runbook and collection/join tools](../CCT-10-Merul-Handoff-2026-11-26/README.md). Mithil's local validation reproduced all three derived JSON files from the measured source archive, matched all four source hashes, accepted the service-window schema, and reconciled requests/errors/p95 against CCT-09 evidence. The [validation receipt](evidence/CCT-10-handoff-validation.json) records the checks and hashes. The supplied JSON files remain local measured derivatives and can be regenerated with `from_receipt.py`; the supplied scripts and README are preserved unchanged with Merul's attribution.

The historical handoff contains ten service rows from one five-second interval of the existing November 25 shopping capture, with 1–13 observed requests per service. Every row has `complete=false`, `ready=null`, and `restart_delta=null`. Pod gauges were collected after that interval. That handoff has zero calibration-eligible windows and no fault/recovery manifest. It validates format and missing-data handling; it is not a sustained baseline or fault run.

## First controlled live pilot

Maitreya's owner-assisted run `cct-probe-cct10-20261127-02` completed the
[bounded pilot protocol](CCT-10_PILOT.md). The controller recorded Apply at
**2026-11-28 03:41:59 UTC** and Recover at **03:42:29 UTC** on one
`productcatalogservice` pod: outgoing delay 100 ms, jitter 10 ms, duration 30 s.
These controller timestamps have one-second precision; the evidence separately
retains injection request times and status observation times. The run ID uses
the owner's November 27 local date.

Across spawning, warm-up and measurement, all 447 workload operations succeeded.
The measured interval contained 298 operations. All 447 workload trace IDs were
archived, containing 6,492 SERVER spans across the whole capture and 4,396 in
the measured interval. Browse, cart and checkout root components each recovered
their expected 4/4, 2/2 and 9/9 relationships. Redirects remain separate frontend
request trees within their original traces. Both monitoring targets stayed up;
timestamped selected logs and raw Prometheus scrapes accompany the traces.

Productcatalog caller RPC median latency was 0.209 ms in the healthy interval,
108.468 ms during observed injection, and 0.212 ms after recovery. These are
descriptive measurements with interval-boundary exclusions, not forecasting
scores or calibrated health thresholds. Pods retained identity, readiness and
restart counts. The experiment and temporary workload were deleted, per-pod
network rules were empty, and shared HTTPS returned 200 after cleanup.

The unchanged handoff join produced **405 observed service windows with fresh
readiness and zero restart deltas**, but all remain `complete=false`: independent
backend request/duration accounting has not been reviewed. Most services also
remain below 20 requests per window. This pilot has **zero eligible calibration
windows**, and its original rows remain unchanged. Forty-eight unsuccessful Jaeger query attempts were retried; every
workload trace ID was eventually saved, with no missing parents or unlinked
spans in the retained snapshots. This does not independently prove coverage.

The [sanitized receipt](evidence/CCT-10-pilot-20261127.json) records counts,
timing, recovery, artifact hashes and limits. The earlier `...-01` attempt stopped
before injection when its log stream timed out; its separate receipt is retained
and confirms no fault or workload remained. One successful pilot does not close
the three-repeat CCT-07 criterion or the paired telemetry-overhead benchmark.

## Pipeline replay and acceptance boundaries

The [recorded fault pipeline replay](CCT-10_PIPELINE_REPLAY.md) now passes:
3,683 Kafka messages produced 6,492 spans, 1,006 metric points and 495 selected
logs. Restart and a full rebuild preserved all 11 tables. The 399 fully contained
measured windows reconcile with the pilot, have fresh readiness/zero restart
deltas, and retain `complete=false`. This closes the technical fault-data replay
step; it does not supply independent coverage or calibrated health thresholds.

The following table separates the new owner-run technical evidence from personal
teammate acceptance. Historical claims above describe their original capture dates.

| Owner | Evidence/status |
| --- | --- |
| Maitreya, Merul and Param | Owner-run independent coverage and fresh pod observations passed in the new healthy run; original event and actual availability times are retained. |
| Merul and Param | The new training run supplies 59 qualifying windows per service; older low-volume rows retain their original incomplete status. |
| Komalika, with Merul/Param | Three reserved repetitions of the bounded delay are in progress. Controller events retain one-second precision and polling observations remain separate. |
| Mithil and Merul | Owner-assisted rule review and training-only threshold freeze are recorded. Personal joint review is not recorded or implied. |

The owner-run pilot exercised the handoff's unchanged pod redaction and join
functions: 395 pod-list observations had a maximum adjacent read gap of 1,128 ms.
The standalone sampler and teammate identities still need their own live access
check. Coverage flags must be supported by independent evidence; post-run gauges
cannot be backfilled as observations of the earlier window.

Whole run IDs separate training, validation and test data. Calibrated thresholds
come from the independently checked healthy run; no fault metadata is a predictor
feature. General forecasting accuracy and live warning still require careful
assessment of the reserved-run results and their coverage.
