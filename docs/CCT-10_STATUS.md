# CCT-10 status — November 28, 2026

CCT-09 is complete. Owner-run coverage, healthy calibration, three bounded
fault/recovery trials, paired tracing measurement and recorded fault pipeline
replay now pass their technical checks. Teammate personal acceptance has not
been recorded. The current forecasting baselines did not demonstrate successful
advance warning in the separately scored final trial.

## Independent healthy capture and frozen thresholds

Campaign `cct-validation-20261128-03` completed healthy training run
`cct-probe-cct10-train-20261128b-01`: 1,950 browse/cart/checkout journeys,
5,850 successful HTTP operations and zero failures. Independent untraced proxy
start/end logs and separate request counters reconciled one-to-one with all
70,200 native SERVER calls. Full uncached review accepted 590 measured service
windows with fresh readiness and zero restart deltas. Each of the ten services
has **59 qualifying healthy five-second windows**, each with at least 20 requests.
The live observer separately recorded 58 timely verified frames.

Campaign `cct-validation-20261128-04` reused this healthy archive after another
full raw reconciliation, with unchanged training observations and threshold
values. Saved pre-install application templates match in all ten services. Fresh
validation/test IDs were reserved and the updated split frozen before their
faults. Across warm-up and measurement in the healthy run and three successful trials, **22,101 HTTP
operations succeeded, 265,212 SERVER calls reconciled, and 2,190 measured service
windows are complete**. See the [sanitized evidence](evidence/CCT-10-validation-20261128.json)
and [collection/reproduction protocol](CCT-10_VALIDATION.md).

Final verification confirms all 11 application pods healthy without reference
sidecars, exact original templates and Service ports restored, no fault resources
or reference ConfigMaps left, both Prometheus targets up, Tailscale online and
private Boutique HTTPS returning 200. Restoration recorded no errors.

The candidate twice-median-healthy-p95 rule was fitted separately to native and
independent duration channels and frozen before the reserved fault runs. Native
calibration digest: `8ae7ba3693fd4fd7e02488e4a72d18df8834b48748f04fa1d2789ca99f5b184e`;
independent digest: `8ab9d987145dd7a13d2d2dc3889484667bb2bc3f6accddffc013a2d2b1c72e32`.
These observations apply to the reference-augmented, grouped five-journey/second
validation workload. They do not revise historical incomplete pilot rows or
establish performance under other workloads. See the [validation protocol](CCT-10_VALIDATION.md).

The [matched tracing pair](CCT-04_PAIRED_OVERHEAD.md) also passed: both unchanged
five-user, 900-second measurements had zero failed requests. Runtime tracing-on
p95 was 32 ms versus 21 ms off. This single descriptive pair retains the
monitoring stack and instrumented images; it does not measure every collection
component or establish statistical significance.

## Three reserved fault/recovery trials

Each used one productcatalog pod's outgoing delay, 100 ms with 10 ms jitter for
30 seconds, after a fresh warm-up and healthy interval. Every trial completed
120 seconds after confirmed recovery, preserved pod identity/readiness/restarts,
and passed the independent full-archive reconciliation. Latencies below are
caller RPC medians in milliseconds; boundary-crossing calls are excluded.

| Run ID | HTTP operations / failures | Complete measured windows | Healthy → injected → recovered |
| --- | ---: | ---: | --- |
| `cct-probe-cct10-validation-20261128c-01` | 5,418 / 0 | 530 | 0.263 → 104.243 → 0.263 |
| `cct-probe-cct10-test-20261128c-01` | 5,430 / 0 | 540 | 0.268 → 106.002 → 0.261 |
| `cct-probe-cct10-test-20261128c-02` | 5,403 / 0 | 530 | 0.274 → 106.930 → 0.282 |

Controller Apply/Recover events retain one-second precision; request and polling
observations are separate. These are three repetitions of one scenario on one
host/target, rather than evidence for other fault types or targets.

Retrospective native-versus-independent health-label agreement has 46 matching
affected labels and 1,502 matching healthy labels, with no disagreements among
scorable labels. The other 52 service/windows remain unknown/censored. Thus
label precision/recall/F1 are 1.0 on 1,548 of 1,600 held-out service/windows;
this is measurement/rule agreement, **not prediction accuracy**.

In the six short pre-injection/post-recovery no-fault control intervals, the
native rule has no alarm starts. The independent rule has one control alarm in
the final pre-injection baseline. Unknown windows remain excluded from observed
control time. These short controls do not establish a stable false-alarm rate.

The original prospective scorer censors intermediate unknown labels, including
fully observed first breaches awaiting persistence. Its original results remain
unchanged. A [separate outcome protocol](evidence/CCT-10-outcome-protocol-v2-20261128.json)
distinguishes pending confirmation from missing measurements without changing
predictions, thresholds or onset times. Its plan and implementation were fixed
before the final trial; only that trial is held out for the supplementary
analysis. At the 30-second horizon all five baselines have mean F1 **0.0**, with
mean scorable coverage about **86.7%**. This result supports no positive
advance-warning claim. Empty cases, missing outcomes and censored predictions
remain explicitly reported.

[Failed attempts](evidence/CCT-10-failed-attempts-20261128.json) remain separate.
One stopped before cluster access, one before workload on idle accounting, and
one reserved baseline stopped before injection after an arrival backlog. The
last had no failed HTTP operations but several roughly four-second calls. It
was excluded from training and successful-repeat counts; its host-pressure
cause is unconfirmed. All affected sessions restored their original settings.

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
| Komalika, with Merul/Param | Three owner-run reserved repetitions passed effect, recovery and independent coverage checks; personal teammate acceptance is unrecorded. Controller precision and polling observations remain separate. |
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
