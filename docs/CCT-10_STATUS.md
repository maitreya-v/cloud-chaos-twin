# CCT-10 status — November 27, 2026

CCT-09 is complete. CCT-10's software and candidate protocol are implemented, but empirical calibration and joint acceptance remain pending.

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
remain below 20 requests per window. There are still **zero eligible calibration
windows**. Forty-eight unsuccessful Jaeger query attempts were retried; every
workload trace ID was eventually saved, with no missing parents or unlinked
spans in the retained snapshots. This does not independently prove coverage.

The [sanitized receipt](evidence/CCT-10-pilot-20261127.json) records counts,
timing, recovery, artifact hashes and limits. The earlier `...-01` attempt stopped
before injection when its log stream timed out; its separate receipt is retained
and confirms no fault or workload remained. One successful pilot does not close
the three-repeat CCT-07 criterion or the paired telemetry-overhead benchmark.

## Remaining dependencies

| Owner | Required evidence |
| --- | --- |
| Maitreya, Merul and Param | A coordinated sustained healthy capture with a declared interval, common run ID/roster, scoped traces, fresh readiness/restart observations and independently reviewed per-window coverage. |
| Merul and Param | Under the current defaults, at least three eligible five-second windows per service with at least 20 requests in each. Low-volume services remain uncalibrated. Keep original event and availability timestamps. |
| Komalika, with Merul/Param | Independently review this pilot's target, controller/observation timing uncertainty and recovery evidence; plan subsequent labeled runs with separate IDs. |
| Mithil and Merul | Review the fitted thresholds, insufficient-volume services, coverage and timing; freeze the agreed rule before reserved-run evaluation. |

The owner-run pilot exercised the handoff's unchanged pod redaction and join
functions: 395 pod-list observations had a maximum adjacent read gap of 1,128 ms.
The standalone sampler and teammate identities still need their own live access
check. Coverage flags must be supported by independent evidence; post-run gauges
cannot be backfilled as observations of the earlier window.

Use separate experiment run IDs for training, validation and test data. No real thresholds or fault labels have been invented, and no forecasting-accuracy or live-warning claim is supported yet.
