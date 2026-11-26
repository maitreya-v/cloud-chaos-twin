# CCT-10 status — November 26, 2026

CCT-09 is complete. CCT-10's software and candidate protocol are implemented, but empirical calibration and joint acceptance remain pending.

## Validated handoff

Merul supplied the [handoff runbook and collection/join tools](../CCT-10-Merul-Handoff-2026-11-26/README.md). Mithil's local validation reproduced all three derived JSON files from the measured source archive, matched all four source hashes, accepted the service-window schema, and reconciled requests/errors/p95 against CCT-09 evidence. The [validation receipt](evidence/CCT-10-handoff-validation.json) records the checks and hashes. The supplied JSON files remain local measured derivatives and can be regenerated with `from_receipt.py`; the supplied scripts and README are preserved unchanged with Merul's attribution.

The handoff contains ten service rows from one five-second interval of the existing November 25 shopping capture, with 1–13 observed requests per service. Every row has `complete=false`, `ready=null`, and `restart_delta=null`. Pod gauges were collected after that interval. There are zero calibration-eligible windows and no fault/recovery manifest. All ten service states remain unknown under the current rule. This handoff validates format and missing-data handling; it is not a new sustained baseline or fault run.

## Remaining dependencies

| Owner | Required evidence |
| --- | --- |
| Maitreya, Merul and Param | A coordinated sustained healthy capture with a declared interval, common run ID/roster, scoped traces, fresh readiness/restart observations and independently reviewed per-window coverage. |
| Merul and Param | Under the current defaults, at least three eligible five-second windows per service with at least 20 requests in each. Low-volume services remain uncalibrated. Keep original event and availability timestamps. |
| Komalika, with Merul/Param | A fault-and-recovery run with matching telemetry and a verified target, effective start/end times, recovery result and evidence, all linked by run ID. |
| Mithil and Merul | Review the fitted thresholds, insufficient-volume services, coverage and timing; freeze the agreed rule before reserved-run evaluation. |

The pod sampler and join helper are supplied for this next capture. Their live timing and permissions still need verification on the shared cluster. The sampler performs reads only. Coverage flags must be supported by independent evidence; post-run gauges cannot be backfilled as observations of the earlier window.

Use separate experiment run IDs for training, validation and test data. No real thresholds or fault labels have been invented, and no forecasting-accuracy or live-warning claim is supported yet.
