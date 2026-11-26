# Mithil Bavishi · CCT-09 / CCT-10 and analytics handoff

## Assignment and starting state

Mithil owns CCT-09, the real dependency graph and baseline metrics, and joint CCT-10 with Merul for health labels and forecast timing. The project plan also assigns root-cause/cascade comparisons, evaluation and visualization. Maitreya owns dashboard integration; Param supports evaluation data preparation; Komalika owns faults and verified ground truth.

At starting revision `d6d3099`, `analytics/` contained only a README. Reusable work included the starter synthetic report, Merul's trace inspection and candidate health rule, and Param's normalization/Parquet pipeline. Git attributes five November 25 integration/merge commits to Mithil; this does not establish authorship of the underlying telemetry. The new analytics was implemented with coding-assistant help on `feature/mithil-analytics`.

## Work implemented

| Source | Contribution |
| --- | --- |
| `analytics/graph.py`, `io.py` | Graph reconstruction, deduplication, cycle/conflict rejection, missing-parent diagnostics, service/edge metrics, receipt reconciliation and JSON/Parquet/archive input. |
| `analytics/health.py` | Whole-run splits, training-interval calibration, window validation, revisions and reuse of Merul's health rule. |
| `analytics/forecast.py` | Earliest/largest anomaly root rankings; no-additional, one-hop, reachable, distance-weighted and temporal-risk cascade methods. |
| `analytics/evaluate.py` | Separate outcome scoring, coverage/censoring, root and forecast metrics, blast-radius error, warning time, control alarms and run-level aggregation. |
| `analytics/demo.py`, `report.py`, `benchmark.py` | Invented cascade, portable graph/timeline report and local graph-construction benchmark. |
| `tests/test_analytics.py` | Graph correctness, leakage prevention, missing data, revision, evaluation and handoff regression tests. |

Defaults follow the proposal: five-second windows, healthy-training calibration and 10/30/60-second horizons. The temporal method is a fixed interpretable rule. There is no fitted propagation-speed model or measured forecasting-accuracy claim.

## How to explain the contribution

The analytics turns traces into a graph showing which services call each other. It measures service latency and errors, then uses a shared rule to mark services healthy, affected or unknown. At an incident cutoff, it ranks possible origins and predicts which currently healthy callers could become affected next. A separate evaluator checks future health observations and reports missing data and late predictions.

The runnable example uses an invented frontend → checkout → payment chain. Payment becomes affected first, then checkout and frontend, followed by recovery. This verifies software behavior, not accuracy on Online Boutique faults.

## Reproduction

```sh
python3 -m cct check
python3 -m unittest discover -s tests -v
python3 -m cct demo
python3 -m analytics demo
python3 -m analytics benchmark --output outputs/analytics-demo/benchmark.json
```

Open `outputs/analytics-demo/report.html`. The [runbook](../analytics/README.md) documents every CLI/input contract. Generated artifacts stay under ignored `outputs/`. The small [verification receipt](evidence/CCT-09-10-mithil.json) records local checks and benchmark observations. The existing November 24 report/slides remain historical submission artifacts.

## Acceptance and required handoffs

The [November 26 CCT-10 status](CCT-10_STATUS.md) records the validated Merul handoff and remaining capture/review dependencies. The supplied partial windows reproduce correctly, but none qualify for calibration.

- [x] Executable graph, calibration, root/cascade baselines and evaluation.
- [x] Synthetic reproduction and regression checks.
- [x] Adapters for the measured receipt and normalized-span contracts.
- [x] CCT-09 real acceptance: regenerated the graph from Merul's measured journey receipt and reconciled its 4/2/9 required relationships and all recorded edge call counts. See the [real-data receipt](evidence/CCT-09-mithil-real.json).
- [ ] CCT-10 real acceptance: review the coverage/readiness join and calibrate thresholds with Merul on recorded runs.
- [ ] Fault evaluation: score reserved test runs with verified effective fault/recovery intervals and independent health outcomes.
- [ ] Prospective warning and time-to-impact: collect suitable live timing evidence and calibrate timing before making these claims.

For CCT-10 and fault evaluation, the remaining handoff is Komalika's verified manifests and independent health windows with event/availability times, fresh readiness, restart deltas, request/error counts, p95 and justified coverage. Param's matching normalized spans/Parquet support integration with those runs. Keep captures in ignored `data/` or `outputs/` with stable run IDs. Local analysis needs no credentials.

The initial software verification receipt records the earlier synthetic-only acceptance state. The measured journey archive is now available locally under ignored `outputs/telemetry/` and supports the CCT-09 acceptance below. The earlier receipt remains a historical record; CCT-10 calibration and fault evaluation are still open.

## CCT-09 recorded-run acceptance

Run `cct-probe-merul-261125052110-5d9e-measured` covers November 25, 2026, 05:21:38.897773–05:21:55.162467 UTC. Its matching measured summary reports successful trace and signal verification. The graph was regenerated from `lineage.server_records`, then compared with the saved per-trace edge counts and required journey relationships. A separate direct-parent count over the recorded server projection also reconciled aggregate edge and service counts.

| Journey | Required relationships matched | Observed distinct edges | Server spans |
| --- | ---: | ---: | ---: |
| Browse | 4/4 | 4 | 14 |
| Cart | 2/2 | 2 | 3 |
| Checkout | 9/9 | 11 | 19 |

Together these traces contain 10 services, 36 server spans, 33 inter-service calls and 13 distinct service edges. There are no duplicate spans, missing parents or recorded server errors. The 15 required relationships count shared edges separately for each journey; the 13 global edges include two extra valid checkout relationships. All service sample counts are below 20, so the generated latency/p95/error summaries are descriptive baseline measurements, not calibrated health thresholds.

```sh
python3 -m analytics graph --receipt outputs/telemetry/cct-probe-merul-261125052110-5d9e/measured/journeys.json --output outputs/analytics-real
```

Outputs are `outputs/analytics-real/report.html`, `graph.json`, and `service_metrics.csv`. The [acceptance receipt](evidence/CCT-09-mithil-real.json) preserves source/artifact hashes, code hashes, counts and limitations. Raw captures remain ignored. This completes CCT-09's recorded-trace graph and baseline-metric gate. It does not establish CCT-10 calibration, fault forecasting accuracy or raw CLIENT/INTERNAL ancestry verification beyond the saved server projection.
