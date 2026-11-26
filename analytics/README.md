# Analytics · Mithil

Mithil owns CCT-09 (service graphs and baseline metrics), joint CCT-10 (health labels and forecast timing), and root-cause/cascade evaluation. This implementation uses Python 3.9+ and the standard library. Parquet input additionally requires `pyarrow`. It reuses Param's normalized spans and nearest-server helper and Merul's candidate health rule.

## Reproduce the synthetic example

From the repository root:

```sh
python3 -m analytics demo
python3 -m unittest discover -s tests -v
```

Open `outputs/analytics-demo/report.html` for a graph, service metrics, health timeline, root rankings and forecast comparisons. JSON inputs/results and service-metric CSV are saved alongside it. This is an invented cascade for software verification, not measured accuracy. Training, validation and test runs are disjoint. The validation run is a healthy synthetic control.

## Real graph handoff: CCT-09

CCT-09 passed on the measured run `cct-probe-merul-261125052110-5d9e-measured`: all 4/2/9 required journey relationships and recorded call counts reconcile. See [acceptance evidence](../docs/evidence/CCT-09-mithil-real.json) and the report at `outputs/analytics-real/report.html`.

The preferred acceptance input is Merul's sanitized `measured/journeys.json`, including each core journey's `lineage.server_records`, `lineage.edges`, trace ID and observation time. A summary cannot substitute for these records.

```sh
python3 -m analytics graph --receipt /path/to/measured/journeys.json --output outputs/analytics-real
python3 -m analytics graph --spans /path/to/parquet/spans --run-id ACTUAL_RUN_ID --output outputs/analytics-parquet
python3 -m analytics graph --archive /path/to/recorded-archive --output outputs/analytics-archive
```

Receipt mode reconstructs the nearest-server projection and reconciles call counts and all 4/2/9 required journey relationships, retaining legitimate extras. It cannot independently verify raw client/internal ancestry removed from the projection. Raw spans use full ancestry traversal. The archive form expects `ingestion.recorded` output (`archive.json` and `traces.json`) and uses source observation time as a conservative availability bound; no Kafka receipt is fabricated.

`--spans` accepts a JSON array, JSONL, or Parquet file/directory with the `processing.normalize` span schema. `--cutoff-ms` restricts span/archive graphs by both event and availability time. An empty eligible graph fails explicitly. Each downstream SERVER contributes one RPC observation; CLIENT/INTERNAL ancestors are traversed. Duplicate identities are deduplicated, conflicts/cycles fail and missing parents are counted. Arrows point caller → callee. Metrics use SERVER durations, never sums across a call tree, with nearest-rank p95 and a low-count flag below 20 observations. The caller must select the healthy baseline interval. Per-journey relationships and global distinct edges have different denominators.

## Calibrate, predict, then evaluate

See the [November 26 CCT-10 status](../docs/CCT-10_STATUS.md) for the validated handoff, collection/join tools, and the real data still required for calibration.

The demo writes examples of every required input. A real split assigns entire experiment IDs to disjoint `train`, `validation`, and `test` lists. `healthy_intervals.json` maps each training run to its independently declared healthy `[start_ms,end_ms]` interval. Select these before inspecting test performance.

```sh
python3 -m analytics calibrate --windows outputs/analytics-demo/windows.json --split outputs/analytics-demo/split.json --healthy-intervals outputs/analytics-demo/healthy_intervals.json --output outputs/analytics-demo/calibration.json
python3 -m analytics predict --spans outputs/analytics-demo/spans.json --windows outputs/analytics-demo/windows.json --calibration outputs/analytics-demo/calibration.json --services outputs/analytics-demo/services.json --run-id synthetic-test --cutoff-ms 1790000030000 --output outputs/analytics-demo/prediction.json
python3 -m analytics evaluate --prediction outputs/analytics-demo/prediction.json --outcomes outputs/analytics-demo/outcomes.json --calibration outputs/analytics-demo/calibration.json --outcome-source synthetic-independent-probe-fixture --root-target paymentservice --output outputs/analytics-demo/evaluation.json
python3 -m analytics aggregate outputs/analytics-demo/evaluation.json --output outputs/analytics-demo/aggregate.json
```

Replace paths, roster, run IDs and cutoff with real inputs. Windows/outcomes accept JSON/JSONL/Parquet with the CCT-10 fields. The service roster is a JSON list declared independently of future data. Prediction does not accept fault targets or outcomes. The verified target is used only to score saved root rankings. Hashes bind results to their calibration and inputs.

Windows are epoch-aligned, 5,000 ms wide, with `available_ms >= end_ms`; use the latest eligible revision at each cutoff. Calibration uses twice the median healthy training p95, at least three valid windows/service, minimum 20 requests and a candidate 5% error limit. Healthy requires complete observations, ready=true, restart_delta=0 and sufficient samples. Latency/error breaches must persist for two consecutive windows. Lost readiness or a positive restart delta is sufficient for affected. Missing data, insufficient samples and uncalibrated latency remain unknown; hard readiness/restart failures still apply to uncalibrated services.

**Param's current service windows deliberately have `complete=false`, `ready=null`, `restart_delta=null`.** They cannot establish health. Merul must supply a reviewed, fresh per-window readiness/restart/coverage join. Do not simply flip these flags. Outcomes must be independently collected health observations using the frozen rule. The CLI records the declared outcome source but cannot prove independence of supplied files.

| Method | Behavior |
| --- | --- |
| Earliest anomaly | Rank currently affected services by first observed affected time available at cutoff; service name breaks ties. |
| Largest anomaly | Rank affected services by max(latency/limit, error fraction/error limit, hard-failure indicator). |
| No additional | Predict an empty newly affected set. |
| Reverse one hop | Predict healthy callers directly dependent on an affected service. |
| Reverse reachable | Predict healthy reverse-reachable services. |
| Distance weighted | Strongest shortest-path product of caller call shares divided by hop distance; include scores >= 0.5. |
| Temporal risk | Distance score × (1 + normalized current symptom + positive normalized p95 change); include scores >= 0.5. |

Cascade baselines predict the same set at each horizon; targets differ at 10/30/60 seconds. Scores are not probabilities. There is no calibrated propagation-speed model: time-to-impact estimates and timing MAE remain null. Do not tune the fixed score threshold on test data. Real threshold tuning and joint review with Merul remain open.

## Evaluation semantics

Only services healthy at cutoff are eligible for new impact. Already affected and unknown services are listed separately. Final outcome revisions may arrive later and are used only by the evaluator. The independent channel must confirm cutoff health. Any unknown/missing window before first affected censors that service, including the ambiguous first window of a latency-only breach. Missingness after confirmed onset does not erase the event. Onsets are five-second label times, not precise physical failure times.

Results include root top-1/top-3/MRR; precision/recall/F1/Jaccard; empty matches, coverage, censored/missed outcomes; affected-count/fraction error over the scorable population; and signed onset-minus-emission warning time. Empty/empty F1 is null. A nonempty union with zero true positives has F1 zero. Aggregation averages cutoffs within each run before averaging runs and retains per-run values. No unsupported confidence intervals are produced.

All predictions currently use offline replay. `emitted_at_ms` records actual execution, never the historical cutoff. `historical_live_forecast_valid=false` remains explicit even for positive timing in synthetic tests. Dependencies and temporal order suggest plausible propagation; they do not establish a causal path.

```sh
python3 -m analytics control --windows outputs/analytics-demo/windows.json --services outputs/analytics-demo/services.json --calibration outputs/analytics-demo/calibration.json --run-id synthetic-validation --start-ms 1790000000000 --end-ms 1790000100000 --output outputs/analytics-demo/control.json
python3 -m analytics benchmark --sizes 10 100 1000 --output outputs/analytics-demo/benchmark.json
```

Controls require an independently declared no-fault interval. Alarms count observed healthy→affected transitions; unknown gaps preserve previous state and are excluded from exposure hours. The benchmark measures local graph construction on invented chains (three repetitions per size), not Kafka/Spark throughput or live scaling.

See [Mithil's handoff](../docs/CCT-09-10_ANALYTICS.md) for verified results and remaining real-data gates.
