# CCT-06 · Spark normalization and Parquet

See [ingestion runbook](../ingestion/README.md) for reproducible commands. Spark 4.0.1 and its Scala 2.13 Kafka connector are pinned. A finite batch replay reads saved per-partition start/end offsets, fails on offset gaps or non-append timestamps, and writes a new staging directory before committing the output. Restart with identical snapshot/settings returns the committed receipt; changed inputs need a new directory. This is a batch replay checkpoint (offset manifest + atomic directory), not Structured Streaming. A full rebuild can be independently compared. Keep broker retention long enough to reproduce the saved range.

| Parquet table | Meaning |
| --- | --- |
| raw_telemetry | Original envelopes, offsets, append time |
| raw_experiments | Isolated experiment/ground truth records; excluded from feature extraction |
| spans / metrics / logs | Explicit normalized schemas, identity/hash, source observation and broker availability times |
| lineage | Nearest server ancestor, linked/root/missing/cyclic status |
| service_windows | Epoch-aligned 5-second SERVER-span end-time windows; requests, errors, mean/p95 ms, low count |
| edge_windows | Caller-to-callee window counts, errors and approximate p95 |
| log_windows | Selected events by service and window, not all logs |
| metric_deltas | Actual gauges, reported deltas, compatible cumulative differences, unknown/reset flags |
| metric_windows | Latest point per series per window; no extrapolation of interval totals |

Deduplication is by run/trace/span, full metric-series identity/start/timestamp, or run/event ID. Conflicting normalized records fail closed. `available_ms` uses the maximum of rounded-up event time, source observation time and broker append time; windows also cannot be available before they close. `--cutoff-ms` recomputes derived tables using only eligible normalized observations; normalized tables still hold the full archive and must not be read directly as prediction features. Late records remain in the raw and normalized tables; a fresh cutoff-specific replay recomputes windows. There is no automatic live watermark or online warning claim. The receipt records actual replay emission separately.

Service windows are sampled trace features, not an independent outcome channel. `complete=false`, `ready=null`, and `restart_delta=null` until independent coverage/freshness and roster integration are available. Low count defaults to fewer than 20 requests; missing metrics are not zero-filled. Actual ground truth must come from experiment-runner events and independently collected health probes, with frozen thresholds agreed by the team. Forecast metrics remain Mithil's evaluation responsibility. Replay of a historical archive cannot establish historical warning lead time.

Current limitations: explicit histogram/gauge/sum support; unsupported metrics fail rather than silently disappear. Trace groups and metric series are grouped on executors; very large individual traces/series may require further partitioning. Local filesystem output only; one writer per output directory. The provided benchmark measures count consistency and throughput for cloned synthetic checkout traces, not live accuracy. See [implementation evidence](../docs/CCT-05-06_PIPELINE.md).
