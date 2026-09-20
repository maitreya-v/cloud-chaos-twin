# Architecture and contracts

## First implementation boundary

The runnable starter consumes a small normalized-span JSON file and writes an observed dependency graph, per-service/per-phase summary, and static HTML report. It has no network side effects. It does not run Kafka, Spark, an application, or fault injection.

Each fixture record is a SERVER span. Parent IDs refer directly to the upstream service's SERVER span. This simplified representation requires an upstream normalization step for real OTLP: raw client/server spans cannot be passed through unchanged without double-counting or misidentifying call edges. Trace identity is scoped by run ID and trace ID. Missing parents are counted in the output; conflicting duplicate span IDs are rejected.

## Planned live flow

- Applications -> Collector -> Jaeger for debugging and Kafka for analysis.
- Prometheus -> incremental metrics exporter -> Kafka, preserving timestamps and deduplicating overlapping reads.
- Selected application logs -> Collector -> Kafka.
- Experiment runner -> independent manifest with actual fault interval and cleanup outcome.
- Spark -> normalized tables and service/edge windows -> Parquet.
- Graph/forecasting methods -> versioned results -> dashboard and evaluation.

## Rules that affect correctness

1. Use UTC and explicit units. Store event time and data-availability time separately.
2. Keep injected targets and fault labels out of telemetry-only predictor inputs.
3. Split complete experiments into train/validation/test before fitting thresholds or preprocessing.
4. A dependency edge A -> B means A calls B; impact may spread B -> A.
5. Match Kafka message encoding to the Spark parser; OTLP JSON is nested.
6. Require evidence that a fault took effect. Accepted configuration is not ground truth.
7. Missing telemetry is not a healthy observation. Report coverage and collection failures.
8. Save forecast cutoff and emitted_at; future labels and late-corrected data cannot enter that prediction.
9. Compare replay throughput separately from live forecasting and physical multi-host scaling.
10. Full-stack versions are pending validation; record them before collecting the main dataset.

Further detail and the experiment matrix are in [PROJECT_PLAN.md](PROJECT_PLAN.md).
