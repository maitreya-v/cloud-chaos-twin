# CCT-05 · Kafka ingestion

For sustained pilot captures with observed fault records, use the separate
[fault replay adapter and validation runbook](../docs/CCT-10_PIPELINE_REPLAY.md).
The original measured-journey adapter below remains unchanged.

Staged offline replay is the MVP: run Kafka on a development machine, not the shared Boutique host. Four topics (`cct.traces`, `cct.metrics`, `cct.logs`, `cct.experiments`) each have three partitions, replication factor 1, seven-day retention and broker LogAppendTime. This is not an HA deployment. Messages are versioned JSON envelopes around a single OTLP resource group (or one experiment manifest), keyed by run ID. Publish waits for acknowledgements, retries transient failures and refuses messages above 900 KB; split larger resource groups before publishing. Keep archived originals. No private raw data should be committed.

## Local setup (Python 3.11 or 3.12, Java 17, Docker Compose v2)

From the repository root, in a shell with Java 17 active:

```sh
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r ingestion/requirements.txt
docker compose -f ingestion/compose.yaml up -d --wait
python -m ingestion.kafka_io init
```

Use a dedicated local broker with no other publishers. Spark downloads its matching Kafka connector from Maven on the first run. Port 9092 is bound to loopback only.

## Synthetic acceptance + scaling (no cluster access)

```sh
python -m processing.benchmark outputs/pipeline/benchmark-01 --scales 1 10 100
```

The script publishes distinct cloned fixture runs plus a deliberate repeated fixture, freezes the exact offset interval, consumes it with Spark, reconciles counts, writes Parquet, repeats the committed replay, and rebuilds the smallest scale. The fixture has 19 spans, 2 metrics, 1 selected log and 9 distinct checkout edges per run. One extra fixture must be deduplicated. Timings are wall-clock on the executing machine; record CPU, RAM, OS and Java version alongside the results. This benchmark varies data volume, not physical service count or actual fault severity. Producer timing uses synchronous acknowledgements and is not maximum broker throughput. No benchmark output is pre-populated.

## Real recorded telemetry

Ask Merul for the sanitized `measured/` receipt directory containing `summary.json`, `journeys.json`, `signals/metrics.json`, and `signals/logs.json`. Do not share credentials, raw application logs, or unrelated telemetry. The summary alone cannot be replayed. From the root:

```sh
python -m ingestion.recorded /path/to/measured outputs/pipeline/recorded-archive
```

The adapter checks run identity, verified measured status, three journey edge sets and selected-log count. It converts the recorded Jaeger *server projection* (not complete OTLP spans) from microseconds to nanoseconds and preserves the selected log and pod-gauge OTLP records. The two actual observed Prometheus call-counter readings per service are represented as cumulative-value gauges, not invented per-window traffic counts. See `archive.json` for the run ID and observation time. Publish on a clean dedicated broker (otherwise snapshot includes prior retained runs):

```sh
RUN_ID=$(python -c 'import json; print(json.load(open("outputs/pipeline/recorded-archive/archive.json"))["run_id"])')
OBSERVED_NS=$(python -c 'import json; print(json.load(open("outputs/pipeline/recorded-archive/archive.json"))["observed_ns"])')
python -m ingestion.kafka_io publish outputs/pipeline/recorded-archive --run-id "$RUN_ID" --recorded --observed-ns "$OBSERVED_NS" --receipt outputs/pipeline/recorded-publish.json
python -m ingestion.kafka_io snapshot outputs/pipeline/recorded-offsets.json
python -m processing.replay outputs/pipeline/recorded-offsets.json outputs/pipeline/recorded-parquet
```

The snapshot command includes all retained messages; use an isolated broker or use the benchmark's before/after interval helper. Inspect receipts for acknowledged vs consumed counts and output table counts. Do not call `docker compose down -v` unless intentionally deleting local data; retain evidence first. `docker compose -f ingestion/compose.yaml stop` stops the broker without deleting data.
