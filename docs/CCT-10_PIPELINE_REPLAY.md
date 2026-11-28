# Recorded fault replay: Kafka → Spark → Parquet

The owner's next integration check replayed the captured pilot
`cct-probe-cct10-20261127-02` through Param's existing ingestion and processing
implementation. The new adapter accepts the sustained pilot archive, preserves
source times and identities, and isolates its observed fault metadata. No new
workload, fault or application rollout was needed.

## Recorded result

| Check | Result |
| --- | --- |
| Acknowledged Kafka messages | 3,683: 2,685 trace, 995 metric, 2 log, 1 experiment |
| Normalized spans / metrics / logs | 6,492 / 1,006 / 495; all source content, identities and availability reconciled |
| SERVER lineage | 5,951 linked, 541 frontend roots; no missing parent |
| Retrospective dependency graph | 10 services, 14 distinct directed service edges |
| Parquet tables | 11 tables; all physical counts checked |
| Full capture service windows | 591, including spawning/warm-up |
| Fully contained measured windows | 399; requests, errors and p95 exactly match the pilot reference |
| Fresh pod join | All 399 measured windows have ready=true and restart_delta=0 |
| Completeness / calibration | All remain complete=false; zero eligible calibration windows |
| Restart | Same snapshot/output returned the committed receipt with every file unchanged |
| Full rebuild | All 11 tables reproduced identical logical row hashes |
| Cleanup | Isolated broker stopped; retained data; shared application/monitoring unchanged |

First replay took 21.245 seconds; independent rebuild took 9.433 seconds. These
are single local runs, not a throughput or forecasting benchmark. Runtime:
Python 3.13.15, Spark 4.0.1 `local[2]`, Java 17.0.21.1+2, Kafka 3.9.1 ARM64,
kafka-python 2.2.15 and PyArrow 21.0.0. Spark's driver heap was 1 GiB. The dedicated
broker had a one-CPU/1-GiB limit, loopback access only, and no other publisher.

The [sanitized verification receipt](evidence/CCT-10-pipeline-replay-20261127.json)
records source, table and bundle hashes. Raw artifacts stay in ignored outputs
and are prepared for private handoff. Teammate acceptance is still pending.

## Scope and limits

The trace representation is the **nearest-SERVER projection**, not complete
client/internal OTLP. Original trace/span IDs, microsecond event times and each
query's observed time are retained. External probe parents are removed from the
projection, as in the existing recorded adapter. Original raw Jaeger traces remain
in the pilot bundle. Redirects retain their distinct frontend request roots.

The adapter forwards actual raw-scrape-time SERVER call counters and target-up
readings as observed gauges, plus the existing post-run pod gauges. It invents no
counter epoch, interpolated requests, histogram or zero baseline. It deduplicates
overlapping Prometheus reads by full series identity/timestamp and rejects
conflicting values. Selected frontend/checkout logs preserve their event IDs and
observation times. Unsupported/unselected Prometheus series remain in the source
capture, rather than being described as normalized telemetry.

The controller's Apply/Recover records retain one-second precision. Exact physical
fault times remain null. The experiment envelope is written only to
`raw_experiments`; it is never a prediction feature or proof that every service
was affected.

Kafka uses real broker LogAppendTime. Replay availability is therefore later than
the original capture; nothing is backdated to make a historical live prediction.
Fresh pod observations are joined retrospectively. The original 405 observed
pilot windows include six boundary-window rows excluded by this replay's fully
contained measured selection, leaving 399 comparable rows. The remaining full
capture windows include warm-up and must not be used as a declared healthy
training interval by default.

This replay does not supply independent per-service coverage, sufficient volume
for all services, frozen thresholds, independent impact labels or forecast scores.
Those CCT-10 acceptance requirements remain open.

## Reproduction

Use an extracted original pilot directory containing `file-hashes.json`, raw
traces, Prometheus reads, fault records, pod snapshots, signal payloads and the
partial service-window reference. The summary alone is insufficient. Install
`ingestion/requirements.txt` and `processing/requirements-verify.txt` in a private
Python environment, with Java 17+ active. The actual validation used the versions
above; see [Spark's installation requirements](https://spark.apache.org/docs/4.0.1/api/python/getting_started/install.html).

Use a new output directory and unique dedicated Compose project. On a development
machine with adequate headroom, first adapt without connecting to Kafka:

```sh
python -m ingestion.pilot adapt SOURCE_DIRECTORY outputs/pipeline/pilot-replay/archive
docker compose -p cct-pilot-replay -f ingestion/compose.pilot.yaml up -d --wait
python -m ingestion.kafka_io --bootstrap 127.0.0.1:19092 init
python -m ingestion.pilot publish outputs/pipeline/pilot-replay/archive \
  --bootstrap 127.0.0.1:19092 --receipt outputs/pipeline/pilot-replay/publish.json
python -m ingestion.kafka_io --bootstrap 127.0.0.1:19092 snapshot \
  outputs/pipeline/pilot-replay/offsets.json
python -m processing.replay outputs/pipeline/pilot-replay/offsets.json \
  outputs/pipeline/pilot-replay/parquet
python -m processing.verify_pilot SOURCE_DIRECTORY outputs/pipeline/pilot-replay/archive \
  outputs/pipeline/pilot-replay/parquet outputs/pipeline/pilot-replay/verification.json
python -m analytics graph --spans outputs/pipeline/pilot-replay/parquet/spans \
  --run-id cct-probe-cct10-20261127-02 --output outputs/pipeline/pilot-replay/analytics
docker compose -p cct-pilot-replay -f ingestion/compose.pilot.yaml stop
```

Stop the dedicated broker after failures too. Do not delete an existing volume or
publish into somebody else's broker. On a reused broker, take before/after offset
snapshots and select only this publish interval, as described in the ingestion
runbook. The original snapshots describe the retained broker's actual offsets;
republishing elsewhere requires new snapshots and new output destinations.

For a full rebuild, replay the same retained offsets into a second new Parquet
directory and give it to `processing.verify_pilot --rebuild SECOND_DIRECTORY` with
a new verification filename. Compare logical row hashes, not binary Parquet file
names. Re-running `processing.replay` with an existing committed output checks the
restart path without appending records.
