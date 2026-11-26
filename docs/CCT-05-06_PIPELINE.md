# Param · CCT-05 / CCT-06 implementation and handoff

## Status

CCT-05 and CCT-06 have been implemented and validated on the contributor's Mac using local Kafka and Spark 4.0.1 (`local[2]`). The development sandbox previously passed 36 offline unit tests. The local synthetic smoke test, three scale points and healthy recorded-telemetry replay all produced matching normalized counts. Synthetic restart count checks passed at every scale. This is offline replay, not continuous live streaming or a prospective forecast.

## Independent upstream evidence

The team-provided [Merul summary](evidence/CCT-03-04-merul-summary.json) is a measured telemetry verification receipt for 2026-11-25. It records 4 browse, 2 cart and 9 checkout relationships (15 journey-specific matches), ten active application services and 14 selected log events (11 frontend, 3 checkout). The Collector exporter count increased by 14, not an individual stored-record readback. Paired collection overhead is pending. This is not a raw dataset or evidence of processing by our pipeline. The measured journey receipt and signal payloads were subsequently provided and converted with the recorded adapter. All required journey relationships were recovered by the adapter; extra valid relationships were retained. The Spark replay then preserved all converted telemetry records.

## Acceptance checklist

- [x] Versioned envelope, four pinned Kafka topics and finite replay implementation.
- [x] Explicit normalization, identity deduplication, availability-aware window features and Parquet writer implementation.
- [x] Recorded-telemetry adapter, fixture tests, sample replay and scale benchmark implementation.
- [x] Run synthetic broker-to-Spark acceptance: expected normalized counts and restart count checks passed.
- [x] Replay one recorded healthy measured archive: required journey relationships verified at conversion and all 14 logs preserved in Spark.
- [x] Record scale measurements and machine configuration.
- [ ] Hand off feature schema and approved Parquet outputs to Mithil.
- [ ] Review independent probes and thresholds with Merul/Mithil; evaluate on separate fault runs supplied by Komalika.

The novelty claim is controlled-chaos evaluation of temporal propagation and blast radius, not invention of failure prediction. Do not use fault metadata or finalized future outcomes as features. The midterm ground-truth protocol and provisional health-rule thresholds still require team agreement and calibration before the main experiments.

## Measured runtime results

Test machine: MacBookAir10,1 (Apple M1), 8 CPU cores, 8 GiB RAM, macOS 26.5.2. Spark ran in `local[2]` mode.

| Synthetic fixture runs | Kafka messages (including duplicate fixture) | Unique spans | Metrics | Logs | Producer s | Replay s | Kafka messages/replay s | Parquet bytes | Restart count check |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 22 | 19 | 2 | 1 | 0.363 | 13.844 | 1.589 | 71759 | Passed |
| 10 | 121 | 190 | 20 | 10 | 0.563 | 7.832 | 15.449 | 125959 | Passed |
| 100 | 1111 | 1900 | 200 | 100 | 1.924 | 7.967 | 139.446 | 429740 | Passed |

The synthetic smoke run independently produced 19 spans, 2 metrics, 1 log and 9 edge windows with a successful restart count check. The benchmark throughput denominator is Spark replay time, not producer plus replay time. These are single-run local synthetic scale points, not a multi-node study or a sustained throughput estimate. Startup/warm-up effects may explain the higher first-scale runtime.

Healthy recorded replay: 33 Kafka messages (10 trace, 21 metric, 2 log), 36 server spans, 42 metric points, 14 selected logs, 36 lineage rows, 13 edge windows, 10 service windows, 42 metric windows and 4 log windows. Replay took 15.131 seconds (2.181 Kafka messages/s); zero normalized records were removed as duplicates. Five-second windows; minimum request count 20; no cutoff was specified. Zero experiment records is expected for this healthy run. The 13 edge windows are not the same measure as the 15 required journey-specific relationships.

The recorded archive contains a Jaeger server-span projection, not full client/internal traces. Its `historical_live_forecast_valid` flag is false. These results establish healthy-data replay, not fault propagation accuracy, independent impact labels, live collection overhead or prospective predictions. Fault-run replay and final evaluation remain pending. Generated outputs stay in ignored `outputs/`; share approved artifacts privately.
