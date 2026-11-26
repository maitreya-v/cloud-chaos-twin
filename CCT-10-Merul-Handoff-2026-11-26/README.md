# CCT-10 telemetry handoff from Merul to Mithil

## What is available

The November 25 measured shopping probe produced 36 distinct SERVER spans in three selected traces. All span end times fall in **05:21:35–05:21:40 UTC**. The probe began at 05:21:38.897773, partway through that five-second interval. Ten services contributed between 1 and 13 spans each. All observed spans had a false error flag. No independent request denominator covers the whole interval. The pod gauges were sampled at **05:22:00.198987 UTC**, after the service window closed. Absolute post-run restart counts cannot establish the change during the window.

| File | Use |
| --- | --- |
| `measured_windows.json` | Ten real rows for testing the CCT-10 reader and missing-data behavior. All have `complete=false`, `ready=null`, and `restart_delta=null`. Zero rows qualify for calibration. |
| `post_run_gauges.json` | 22 observed gauge values covering the ten application services and Redis, with original Unix-nanosecond sample times. These belong to a separate snapshot. |
| `source_audit.json` | Run ID, source hashes, scope and missing fault fields. There is no observed fault target, effective start/end time or recovery result in this handoff. |
| `from_receipt.py` | Rebuilds the three JSON files from the measured folder in the outputs ZIP already shared. |
| `capture_pods.py`, `join_windows.py` | Read-only Kubernetes sampling and a conservative join for a future run. Both use the Python standard library. Reading a Parquet input additionally requires `pyarrow`. |

Rows represent observed service calls, assigned by SERVER-span **end time** to half-open, epoch-aligned UTC windows. Requests, errors and nearest-rank p95 describe only the saved spans. Services or intervals without observed spans are omitted, so absence stays unknown. Each row's `available_ms` is at least its end time and the latest rounded-up Jaeger query observation time among its contributing spans. This gives a source-level bound for retrospective analysis. Kafka, Spark and online model availability may be later. The warm-up is excluded.

The ten counts, error counts and p95 values were checked against [Mithil’s CCT-09 evidence](https://github.com/maitreya-v/cloud-chaos-twin/blob/9b704e7fb09b43311f63789a5413ae60479a6db3/docs/evidence/CCT-09-mithil-real.json). Param’s [pipeline report](https://github.com/maitreya-v/cloud-chaos-twin/blob/9b704e7fb09b43311f63789a5413ae60479a6db3/docs/CCT-05-06_PIPELINE.md) reports ten replay windows for the same capture. Its replay used no fault records. September's workload baseline predates this service-level telemetry. Neither dataset supplies verified calibration windows or a recorded fault and recovery.

To regenerate the JSON from the original archive, point `MEASURED` to the extracted directory containing `journeys.json` and `summary.json`. Run the following from the unzipped handoff directory.

```sh
MEASURED="/path/to/outputs/telemetry/cct-probe-merul-261125052110-5d9e/measured"
python3 from_receipt.py "$MEASURED" rebuilt
```

## Next healthy and fault runs

1. **Maitreya, Merul and Param** agree on a sustained healthy shopping window, a unique run ID, service roster, expected replicas, and an independent reference for total completed service requests. Record the actual healthy interval. Param archives all scoped spans with original observation and availability times. For his current training defaults Mithil needs at least three eligible windows per service, each with at least 20 requests. A service that never reaches this threshold remains uncalibrated.
2. **Merul** starts the pod sampler before shopping and keeps it running beyond the final window, including recovery when applicable. Use a separate run ID and new output path for every run. The example below collects 180 seconds of pod observations. Start workload and tracing in their agreed window while this command runs. The sampler only reads application pods. It records list failures and omits connection details. Its timing and RBAC still need a live test on the connected cluster.
3. **Komalika** supplies the independently observed fault target, effective start and end in UTC milliseconds, and the verified recovery result with its time and evidence. Use the same run ID throughout. Keep fault annotations apart from prediction inputs. The repository's example fault configuration and synthetic demonstrations cannot supply these observations.
4. **Param and Merul** reconcile captured server records against the independent per-service source for each five-second interval. Retain evidence of identity/roster, matching completions, errors, durations, and coverage of the whole window. A smooth chart, a successful scrape, or the same spans counted twice cannot establish independence. If independent accounting is unavailable, publish the incomplete rows and explain why.
5. **Mithil and Merul** review per-service training counts, the fitted latency thresholds, low-volume services, coverage loss and actual availability delay. Keep train, validation and test run IDs disjoint. Freeze reviewed thresholds before fault-run evaluation.

```sh
# Supply the same actual ID that Param will use when recording this experiment.
RUN_ID="replace-with-agreed-run-id"
python3 capture_pods.py --run-id "$RUN_ID" --seconds 180 --output "$RUN_ID.pods.jsonl"
```

After Param supplies the matching JSON array or `service_windows` Parquet directory, run the join below. These commands are for new data. Keep the old shopping receipt as a separate run.

```sh
python3 join_windows.py --run-id "$RUN_ID" --windows /path/to/service_windows \
  --pods "$RUN_ID.pods.jsonl" --output "$RUN_ID.partial.json"
# Once coverage has been independently reconciled and reviewed
python3 join_windows.py --run-id "$RUN_ID" --windows /path/to/service_windows \
  --pods "$RUN_ID.pods.jsonl" --coverage /path/to/reviewed_coverage.json \
  --output "$RUN_ID.reviewed.json"
```

The join expects one input revision per service/window. It uses closely spaced pod-list reads before, during, and after the window. It checks stable pod and container identities rather than assuming the restart count belongs to one continuous process. Readiness uses the last fresh in-window pod list. A positive restart delta is attributable only if the increases fit wholly between in-window reads. Missing or slow samples, pod replacement, ambiguous changes or missing statuses leave fields unknown. A Kubernetes list is a snapshot and can miss brief transitions. Check the declared replica roster and independent evidence before attesting completeness. Rows become available no earlier than the last source used. The joined file holds final revisions, so use their actual availability times for cutoff analysis.

A reviewed coverage JSON array has one object per attested window, keyed by `run_id`, `service`, `start_ms`, `end_ms`. Each object includes the independent `requests`, `errors`, `duration_count`, `verified` and `records_reconciled` booleans, `independent_source`, `evidence_ref`, `reviewer`, and the actual `available_ms` of that review. Set the two flags true only after checking individual completions and durations, the full interval and expected replica roster. The tool checks the attestation and matching counts. The reviewer supplies the evidence behind it. A post-run human review creates a retrospective revision. It cannot establish that this revision was available to make a live warning.

## Merul’s health-rule review before calibration

The existing rule permits a fresh readiness loss or a confirmed restart to flag a service. For latency and error labels it requires adequate volume and complete observations. A service is affected after two consecutive breaches. The candidates are a p95 above twice the median healthy training-window p95, or an error fraction of at least 5%. Reviewing these rules is still pending the fitted thresholds, real healthy windows, and an independent fault/recovery record. No forecast accuracy, fault outcome or timing improvement has been measured by this handoff.
