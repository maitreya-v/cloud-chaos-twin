# Collecting evidence from Online Boutique

Merul owns trace propagation and Jaeger (CCT-03), service metrics and selected logs (CCT-04), and contributes the health-label contract with Mithil (CCT-10). The [host rollout record](../docs/CCT-03_TRACING_ROLLOUT.md) now records ten traced services, all three complete shopping journeys and service-counter activity. Merul still needs to perform independent verification, capture and confirm selected logs, and measure collection overhead with Maitreya. Health-label calibration with Mithil remains pending.

## Start on Merul's Mac

Use your invited Tailscale identity. On macOS, install the client if missing with `brew install kubectl`. The host's setup and access guide is [here](../docs/PRIVATE_ACCESS.md).

```sh
mkdir -p "$HOME/.kube"
export KUBECONFIG="$HOME/.kube/cct-team.yaml"
tailscale configure kubeconfig cct-kubernetes
kubectl auth whoami
kubectl get pods -n cct-app
kubectl auth can-i create deployments.apps -n cct-observability
kubectl auth can-i get pods/log -n cct-app
```

Confirm the identity is yours. Keep this KUBECONFIG selected for Merul's commands. Monitoring resources use `cct-observability`; the host owns `cct-app`. Everything below runs from the project repository root. Stagger this pilot with Kafka/Spark and fault testing on the 16 GiB Mac.

## Review and run the monitoring stack

The renderer outputs three single-replica Deployments, three ClusterIP Services and two ConfigMaps. All pods run non-root with dropped capabilities, the runtime-default seccomp profile, a read-only root filesystem and no mounted Kubernetes token. Each component requests 50m CPU and 64 MiB, with limits of 500m and 256 MiB. Jaeger retains up to 1,000 traces in memory. Prometheus scrapes every five seconds and uses ephemeral storage. The Collector keeps selected OTLP logs in rotating files on a bounded temporary volume. Save evidence before restarting or deleting workloads.

1. Generate and inspect the installation. A host with Docker/buildx can resolve the three release tags to native linux/arm64 registry digests using `--pin-arm64`. Have Maitreya run the binary checks and review the exact rendered file and available capacity before deployment. Share that reviewed file privately with Merul for the apply. The default render, which needs only Python, is useful for review.
2. Run server-side dry run under Merul's identity. Inspect existing resources with these names before applying. Use the approved file for the actual rollout.
3. After the rollout, save the running image IDs. Confirm that both Prometheus targets are up and wait for a few scrapes before the measured shopping pass.

```sh
mkdir -p outputs/telemetry
python3 observability/stack.py > outputs/telemetry/stack.review.json
# Docker host, before review of the final file
sh observability/validate_images.sh
python3 observability/stack.py --pin-arm64 > outputs/telemetry/stack.json
# Merul, after review
kubectl -n cct-observability apply --dry-run=server -f outputs/telemetry/stack.json
kubectl -n cct-observability apply -f outputs/telemetry/stack.json
for d in cct-jaeger cct-collector cct-prometheus; do
  kubectl -n cct-observability rollout status deployment/"$d" --timeout=180s || break
done
kubectl -n cct-observability get pods -o json > outputs/telemetry/monitor-pods.json
kubectl -n cct-app get pods -o json > outputs/telemetry/app-pods.json
git rev-parse HEAD > outputs/telemetry/revision.txt
```

Prometheus derives request counts, error breakdowns and duration histograms from SERVER spans. CLIENT spans remain available in Jaeger. A gRPC health-check filter removes named health RPCs from span-derived metrics. The Collector's own scrape target records refusals and export failures when those series exist. Successful OTLP receipt and successful delivery to Jaeger or Kafka are distinct checks. Service CPU and memory require a separately authorized source. The pod API reader provides readiness and restart gauges.

## Application changes for Maitreya to review and apply

Frontend, catalog, email and recommendation use their existing native images. Six rebuilt variants cover ads, cart, shipping, currency, payment and checkout. The rollout enables tracing, preserves the original profiler settings, and targets the in-cluster Collector. The Node clients receive an HTTP-form gRPC address. The owner script prints a staged rollout with one patch per service. Review it and record each affected Deployment's current revision (`kubectl -n cct-app rollout history deployment/NAME`) and image before applying. Use the owner's project kubeconfig and explicit kind context for this step, with the Collector already Ready.

Cart registers ASP.NET Core instrumentation and the OTLP exporter, with trimming disabled in the traced build. Shipping installs a gRPC stats handler, W3C propagation and a Go OTLP batch exporter. Ads use the versioned, SHA-256-checked OpenTelemetry Java agent with only trace export enabled. Its agent directory must remain traversable by the non-root application user. Currency and payment use `@opentelemetry/exporter-trace-otlp-grpc` 0.222.0 to match their Node SDK; the original legacy 0.26.0 exporter did not deliver their spans during the live probe. Both package locks are updated. Checkout passes its request context into the currency RPC rather than `context.TODO()`, which detached that call from the shopping trace. Redis remains a datastore outside the ten application-service tracing targets. The [patch](boutique-v0.10.7.patch) applies to Boutique commit `5b608cb1dfe317d05e4f1bb8c99b7618452fde87`. Source context is Apache-2.0; retain upstream notices. The exporter is documented in the [OpenTelemetry JavaScript repository](https://github.com/open-telemetry/opentelemetry-js/tree/main/experimental/packages/exporter-trace-otlp-grpc).

The cart patch pins OpenTelemetry hosting and the OTLP exporter to 1.15.3. This brings in the fixed API header parser and fixes the exporter's experimental disk-retry defaults; see the upstream [API advisory](https://github.com/open-telemetry/opentelemetry-dotnet/security/advisories/GHSA-g94r-2vxg-569j) and [exporter advisory](https://github.com/open-telemetry/opentelemetry-dotnet/security/advisories/GHSA-4625-4j76-fww9). The original 1.12.0 pins produced NuGet vulnerability warnings during the native build.

```sh
# Maitreya on the prepared host, once he approves building native variants
python3 observability/build_images.py
python3 observability/owner.py --images .local/telemetry/image-lock.json > outputs/telemetry/owner-ten.sh
# Review image digests, resources and environment. Run during the agreed window.
sh outputs/telemetry/owner-ten.sh
```

The builder clones the owner's pinned source into a temporary directory, applies the patch, builds/loads six new image names and records digests. It checks Java-agent startup under the non-root UID and sends a real SDK span to a local OTLP/gRPC receiver inside each Node image before import. It preserves the original Boutique image lock. The [host image lock](../configs/boutique.telemetry.lock.json) records the imported variants; these references need the corresponding images loaded into kind on another host. If one rollout fails, stop and inspect its events. For each changed application Deployment Maitreya can restore its recorded revision with `kubectl --context kind-cloud-chaos-twin -n cct-app rollout undo deployment/NAME --to-revision=NUMBER`, then wait for readiness and rerun the original shopping smoke check. Save receipts first. Maitreya and Merul can remove only the resources from the approved monitoring manifest with `kubectl delete -f outputs/telemetry/stack.json` after they agree to end the pilot.

## Record an independent shopping verification

From the repository root, with the three monitoring services ready and tracing enabled on the application, run:

```sh
python3 -m observability.record
```

The command uses the team kubeconfig (`~/.kube/cct-team.yaml`), prints the caller’s Kubernetes identity, and exercises browse, cart, and checkout twice. Warm-up primes the span and exporter counters. The measured run checks each request’s trace relationships, changes in service call counts, selected timestamped frontend and checkout events, and pod-state gauges. Reserve a quiet window so other traffic does not contribute to the exporter count.

Each run gets a private directory under `outputs/telemetry/`. The terminal prints the measured `summary.json` path. The relay checks OTLP acceptance and compares the number of selected records with the change in the file exporter’s sent-log counter on the same Collector instance. A missing baseline, changed Collector instance, or count mismatch leaves delivery unverified. The receipt records count-level delivery evidence. The overhead comparison requires a separate pair of workload runs.

## Prove browsing, cart and checkout under Merul's existing role

This Job talks to ClusterIP endpoints and prints a single sanitized JSON receipt. It requires no port-forward permission. Each run creates a fresh cookie jar and a unique W3C trace ID per HTTP request. It checks a home page, an add-to-cart redirect, cart contents, a simulated order confirmation and an empty cart after checkout. It retrieves each selected trace by exact ID, walks parent references to a frontend SERVER span, and requires the [declared journey edges](lineage.py). The checker records missing edges and unattached spans. It cannot establish coverage for unrelated optional features. Aggregate counter deltas corroborate service activity during an isolated test window. Traffic from other users and health checks can influence them.

```sh
RUN="cct-probe-$(date -u +%Y%m%d%H%M%S)"
D="outputs/telemetry/$RUN"
mkdir -p "$D"
python3 observability/stack.py --probe "$RUN" > "$D/probe.json"
kubectl -n cct-observability create -f "$D/probe.json"
# Negative coverage makes the Job fail by design. Read its receipt in either case.
kubectl -n cct-observability wait --for=condition=complete job/"$RUN" --timeout=150s
kubectl -n cct-observability logs job/"$RUN" > "$D/journeys.json"
python3 -m json.tool "$D/journeys.json"
python3 observability/signals.py --evidence "$D/journeys.json" --output "$D/signals"
```

Use an unmeasured warm-up probe first so the workload counters already exist; a newly created series has an unknown starting count. For the next measured Job choose a new RUN. Inspect any Job failure via `kubectl -n cct-observability describe job/"$RUN"` and inspect the specific pod's events if the container never started. A seven-service rollout should expose the uninstrumented hops. Keep the negative receipt. For ad, cart and shipping, a CLIENT span alone is insufficient. Client-to-server waits can take up to 45 seconds after shopping completes. The receipt includes times, trace IDs, server IDs, parent-server IDs, operations, durations in microseconds, frontend root counts, connected edges, and before/after Prometheus sample values. Cross-service caller identity is not proof of fault causation.

### Send the selected logs and pod gauges

The client uses its existing read-only project permissions to read current pods, readiness/restarts and timestamped stdout from frontend and checkout. It reads the exact half-open UTC probe interval, retains only allowlisted event names, and drops free text and customer fields. Missing pods, empty selections and individual read errors appear in the summary. Kubernetes' log timestamp has nanosecond precision. Selected logs carry a later observation time. Each gauge records the actual read time, which can be after the shopping interval. For incident monitoring, capture snapshots during the run as well. No backdating of availability is valid. A removed pod or its previous container may have uncollected logs; this bounded capture is suitable for the shopping pilot only.

With the default RBAC, a short-lived namespace Job can relay the saved sanitized batches. Check their size and contents before creating its ConfigMap. The Job's two acknowledgements cover OTLP acceptance. Use Prometheus and a Kafka consumer to check the downstream sinks independently.

```sh
python3 observability/stack.py --relay "$D/signals" --run-name "$RUN" > "$D/relay.json"
kubectl -n cct-observability create -f "$D/relay.json"
kubectl -n cct-observability wait --for=condition=complete job/"$RUN-relay" --timeout=90s
kubectl -n cct-observability logs job/"$RUN-relay" > "$D/relay-receipt.txt"
# After saving all local evidence
kubectl -n cct-observability delete -f "$D/relay.json"
kubectl -n cct-observability delete -f "$D/probe.json"
```

If an authorized Collector HTTP endpoint is already available, use `signals.py --otlp-http URL` when making the capture instead. A retry after a partial two-signal send may duplicate records. Deduplicate by log event ID or metric series/timestamp. To inspect Jaeger and Prometheus directly, Maitreya may forward services to his own loopback interface. Private dashboard sharing or additional Kubernetes port-forward access requires his separate configuration.

## Interfaces, quality and forecasting

- **Param.** Run `python3 -m observability.export_sample outputs/sample-otlp` for nested OTLP/JSON samples. They contain 19 spans, two pod gauges and one sanitized log, all synthetic. Review the [signal contract](CONTRACT.md) together before enabling the optional Kafka branch. Pass the agreed private broker to `stack.py --kafka BROKER:PORT --pin-arm64`, review/dry-run the new manifest, then coordinate the change. Topics are `cct.traces`, `cct.metrics`, `cct.logs` with `otlp_json`. Use this plaintext option only on the team's approved internal transport. Broker operation, consumer counts, availability timestamps, restart recovery and Spark/Parquet belong to Param's integration gate.
- **Mithil.** The shopping receipt's `lineage.server_records` and `lineage.edges` preserve trace, parent, duration and direction for each matched journey. Service windows and the candidate label rule are defined in [CONTRACT.md](CONTRACT.md). The existing synthetic-only demo schema stays version 1.0. Live records use the separate receipt format.
- **Overhead.** Maitreya runs the existing 5-user, seed-42, 120 s warm-up / 900 s measured workload in isolated, paired off/on windows. Alternate order across at least three matched pairs when feasible. Preserve hardware, workload code hashes, fixed app versions, sampling policy and raw run directories. Pass actual directories to `python3 observability/overhead.py OFF_DIR ON_DIR`. Report per-pair p95 latency, throughput, mean VM CPU and peak VM memory with signed percentage changes. Perform off/on comparisons under the same application binaries when estimating instrumentation cost. A pre-patch vs post-patch comparison measures bundled deployment overhead. The September result is a historical reference; no instrumented counterpart has been measured.
- **CCT-11.** Archive the manifest revision, exact running images, UTC interval, both captures, workload counts, collector refusal/export-failure series, missing services and accepted signal counts. Param must reconcile Kafka and Parquet counts. Komalika supplies independent fault timing/recovery. Mithil supplies graph/forecast output. The 3-journey probe alone cannot close the integrated demonstration.

## Offline checks

```sh
python3 -m cct check
python3 -m unittest discover -s tests -v
python3 -m cct demo
python3 -m observability.fixture
python3 -m observability.export_sample outputs/sample-otlp
```

The ablation has three fixed trace topologies. Counting expected edges separately per journey, direct SERVER-parent matching obtains 0/15 and walking CLIENT spans to the nearest SERVER obtains 15/15, with zero unexpected edges. All RPCs in this fixture have an intermediate CLIENT span by construction. This is a transformation check. It cannot estimate forecasting accuracy, live coverage or speedup.
