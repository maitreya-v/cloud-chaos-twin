# Collecting evidence from Online Boutique

Merul implemented trace collection and journey verification (CCT-03), service metrics and selected-log capture (CCT-04), and a candidate health-label rule for review with Mithil (CCT-10). The offline tests cover trace reconstruction, signal handling, and cutoff timing. The shared-cluster acceptance criteria are complete traces for browse, cart, and checkout, plus timestamped metrics and logs that match workload activity. Maitreya documented the native image builds, live traces and service counters in the [host rollout record](../docs/CCT-03_TRACING_ROLLOUT.md). Merul independently verified the three journeys, pod gauges and selected log delivery. The paired collection-overhead measurement remains open.

## Cluster access and prerequisites

Live checks require kubectl, Tailscale, and an individual account with the project’s Kubernetes permissions. The commands below use the project kubeconfig. On macOS, kubectl is available through `brew install kubectl`. See the [cluster access guide](../docs/PRIVATE_ACCESS.md) for account setup.

```sh
mkdir -p "$HOME/.kube"
export KUBECONFIG="$HOME/.kube/cct-team.yaml"
tailscale configure kubeconfig cct-kubernetes
kubectl auth whoami
kubectl get pods -n cct-app
kubectl auth can-i create deployments.apps -n cct-observability
kubectl auth can-i get pods/log -n cct-app
```

Verify that the reported identity matches the signed-in team member and that the permission checks succeed. The monitoring role can deploy in `cct-observability` and read application logs in `cct-app`. Changes to application workloads require the deployment administrator. Run the following commands from the repository root. The shared testbed has 16 GiB of memory, so schedule monitoring, data processing, and fault experiments in separate windows.

## Deploy and verify the monitoring stack

The monitoring maintainer deploys the Collector, Jaeger, and Prometheus directly in `cct-observability`. The deployment administrator handles changes in `cct-app`. With the project Tailscale account connected, run from the repository root:

```sh
python3 -m observability.rollout
```

The command requires Python 3.9+, kubectl, and the Tailscale CLI. It selects the project kubeconfig, prints the authenticated Kubernetes identity, renders the eight namespace-scoped resources with pinned Linux ARM64 images, checks them with a server-side dry run, and applies them. Re-running the command reconciles the same resources. Kubernetes reports field ownership conflicts for the maintainer to inspect.

Each of the three single-replica deployments requests 50m CPU and 64 MiB of memory and has limits of 500m CPU and 256 MiB. The pods run as non-root with a read-only root filesystem, dropped capabilities, the runtime-default seccomp profile, and service account token mounting disabled. Jaeger retains up to 1,000 traces in memory. Prometheus uses ephemeral storage and scrapes every five seconds. The Collector writes selected logs to rotating files on a bounded temporary volume. Archive measurements before a restart.

The rollout waits for the three deployments, then runs a temporary namespace Job to query Jaeger and check both Prometheus scrape targets. A successful run prints `MONITORING READY` and writes `ready.json`, the pinned manifest, and running image IDs under `outputs/telemetry/rollout-*/`. Share the latest successful receipt and the in-cluster OTLP/gRPC endpoint, `cct-collector.cct-observability.svc:4317`, with the deployment administrator. If a check fails, inspect the output and the corresponding run directory before retrying. Shopping-journey coverage and signal measurements follow the application rollout.

The standalone renderer remains available for inspection (`python3 observability/stack.py`) and can resolve current registry digests on a Docker/buildx host (`python3 observability/stack.py --pin-arm64`). For removal after the pilot, use the exact manifest archived by the rollout that installed the stack.

Prometheus derives request counts, error breakdowns and duration histograms from SERVER spans. CLIENT spans remain available in Jaeger. A gRPC health-check filter removes named health RPCs from span-derived metrics. The Collector's own scrape target records refusals and export failures when those series exist. Successful OTLP receipt and successful delivery to Jaeger or Kafka are distinct checks. Service CPU and memory require a separately authorized source. The pod API reader provides readiness and restart gauges.

## Application instrumentation and rollout

Frontend, catalog, email and recommendation use their existing native images. Six rebuilt variants cover ads, cart, shipping, currency, payment and checkout. The rollout enables tracing, preserves the original profiler settings, and targets the in-cluster Collector. The Node clients receive an HTTP-form gRPC address. The owner script prints a staged rollout with one patch per service. Review it and record each affected Deployment's current revision (`kubectl -n cct-app rollout history deployment/NAME`) and image before applying. Use the owner's project kubeconfig and explicit kind context for this step, with the Collector already Ready.

Cart registers ASP.NET Core instrumentation and the OTLP exporter, with trimming disabled in the traced build. Shipping installs a gRPC stats handler, W3C propagation and a Go OTLP batch exporter. Ads use the versioned, SHA-256-checked OpenTelemetry Java agent with only trace export enabled. Its agent directory must remain traversable by the non-root application user. Currency and payment use `@opentelemetry/exporter-trace-otlp-grpc` 0.222.0 to match their Node SDK; the original legacy 0.26.0 exporter did not deliver their spans during the live probe. Both package locks are updated. Checkout passes its request context into the currency RPC rather than `context.TODO()`, which detached that call from the shopping trace. Redis remains a datastore outside the ten application-service tracing targets. The [patch](boutique-v0.10.7.patch) applies to Boutique commit `5b608cb1dfe317d05e4f1bb8c99b7618452fde87`. Source context is Apache-2.0; retain upstream notices. The exporter is documented in the [OpenTelemetry JavaScript repository](https://github.com/open-telemetry/opentelemetry-js/tree/main/experimental/packages/exporter-trace-otlp-grpc).

The cart patch pins OpenTelemetry hosting and the OTLP exporter to 1.15.3. This brings in the fixed API header parser and fixes the exporter's experimental disk-retry defaults; see the upstream [API advisory](https://github.com/open-telemetry/opentelemetry-dotnet/security/advisories/GHSA-g94r-2vxg-569j) and [exporter advisory](https://github.com/open-telemetry/opentelemetry-dotnet/security/advisories/GHSA-4625-4j76-fww9). The original 1.12.0 pins produced NuGet vulnerability warnings during the native build.

```sh
# Build on the prepared cluster host after reviewing the native variants
python3 observability/build_images.py
python3 observability/owner.py --images .local/telemetry/image-lock.json > outputs/telemetry/owner-ten.sh
# Review image digests, resources and environment. Run during the agreed window.
sh outputs/telemetry/owner-ten.sh
```

The builder clones the pinned Boutique source into a temporary directory, applies the patch, builds and loads six traced images, and records their digests. It checks Java-agent startup under the application UID and sends an SDK span to a local OTLP receiver in each rebuilt Node image. Other hosts must build or import the images listed in the [six-image lock](../configs/boutique.telemetry.lock.json). It preserves the original Boutique image lock. If one rollout fails, stop and inspect its events. For each changed application Deployment, the deployment administrator can restore the recorded revision with `kubectl --context kind-cloud-chaos-twin -n cct-app rollout undo deployment/NAME --to-revision=NUMBER`, then wait for readiness and rerun the original shopping smoke check. Save receipts first. At the end of the agreed test window, the monitoring maintainer can remove the pilot resources with `kubectl -n cct-observability delete -f <rollout-directory>/stack.json` for the recorded installation.

## Record an independent shopping verification

Once the monitoring stack and application tracing are ready, run this command from the repository root.

```sh
python3 -m observability.record
```

The command uses the team kubeconfig (`~/.kube/cct-team.yaml`), prints the caller’s Kubernetes identity, and exercises browse, cart, and checkout twice. Warm-up primes the span and exporter counters. The measured run checks each request’s trace relationships, changes in service call counts, selected timestamped frontend and checkout events, and pod-state gauges. Reserve a quiet window so other traffic does not contribute to the exporter count.

Each run gets a private directory under `outputs/telemetry/`. The terminal prints the measured `summary.json` path. The relay checks OTLP acceptance and compares the number of selected records with the change in the file exporter’s sent-log counter on the same Collector instance. A missing baseline, changed Collector instance, or count mismatch leaves delivery unverified. The receipt records count-level delivery evidence. The overhead comparison requires a separate pair of workload runs.

## Verify browse, cart, and checkout traces

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

Use an unmeasured warm-up probe first so the workload counters already exist; a newly created series has an unknown starting count. For the next measured Job choose a new RUN. Inspect any Job failure via `kubectl -n cct-observability describe job/"$RUN"` and inspect the specific pod's events if the container never started. A partial rollout can leave gaps. Keep the receipt when a relationship is missing. For ad, cart and shipping, a CLIENT span alone is insufficient. Client-to-server waits can take up to 45 seconds after shopping completes. The receipt includes times, trace IDs, server IDs, parent-server IDs, operations, durations in microseconds, frontend root counts, connected edges, and before/after Prometheus sample values. Cross-service caller identity is not proof of fault causation.

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

If an authorized Collector HTTP endpoint is already available, use `signals.py --otlp-http URL` when making the capture instead. A retry after a partial two-signal send may duplicate records. Deduplicate by log event ID or metric series/timestamp. For local inspection, the deployment administrator can forward Jaeger and Prometheus to a loopback interface. Shared dashboards or additional port-forward permissions require separate configuration.

## Interfaces, quality and forecasting

- **Kafka and Spark (Param).** Run `python3 -m observability.export_sample outputs/sample-otlp` for nested OTLP/JSON samples. They contain 19 spans, two pod gauges and one sanitized log, all synthetic. Review the [signal contract](CONTRACT.md) together before enabling the optional Kafka branch. Pass the agreed private broker to `stack.py --kafka BROKER:PORT --pin-arm64`, review/dry-run the new manifest, then coordinate the change. Topics are `cct.traces`, `cct.metrics`, `cct.logs` with `otlp_json`. Use this plaintext option only on the team's approved internal transport. Broker operation, consumer counts, availability timestamps, restart recovery and Spark/Parquet belong to Param's integration gate.
- **Dependency graph and health labels (Mithil and Merul).** The shopping receipt's `lineage.server_records` and `lineage.edges` preserve trace, parent, duration and direction for each matched journey. Service windows and the candidate label rule are defined in [CONTRACT.md](CONTRACT.md). The existing synthetic-only demo schema stays version 1.0. Live records use the separate receipt format.
- **Collection overhead (Maitreya and Merul).** The measurement protocol uses the existing 5-user, seed-42, 120 s warm-up / 900 s measured workload in isolated, paired off/on windows. Alternate order across at least three matched pairs when feasible. Preserve hardware, workload code hashes, fixed app versions, sampling policy and raw run directories. Pass actual directories to `python3 observability/overhead.py OFF_DIR ON_DIR`. Report per-pair p95 latency, throughput, mean VM CPU and peak VM memory with signed percentage changes. Perform off/on comparisons under the same application binaries when estimating instrumentation cost. A pre-patch vs post-patch comparison measures bundled deployment overhead. Maitreya recorded the September baseline. A matched run with instrumentation enabled is pending.
- **Integrated experiment (CCT-11).** Archive the manifest revision, exact running images, UTC interval, both captures, workload counts, collector refusal/export-failure series, missing services and accepted signal counts. Param must reconcile Kafka and Parquet counts. Komalika supplies independent fault timing/recovery. Mithil supplies graph/forecast output. The 3-journey probe alone cannot close the integrated demonstration.

## Offline checks

```sh
python3 -m cct check
python3 -m unittest discover -s tests -v
python3 -m cct demo
python3 -m observability.fixture
python3 -m observability.export_sample outputs/sample-otlp
```

The ablation has three fixed trace topologies. Counting expected edges separately per journey, direct SERVER-parent matching obtains 0/15 and walking CLIENT spans to the nearest SERVER obtains 15/15, with zero unexpected edges. All RPCs in this fixture have an intermediate CLIENT span by construction. This is a transformation check. It cannot estimate forecasting accuracy, live coverage or speedup.
