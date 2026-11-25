# Collecting evidence from Online Boutique

Merul implemented trace collection and journey verification (CCT-03), service metrics and selected-log capture (CCT-04), and a candidate health-label rule for review with Mithil (CCT-10). The offline tests cover trace reconstruction, signal handling, and cutoff timing. The shared-cluster acceptance criteria are complete traces for browse, cart, and checkout, plus timestamped metrics and logs that match workload activity. Native image builds, live signal delivery, and collection overhead still require verification.

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

The pinned Boutique source supports tracing in frontend, checkout, currency, payment, catalog, email and recommendation. The initial rollout enables only tracing, keeps profilers off as originally configured, and targets the in-cluster Collector. The Node clients receive an HTTP-form gRPC address. The rollout script generates commands for each affected Deployment. Review it and record each affected Deployment's current revision (`kubectl -n cct-app rollout history deployment/NAME`) and image before applying. This step requires the deployment administrator’s project kubeconfig and explicit kind context.

```sh
python3 observability/owner.py > outputs/telemetry/owner-seven.sh
# Deployment administrator, after review and Collector readiness
sh outputs/telemetry/owner-seven.sh
```

Three additional variants supply missing server-side instrumentation. Cart registers ASP.NET Core instrumentation and the OTLP exporter, with trimming disabled in the traced build. Shipping installs a gRPC stats handler, W3C propagation and a Go OTLP batch exporter. Ads use the versioned, SHA-256-checked OpenTelemetry Java agent with only trace export enabled. Redis remains a datastore outside the ten application-service tracing targets. The [patch](boutique-v0.10.7.patch) applies to Boutique commit `5b608cb1dfe317d05e4f1bb8c99b7618452fde87`. Source context is Apache-2.0; retain upstream notices. Source patch applicability was checked against the pinned files. Image build, runtime compatibility, resources and true path coverage are open gates.

```sh
# Build on the prepared cluster host after reviewing the native variants
python3 observability/build_images.py
python3 observability/owner.py --images .local/telemetry/image-lock.json > outputs/telemetry/owner-ten.sh
# Review image digests, resources and environment. Run during the agreed window.
sh outputs/telemetry/owner-ten.sh
```

The builder clones the pinned Boutique source into a temporary directory, applies the patch, builds/loads three new image names and records digests. It preserves the original Boutique image lock. If one rollout fails, stop and inspect its events. For each changed application Deployment, the deployment administrator can restore the recorded revision with `kubectl --context kind-cloud-chaos-twin -n cct-app rollout undo deployment/NAME --to-revision=NUMBER`, then wait for readiness and rerun the original shopping smoke check. Save receipts first. At the end of the agreed test window, the monitoring maintainer can remove the pilot resources with `kubectl -n cct-observability delete -f <rollout-directory>/stack.json` for the recorded installation.

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
