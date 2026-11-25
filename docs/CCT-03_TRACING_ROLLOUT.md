# Boutique tracing rollout

Owner: Maitreya. Evidence recorded on 2026-11-25 UTC. The shared deployment is ready for Merul's independent shopping verification. See the [sanitized receipt](evidence/CCT-03-tracing-rollout.json) and [six-image lock](../configs/boutique.telemetry.lock.json).

## Observed result

| Check | Result |
| --- | --- |
| Application readiness | Ten application services and Redis Ready |
| Tracing destination | `cct-collector.cct-observability.svc:4317` |
| Native variants | Six `linux/arm64` images loaded into kind and pinned by digest |
| Browse trace | 4/4 declared service relationships, complete parent chain |
| Cart trace | 2/2 declared service relationships, complete parent chain |
| Checkout trace | 9/9 declared service relationships, complete parent chain |
| Service metrics | Positive call-counter deltas for all ten services |
| Prometheus | Collector and service targets both up |
| Shopping smoke | Browsing, quantity changes, simulated checkout and empty cart passed |
| Offline checks | 26 unit tests passed; normalized-span check passed |
| Configuration validation | Pinned Collector and Prometheus binary checks passed |

The readiness probe uses synthetic customer details and exact trace IDs. Its interval, trace IDs, images, deployment revisions and counter deltas are retained in the receipt. Relationship counts are counted separately per journey. Counter deltas can include concurrent traffic and health requests; these checks establish working collection, not isolated throughput or collection overhead.

## Runtime fixes

The rollout starts from merged telemetry PR #4, commit `f16d60a218d3b62a4320c0118ef1ac106580dce4`, which was merged into the deployment branch. The pinned Boutique source remains `5b608cb1dfe317d05e4f1bb8c99b7618452fde87`.

- Ads initially failed because Docker's downloaded-file mode also left `/opt/otel` without directory traversal permission. The patch sets the directory to `0755`. The builder verifies Java-agent startup as UID 1000 before importing the image. The previous healthy ads pod remained serving during repair.
- Cart's hosting and OTLP exporter packages now use OpenTelemetry 1.15.3 after NuGet flagged the original 1.12.0 dependencies. The upstream advisory links are in the [runbook](../observability/README.md).
- Currency and payment initially produced no server spans with the legacy 0.26.0 exporter. Their imports and package locks now select `@opentelemetry/exporter-trace-otlp-grpc` 0.222.0, matching the SDK. Each image must pass an actual SDK-to-OTLP/gRPC serialization check before import.
- Checkout's currency RPC used `context.TODO()`, detaching it from the shopping trace. Passing its existing request context restores the missing checkout-to-currency relationship.
- The owner renderer accepts the original three-image lock, the five-image intermediate lock and the final six-image lock. It emits one patch and readiness wait per service, including services that also appear in the rebuilt-image map.

The original image lock is preserved. Full build logs, initial negative probe receipts, final raw receipts and before/after deployment snapshots remain in ignored local storage. Only the compact sanitized receipt is committed.

## Handoff and recovery

Merul can run a new warm-up and recorded browse/cart/checkout probe using the [runbook](../observability/README.md). Keep the raw receipt, then capture selected timestamped logs and pod gauges from that exact interval. Verify downstream log delivery and record the paired collection-overhead measurement separately. CCT-10 threshold calibration with Mithil remains a later gate. This owner readiness check does not mark all of CCT-04 or CCT-10 complete.

The host has the imported image bytes; another kind cluster must build or import them before applying the recorded digest names. The builder checks platform and runtime compatibility but does not perform a complete dependency-security audit. Existing upstream npm dependency warnings remain outside this tracing fix.

All ten affected application deployments were at revision 1 before this rollout. To recover one service, use the owner's kubeconfig and explicit project context:

```sh
kubectl --context kind-cloud-chaos-twin -n cct-app rollout undo deployment/NAME --to-revision=1
kubectl --context kind-cloud-chaos-twin -n cct-app rollout status deployment/NAME --timeout=180s
```

Restore each affected service if ending the entire tracing pilot, then rerun the shopping smoke check. Capture monitoring evidence before stopping the ephemeral Jaeger/Prometheus stack. The private website's managed connection reconnects after the frontend pod changes; a request that reaches the replaced pod can fail during that transition.
