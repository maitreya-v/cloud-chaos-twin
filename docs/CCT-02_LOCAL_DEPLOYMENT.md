# CCT-02: local Online Boutique deployment

Owner: Maitreya. Host: the Apple M5 Mac selected in CCT-01. This milestone covers local deployment, real shopping journeys, and a healthy baseline. Private team access remains a separate review step.

## Current status

Online Boutique v0.10.7 is deployed into `cct-app` on the existing `cloud-chaos-twin` kind cluster. All ten application services and Redis became Ready with zero restarts. The upstream continuous load generator is deliberately scaled to zero; the baseline uses its native image in a separately controlled pod.

The automated shopping test passed browsing, product details, cart quantity changes (one item, then three), simulated checkout, and an empty cart after checkout. Only synthetic customer and payment details are used. The nominal 15-minute baseline is complete and accepted with the timer tolerance documented below. The temporary workload has been removed; Boutique remains running for review.

## Observed baseline — 2026-09-20

The measured interval ran from 23:26:40 to 23:41:40 UTC after a two-minute warm-up. The [compact evidence record](evidence/CCT-02-maitreya.json) includes exact times, configuration hashes, and artifact locations.

| Check | Observed result |
| --- | --- |
| Concurrent users | 5 |
| Actual measured duration | 899.998938286 seconds; 900-second target |
| Workload requests / failures | 1,000 / 0 |
| Checkout requests | 46, all successful |
| Achieved request rate | 1.11 requests/second |
| Median / p95 response time | 12 ms / 19 ms, measured inside the cluster |
| Application components | All 11 Ready; zero application restarts |
| Health samples during measurement | 179; largest gap 5.01 seconds |
| Pod replacements, new restarts, OOM kills, node pressure | None observed |
| Peak Linux VM memory working set | 1.59 GiB of 7.75 GiB; 79.5% headroom |
| Mean / peak Linux VM CPU | 0.210 / 0.246 cores out of 10 |
| Automated code checks | 12 passed |

The original strict duration gate rejected the run because its event-loop timer fired 1.062 ms early. The reviewed gate allows 10 ms of timer precision, retains the exact elapsed time, and passes the unchanged measurements. The original `summary.json` is preserved beside `summary.reviewed.json` in `outputs/baselines/20260920T232434Z/`. Future timers explicitly recheck their monotonic deadline; regression checks reject a duration 20 ms short as well as materially shorter runs.

Result collection succeeded, but the original cleanup wait collided with Kubernetes' default 30-second shutdown grace period. The test pod finished deleting; the remaining temporary ConfigMap was removed and cleanup verified. Future test pods use a five-second shutdown grace period. Application pods were unaffected.

Per-request events reconcile to all 1,000 measured requests. Raw CSVs, logs, readiness/resource records, and run configuration remain in the baseline directory; the shopping evidence is in `outputs/boutique-smoke/20260920T232412Z/`. The local browser URL returned HTTP 200 after cleanup. These results establish a low-load local baseline; they do not establish full-stack capacity or an Internet-facing latency figure.

## Open the application

Open **http://127.0.0.1:8080** in your browser. The address is local to this Mac. The subsequent [private-access setup](PRIVATE_ACCESS.md) now maintains this connection with a per-user Mac startup job; do not start another connection on the same port while it is running.

For manual use on another prepared host, or after stopping that startup job, go to this repository and run:

```sh
source .local/env
kubectl --context kind-cloud-chaos-twin -n cct-app port-forward \
  --address 127.0.0.1 service/frontend 8080:80
```

Keep that Terminal running. Ctrl+C stops the browser connection while leaving the application deployed. If port 8080 is already occupied by the existing connection, use that connection instead of starting another. Start Docker Desktop before opening the application after a shutdown.

The Mac must stay awake with Docker running. Restarting Docker or the Mac interrupts the connection; the startup job retries after login while Docker and the pods recover. Manual connections must be restarted manually. The sample Redis cart uses ephemeral storage, so treat cart contents as disposable; experiment results are retained separately on the Mac under `outputs/`.

## Version and architecture record

- Upstream release: `v0.10.7`.
- Exact source commit: `5b608cb1dfe317d05e4f1bb8c99b7618452fde87`.
- Upstream deployment SHA-256: `ae3c20181c051048e6a5cae51fa530681affa4043ba60ae95d4f39e1262f2744`.
- Runtime platform: `linux/arm64` on kind/Kubernetes 1.36.4.
- Exact application image digests, build image IDs, Dockerfile hashes, Redis/BusyBox digests, and rendered deployment checksum: [image lock](../configs/boutique.local.lock.json).

The checked published frontend and cart images were `linux/amd64`. All eleven application/workload images were therefore built locally from the same pinned upstream source for `linux/arm64`. The upstream build configuration supports ARM64. Explicit build arguments select ARM64; the floating `gcr.io/distroless/static` base was resolved to an ARM64 digest and recorded. Other upstream base references retain their specified digests. No application source was changed.

Images were loaded directly into this kind node and registered under their digest names. Application containers use `imagePullPolicy: Never` and exact digest references. These images are local to this cluster; a fresh cluster needs them loaded again. Nothing was pushed to an external registry.

## Reproduce the deployment

The existing Mac already has the source and images. On a prepared ARM64 development host with Docker, kind, and kubectl, fetch the pinned upstream source if it is absent:

```sh
git clone --depth 1 --branch v0.10.7 \
  https://github.com/GoogleCloudPlatform/microservices-demo.git vendor/online-boutique
git -C vendor/online-boutique rev-parse HEAD
```

Verify that the output matches the exact commit above. After preparing the project-specific cluster and `.local/env` described in CCT-01:

```sh
set -e
source .local/env
python3 scripts/prepare_boutique.py
kubectl --context kind-cloud-chaos-twin create namespace cct-app --dry-run=client -o yaml \
  | kubectl --context kind-cloud-chaos-twin apply -f -
kubectl --context kind-cloud-chaos-twin apply --dry-run=server -f .local/boutique/manifest.json
kubectl --context kind-cloud-chaos-twin apply -f .local/boutique/manifest.json
kubectl --context kind-cloud-chaos-twin -n cct-app wait \
  --for=condition=Available deployments --all --timeout=240s
kubectl --context kind-cloud-chaos-twin -n cct-app get pods
```

The preparation script refuses a different or modified upstream checkout, checks the upstream deployment checksum, builds/loads images, and regenerates the local deployment and lock. Fresh native builds may produce new output digests; review and record the resulting lock before comparing experiments. Preserve image archives if exact existing build bytes must be transferred to another host.

The local deployment preserves upstream resource requests and limits. Across all twelve deployment definitions, including the disabled load generator, these total 1,368 MiB requested memory and 2,542 MiB memory limits. Actual runtime usage is measured separately. `frontend-external` is changed to `ClusterIP`; all resources are scoped to `cct-app`; service accounts are created before workloads.

## Re-run the acceptance checks

With the local port-forward active:

```sh
source .local/env
python3 scripts/smoke_boutique.py
caffeinate -i python3 -u scripts/baseline_boutique.py
```

The baseline uses five closed-loop users, one user spawned per second, the upstream weighted task mix, random seed 42, and upstream 1–10 second think times. A 120-second warm-up begins after all users have spawned. Statistics reset before a full 900-second measured interval. Fixed users are not a fixed arrival rate, and a random seed does not make concurrent timing identical between runs.

The upstream task weights are home 1, currency selection 2, product browsing 10, adding to cart 2, viewing cart 3, and checkout 1. Adding to cart and checkout issue multiple HTTP requests, so task weights are not request percentages.

The runner retains per-request timings, Locust CSV statistics/history, logs, image versions, pod readiness/restarts, and Kubernetes runtime resource samples at approximately five-second intervals. Raw captures stay in the ignored `outputs/baselines/` directory. The test workload and its temporary configuration are removed after results have been copied; application services remain running.

Do not browse the application, change deployments, or run other workload/fault tools during a measured baseline. The baseline itself does not require an active browser port-forward because it uses the in-cluster frontend service.

Acceptance requires all core journeys to be exercised, a 900-second measurement (with at most 10 ms of timer tolerance), zero failed workload requests or Locust exceptions, no pod replacement/unreadiness/restarts/OOM kills, no node resource-pressure condition, and adequate sampling coverage. Actual elapsed time is always retained. Initial resource gates are at least 20% VM memory headroom and no CPU utilization over 80% sustained for 60 seconds. These thresholds are operational starting criteria, not application performance guarantees.

Readiness sampling cannot exclude every sub-second interruption. VM resource readings do not describe all macOS applications. Low-load latency percentiles are descriptive and should be accompanied by request counts. This milestone does not validate telemetry continuity, Chaos Mesh, or the combined Kafka/Spark stack.

## Upstream attribution

Online Boutique is from [GoogleCloudPlatform/microservices-demo](https://github.com/GoogleCloudPlatform/microservices-demo/tree/5b608cb1dfe317d05e4f1bb8c99b7618452fde87), licensed under Apache-2.0. Its original license and copyright notices are preserved in the downloaded `vendor/online-boutique` checkout and upstream files. The checkout and build logs are excluded from Git. Container base images retain their respective upstream licenses.
