# Team task board

Update these checkboxes through pull requests. Owners are from the proposal; GitHub handles have not yet been collected, so no automatic account assignments are configured.

## Initial setup

- [x] Create repository structure, onboarding, and contribution guide.
- [x] Add normalized-span contract and synthetic fixture.
- [x] Add runnable offline graph/summary report and validation checks.
- [x] Add initial infrastructure/observability/fault templates.
- [ ] All teammates can access the repository and run the offline demo.
- [x] Record Maitreya's hardware and select the initial staged experiment host; see [CCT-01 host record](CCT-01_HOST_SETUP.md). Other teammates' hardware remains uncollected.
- [ ] Validate a pinned live stack; record exact versions.

## First deliverables

| ID | Owner | Task | Depends on | Done when |
| --- | --- | --- | --- | --- |
| CCT-01 | Maitreya | Hardware inventory and host selection | Team replies | OS/architecture/RAM/disk recorded and host chosen |
| CCT-02 | Maitreya | Pin and deploy Online Boutique | CCT-01 | Browsing/cart/checkout and 15-minute baseline succeed |
| CCT-03 | Merul | Collector, trace propagation, Jaeger | CCT-02 | Complete traces for the three core user journeys |
| CCT-04 | Merul | Metrics, logs, schema review | CCT-02 | Timestamped service data matches workload activity |
| CCT-05 | Param | Kafka ingestion from a sample | Schema review | Read/write/replay a sample with recorded counts |
| CCT-06 | Param | Spark normalization and Parquet | CCT-05 | Correct feature rows; restart does not double counts |
| CCT-07 | Komalika | Scoped fault and recovery check | CCT-02 | Effective fault interval and cleanup verified three times |
| CCT-08 | Komalika | Experiment runner and manifest | CCT-07 | Warm-up/baseline/fault/recovery recorded automatically |
| CCT-09 | Mithil | Real trace graph and baseline metrics | CCT-03 | Service edges verified against request traces |
| CCT-10 | Mithil + Merul | Health labels and forecast timing | CCT-04, CCT-08 | Cutoffs, labels, missing-data rules, and horizons documented |
| CCT-11 | Maitreya + all | First integrated demo | CCT-06, CCT-08, CCT-09 | One saved real run reproduces a graph and timeline |

### CCT-01 progress — 2026-09-20

Maitreya's host inventory, host selection, and prerequisite setup are complete. The Apple M5 Mac (16 GiB RAM, 10 cores, approximately 335 GiB free after setup) is selected for initial staged development and experiments. Docker's container test passed; the pinned Kubernetes 1.36.4 node and all nine system pods are Ready; a disposable pod passed the cluster DNS check. The [host record](CCT-01_HOST_SETUP.md) links the measured evidence and activation instructions. Team-wide hardware collection is still pending; no other member's capacity has been assumed.

### CCT-02 progress — 2026-09-20

Completed locally: pinned Online Boutique v0.10.7 is deployed with native ARM64 images on the selected Mac. Browsing, cart quantity changes, and simulated checkout passed. The nominal 15-minute baseline completed with five users, 1,000 successful requests, 46 checkouts, no new restarts, and 79.5% minimum VM memory headroom. The [deployment record](CCT-02_LOCAL_DEPLOYMENT.md) documents the 1.062 ms timer discrepancy, accepted precision tolerance, cleanup correction, and [measured evidence](evidence/CCT-02-maitreya.json). The full-stack acceptance gate remains open.

### Private-access progress — 2026-09-21

Private Boutique HTTPS, the official authenticated Kubernetes proxy, and the approved individual permissions are installed. The owner's sign-in works through Tailscale; all 11 application pods are healthy. Forty authorization checks and twenty integration/admission checks passed. The owner sent the four approved Member invitations, and the reloaded console confirms all four are Invited as Members. Acceptance, any required owner approval, and a teammate test from another Internet connection remain pending. See the [connection guide](PRIVATE_ACCESS.md) and [sanitized evidence](evidence/PRIVATE_ACCESS-maitreya.json).

Param can start with fixtures while live collection is being built. Mithil can use the existing synthetic graph interface before real traces arrive. Maitreya supports dashboard wiring; Param supports evaluation tables; Merul reviews telemetry and health labels.

## Weekly gates

| Week | Required evidence |
| --- | --- |
| 1 | Healthy testbed, selected host, scoped fault compatibility |
| 2 | Traces/metrics/logs and repeatable fault/recovery walkthrough |
| 3 | Kafka -> Spark -> Parquet on real data, healthy service graph |
| 4 | Validated fault library, schemas, labels, and cleanup |
| 5 | Pilot dataset and experiment-to-graph MVP |
| 6 | Root-cause/cascade baselines and cutoff-aware forecast protocol |
| 7 | Main experiments, controls, repeated and reserved test runs |
| 8 | Accuracy, replay/scale benchmarks, failure analysis |
| 9 | Feature freeze, report, dashboard, demo rehearsal |
| 10 | Reproduction, buffer, final packaging |

Do not mark tasks complete solely because a manifest exists. Link the observed run, output, or reproducible check that passed its gate.
