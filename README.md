# Cloud Chaos Twin

**Group 1 · AMS 560.01 / CSE 542.01 · Fall 2026**

Cloud Chaos Twin studies how failures spread through Online Boutique, a microservice shopping application. The project has two objectives. The first is to reconstruct which services handled a request and became unhealthy during a recorded fault. The second is to test whether signals available at a given time can predict which services will be affected next.

## Implementation and evidence

| Component | What the team has verified | Remaining work |
| --- | --- | --- |
| Testbed and workload | Maitreya deployed Online Boutique on an ARM64 kind cluster and verified browsing, cart updates, and checkout. A five-user baseline recorded 1,000 successful requests and 46 checkouts. | Repeat the workload with telemetry enabled to measure collection overhead. |
| Traces, metrics, and logs | The [live rollout](docs/CCT-03_TRACING_ROLLOUT.md) recovered 4/4 browse, 2/2 cart, and 9/9 checkout relationships. Merul independently verified the journeys, service counters, pod gauges and selected log delivery using the [recorded verification workflow](observability/README.md#record-an-independent-shopping-verification). | Measure collection overhead with matched instrumentation-off and instrumentation-on runs. |
| Health labels and forecasting | Merul implemented a candidate rule for classifying services as healthy, affected, or unknown using observations available at the prediction cutoff. | Merul and Mithil review the rule together and calibrate thresholds on recorded runs. Forecast accuracy remains unmeasured. |
| Processing and experiments | The repository includes sample telemetry, a normalized-span schema, an offline dependency graph and report, and a scoped fault template. | Validate Kafka/Spark replay, Parquet counts, fault and recovery records, and an integrated experiment. |

The [deployment record](docs/CCT-02_LOCAL_DEPLOYMENT.md) documents the baseline and its timing precision. The [task board](docs/TEAM_TASKS.md) defines the acceptance criteria for each deliverable.

## Repository and frozen version

The public repository is [maitreya-v/cloud-chaos-twin](https://github.com/maitreya-v/cloud-chaos-twin). The tag `merul-telemetry-2026-11-24` identifies the tested telemetry implementation at commit `cfab3d0ce1af576aaa3bbba72276322c8c361c66`. The tag reproduces the November 24 offline milestone. The [November 25 integrated revision](https://github.com/maitreya-v/cloud-chaos-twin/tree/e764ca675bb8a5ce3e75bb3e7fe9e00345028adf) adds the live tracing fixes, rollout tool and independent verifier. The slides and report linked here describe the November 24 milestone. [Merged telemetry PR](https://github.com/maitreya-v/cloud-chaos-twin/pull/4) · [Presentation slides](docs/submission/Cloud-Chaos-Twin-Slides.pdf) · [Project report](docs/submission/Cloud-Chaos-Twin-Report.pdf)

## Installation and reproduction

For the current code, clone the repository and stay on `main`. The live verifier runs with `python3 -m observability.record` after cluster setup in the [telemetry runbook](observability/README.md). To reproduce the frozen offline milestone, use the checkout below. The offline workflow requires Git and Python 3.9 or newer. It uses the Python standard library. Run the commands from a shell, using WSL or the `py -3` launcher for Python on Windows.

```sh
git clone https://github.com/maitreya-v/cloud-chaos-twin.git Cloud-Chaos-Twin
cd Cloud-Chaos-Twin
git switch --detach merul-telemetry-2026-11-24
git rev-parse HEAD
python3 -m cct doctor
python3 -m cct check
python3 -m unittest discover -s tests -v
python3 -m cct demo
python3 -m observability.fixture
python3 -m observability.export_sample outputs/sample-otlp
python3 observability/stack.py > outputs/stack.review.json
```

For reproduction from an existing Git bundle, replace the clone command with `git clone -b feature/merul-telemetry Cloud-Chaos-Twin.bundle Cloud-Chaos-Twin`.

| Verification | Expected output at the frozen revision |
| --- | --- |
| Revision and tests | The commit matches the SHA above and all 25 unit tests pass. |
| Offline demo | Nine synthetic spans produce `report.html`, `graph.json`, and `service_summary.csv` in `outputs/demo/`. Open the HTML file in a browser. |
| Journey reconstruction | Walking through client spans to the nearest server ancestor recovers 4 browse, 2 cart, and 9 checkout relationships. Direct server-parent matching recovers 0 of 15 because the fixtures place a client span between services. |
| Telemetry samples and manifests | `outputs/sample-otlp/` contains synthetic OTLP records for the processing pipeline. `outputs/stack.review.json` contains the rendered Kubernetes resources for review. |

## Architecture and execution path

The monitoring stack accepts application spans over OTLP. The Collector sends full traces to Jaeger and derives request counts and duration histograms from server spans for Prometheus. During a shopping test, the probe retrieves the trace for each request by ID and checks its caller–callee relationships. A separate reader captures selected application logs, pod readiness, and restart counts for the workload evidence. The [telemetry runbook](observability/README.md) covers scoped deployment, native image builds, verification, and rollback. The [signal contract](observability/CONTRACT.md) defines identities, timestamps, units, and missing-data rules.

The diagram shows the planned end-to-end experiment. Kafka export is optional in the monitoring configuration. The processing stage has passed local synthetic and recorded healthy-data replay. Fault-run integration and prediction evaluation remain pending.

```mermaid
flowchart TD
  W[Locust workload] --> A[Online Boutique]
  F[Controlled fault] --> A
  A --> O[Traces, metrics and selected logs]
  O --> K[Kafka]
  K --> S[Spark and Parquet]
  S --> G[Temporal service graph]
  G --> E[Root cause and forecast evaluation]
  E --> D[Dashboard]
```

A dependency edge points from the calling service to the service it calls. A failure in a downstream service can affect its callers. Reconstruction uses observations from the recorded run. Forecast evaluation uses only information available by the prediction cutoff.

## Ownership and source guide

| Team member | Responsibility |
| --- | --- |
| Maitreya Vaghulade | Testbed deployment, workload, architecture, dashboard integration, and shared experiments |
| Merul Shah | Trace propagation, service metrics, selected logs, telemetry quality, and health-label review |
| Param Shah | Kafka ingestion, Spark processing, Parquet storage, processing benchmarks, and evaluation tables |
| Komalika Acharya | Fault experiments, experiment runner, recovery checks, and ground-truth records |
| Mithil Bavishi | Dependency graphs, health-label design, cascade analysis, and forecast evaluation |

| Location | Purpose |
| --- | --- |
| `cct/`, `tests/` | Offline analysis, demo generation, and automated checks |
| `scripts/`, `infra/`, `workload/` | Testbed setup, shopping checks, and baseline workload |
| `observability/` | Monitoring configuration, application tracing patches, journey verification, and candidate health rule |
| `data/samples/`, `schemas/`, `configs/` | Synthetic inputs, data contracts, and experiment configuration |
| `ingestion/`, `processing/`, `chaos/`, `analytics/`, `dashboard/` | Component plans, interfaces, and experiment templates |
| `docs/`, `outputs/` | Versioned evidence and submission files in `docs/`. Generated local results in `outputs/`. |

See the [setup guide](docs/SETUP.md), [cluster access guide](docs/PRIVATE_ACCESS.md), and [project plan](docs/PROJECT_PLAN.md) for deployment requirements and milestones. The [contribution guide](CONTRIBUTING.md), [data guide](data/README.md), and [sharing guide](docs/SHARING.md) cover review, storage, and repository access.

### Kafka / Spark / Parquet replay (Param)

The staged pipeline implementation, local setup and acceptance commands are in [ingestion/README.md](ingestion/README.md); table contracts and cutoff semantics are in [processing/README.md](processing/README.md). See [CCT-05/06 status](docs/CCT-05-06_PIPELINE.md) for measured local acceptance and scalability results, limitations and remaining fault-run integration gates. The offline starter demo does not start Kafka or Spark.
