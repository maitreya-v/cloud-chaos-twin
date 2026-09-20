# Setup and onboarding

## A. Start today: lightweight offline mode

Install Git and Python 3.9 or newer if absent. The starter uses the Python standard library, so no package installation is required. Python 3.11/3.12 are convenient choices for new environments; choose separate, pinned dependencies when adding Spark or other services.

Clone the repository and run the commands in the root README. The environment checker reports available commands, platform, and disk space; it does not install software, start Docker, connect to a cluster, or measure RAM. Missing Docker/Kubernetes tools do not prevent the offline demo.

Open the generated HTML report directly in your browser. Compare it with the fixture in `data/samples/`: healthy, fault, and recovery phases are deliberately invented. The graph builder accepts the starter's normalized server-span format, not raw OTLP messages.

## B. Record team hardware

Each teammate should add their specifications to a task or shared team note: OS, CPU architecture, RAM, available SSD space, container support, and administrative access. Avoid publishing device serial numbers or other personal machine identifiers.

Planning estimates: around 8 GB RAM supports coding/small samples; around 16 GB may support staged application collection and separate processing; a 32 GB host with roughly 8 cores and 100 GB free storage is preferred for an initial combined stack. Measure actual usage. No GPU is needed for the initial methods.

## C. Prepare the live environment

Maitreya owns the first validated deployment. Install a supported container runtime, kubectl, kind, and Helm using their official instructions. Test the exact host/architecture. Record selected tool versions and image digests in a copy of `configs/versions.example.json` before shared experiments.

- [Docker installation](https://docs.docker.com/get-started/get-docker/)
- [kubectl installation](https://kubernetes.io/docs/tasks/tools/)
- [kind quick start](https://kind.sigs.k8s.io/docs/user/quick-start/)
- [Helm installation](https://helm.sh/docs/intro/install/)
- [Online Boutique](https://github.com/GoogleCloudPlatform/microservices-demo)
- [Chaos Mesh installation](https://chaos-mesh.org/docs/production-installation-using-helm/)

Follow `infra/README.md` for cluster preparation. Pin and review the upstream application manifest, deploy the testbed, and verify browsing/cart/checkout before adding monitoring. Collector configuration alone does not instrument applications; each required service must emit spans and propagate context.

**Live acceptance gate:** stable healthy workload, complete traces for core user journeys, resource headroom, one verified scoped fault, cleanup, and recovery. Templates in this repository are starting points and have not yet passed this gate.

## D. Work on a smaller laptop

Capture real telemetry while the application runs, stop the application, replay the capture through Kafka, and process it with Spark. Alternatively, split collection and analytics across two hosts. Record the configuration and call the result a replay experiment. A live forecast claim requires timing evidence from a live configuration.

## Troubleshooting

| Symptom | Next step |
| --- | --- |
| `python3` unavailable | Install Python or use `py -3` on Windows. |
| `No module named cct` | Run from the cloned repository root. |
| Demo input validation error | Use the tracked sample first; check the normalized-span contract. |
| Docker/kind absent | Continue offline tasks; complete live installation before deploying templates. |
| Cluster starts but application is unstable | Inspect host/container memory limits and reduce load; do not label a broken baseline as a fault effect. |
| No distributed traces | Check application instrumentation, exporter endpoint, sampling, and context propagation. |

Keep collected datasets outside Git and track their location and checksum in an experiment index.
