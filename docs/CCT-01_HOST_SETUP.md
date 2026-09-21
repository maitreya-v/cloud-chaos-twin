# CCT-01: Maitreya's host inventory and setup

Recorded on 2026-09-20. This record covers Maitreya's current Mac. Other teammates' hardware has not been inspected or inferred.

**Status: Maitreya's hardware inventory, initial host choice, and local environment setup are complete.** Docker and the dedicated Kubernetes cluster passed smoke checks. Application deployment and the baseline remain CCT-02. A [sanitized verification record](evidence/CCT-01-maitreya.json) contains the measured results and exact versions.

## Hardware and host choice

| Item | Observed value |
| --- | --- |
| Owner | Maitreya |
| Operating system | macOS 27.0, build 26A428 |
| Processor | Apple M5 |
| Architecture | arm64 (Apple Silicon) |
| Physical / logical CPU cores | 10 / 10 |
| Physical memory | 16 GiB |
| Free disk before installation | Approximately 340 GiB |
| Free disk after setup | Approximately 335 GiB |
| Hardware virtualization | Available (`kern.hv_support=1`) |
| Account permissions | Member of the local admin group; password-dependent operations still require the user |
| Existing development tools | Git and Python 3.9.6 |

Select this Mac as the **initial local development and staged experiment host**. Collect application telemetry in one session; run heavier Kafka/Spark processing separately until measurements demonstrate sufficient headroom. The 16 GiB memory capacity is below the project's preferred 32 GiB combined-stack allowance. Host selection does not establish application stability or fault-injection compatibility: those remain live validation gates.

No paid cloud resources are needed for this initial setup. Other teammates can contribute hardware information later, particularly if a larger shared experiment host is available.

## Installation record

| Tool | Selected version | Status |
| --- | --- | --- |
| Docker Desktop, Apple Silicon | 4.91.0 (build 239619), Engine 29.8.0 | Installed, running, and Linux container test passed |
| kind | 0.33.0 | Installed in `.local/bin`; version command passed |
| kubectl | 1.36.4 | Installed in `.local/bin`; client version command passed |
| Helm | 4.3.0 | Installed in `.local/bin`; version command passed |
| Kubernetes | 1.36.4 | One Ready control-plane node; all 9 system pods Ready |

The three command-line tools were downloaded from their official release locations, checked against the published SHA-256 checksums, and executed successfully. Download records are in the local, ignored `.local/tool-downloads.json`. Docker's verified installer checksum is `31a324e8f72acf178c9f5d46cd71240705541faa69827588813e539d29c7e859`.

Use the project-local tools in a Terminal session, from the repository root:

```sh
source .local/env
docker version
kind version
kubectl version --client
kubectl --context kind-cloud-chaos-twin get nodes
helm version --short
python3 -m cct doctor
```

The local activation file selects this project's tools and kubeconfig for the current Terminal session. It does not edit global shell configuration. Homebrew is not needed for this installation. `.local/env` and the tools are specific to this Mac and excluded from Git; on another host, install the tools using the linked official instructions.

## Runtime settings and readiness

Docker reports **10 virtual CPUs and 8,319,504,384 bytes (about 7.75 GiB) of usable VM memory**. This is the observed running allocation; no custom CPU or memory setting was applied. The host has approximately 335 GiB free after setup. Docker's virtual disk capacity has not been independently recorded. Keep at least 64 GiB host storage available as an initial project allowance, and monitor usage during CCT-02. Leave memory for macOS and reduce competing applications during experiments.

The project uses kind cluster `cloud-chaos-twin`, context `kind-cloud-chaos-twin`, and the ignored `.local/kubeconfig` (file permissions 0600). No global kubeconfig was changed. Docker Desktop's separate built-in Kubernetes cluster was not enabled by this setup. Keep it disabled to avoid running two clusters. The Kubernetes API uses kind's default loopback binding; future application port forwarding should also stay local.

The node image is pinned to:

```text
kindest/node:v1.36.4@sha256:099e049362a1526b2db71494e1947aae99bd16290d7c895f2b7ea312e3cbfaed
```

The new-cluster creation command used was:

```sh
source .local/env
kind create cluster --name cloud-chaos-twin --config infra/kind.yaml \
  --image kindest/node:v1.36.4@sha256:099e049362a1526b2db71494e1947aae99bd16290d7c895f2b7ea312e3cbfaed \
  --kubeconfig .local/kubeconfig --wait 180s
```

The cluster already exists on this Mac; use the inspection commands above instead of recreating it. Docker and the cluster remain running. Quit Docker Desktop when finished to release the VM's resources; open Docker Desktop before the next session.

- [x] Hardware inventory recorded and initial host selected.
- [x] Kubernetes command-line tools installed and version checked.
- [x] Existing offline fixture checks and all five tests pass.
- [x] Docker Desktop installed and engine responding.
- [x] Linux container smoke test passes (`hello-world`, arm64).
- [x] Docker CPU and memory allocation recorded from the running engine.
- [x] Project Kubernetes node and all nine system pods are Ready, with zero container restarts at verification.
- [x] A disposable BusyBox pod successfully resolved `kubernetes.default.svc.cluster.local`, exited successfully, and was removed.

The smoke images' resolved digests are saved in the verification record. The original Docker test output and Kubernetes test-pod record are retained locally in `.local/evidence/`. Neither smoke test is an application performance measurement.

CCT-02 remains separate: pin and deploy Online Boutique, verify browse/cart/checkout, and record a 15-minute healthy baseline. Passing the environment checks above does not complete CCT-02 or validate the full telemetry/chaos stack.

## Sources

- [Docker Desktop installation for Mac](https://docs.docker.com/desktop/setup/install/mac-install/)
- [Docker Desktop release notes](https://docs.docker.com/desktop/release-notes/)
- [kind 0.33.0 release and node-image digests](https://github.com/kubernetes-sigs/kind/releases/tag/v0.33.0)
- [kubectl installation on macOS](https://kubernetes.io/docs/tasks/tools/install-kubectl-macos/)
- [Helm 4.3.0 release](https://github.com/helm/helm/releases/tag/v4.3.0)
