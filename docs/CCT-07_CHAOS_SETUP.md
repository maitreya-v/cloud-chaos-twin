# Chaos Mesh setup and Komalika's initial fault scope

Owner: Maitreya. Verified on 2026-11-27. Installation, runtime compatibility, admission limits and Komalika's initial permissions are ready. CCT-07's three recorded Boutique fault runs and the CCT-08 experiment runner remain Komalika's deliverables.

See the [version lock](../configs/chaos-mesh.lock.json), [Helm values](../infra/chaos-mesh/values.json), [admission policy](../infra/chaos-mesh/admission-policy.json), [experiment Role](../infra/chaos-mesh/experiment-role.json) and [sanitized evidence](evidence/CCT-07-chaos-mesh-setup.json).

## Installed configuration

Chaos Mesh 2.8.8 is installed as release `cct-chaos` in `cct-chaos-system`. The official chart's SHA-256 matches its Helm index. Controller and daemon images are pinned to native linux/arm64 digests. The kind node uses Kubernetes 1.36.4 and containerd 2.3.4 at `/run/containerd/containerd.sock`.

There is one controller replica and one daemon on the single kind node. The controller watches experiments and selects targets only in `cct-app`. Only `networkchaos` and its internal `podnetworkchaos` controller are enabled. Dashboard, DNS-fault server, profiling and duplicate Prometheus deployment are disabled. Controller-to-daemon traffic uses the chart's mTLS certificates. Host-network testing is disabled.

The daemon runs privileged, uses the kind node's PID namespace and mounts its runtime socket to perform network changes. It is trusted infrastructure; these permissions are not assigned to teammates. Its namespace permits that required security profile. The chart installs CRDs, webhook configurations and controller RBAC, including unused dashboard service account/roles even though no dashboard is deployed. Rendered certificates and private member bindings stay under ignored `.local/chaos/`.

The controller requests 50m CPU/256 MiB with limits of 500m/512 MiB. The daemon requests 50m/128 MiB with limits of 500m/256 MiB. This installation check is not a full-stack capacity or collection-overhead benchmark.

Official references: [supported versions](https://chaos-mesh.org/supported-releases/), [Helm and namespace-scoped installation](https://chaos-mesh.org/docs/production-installation-using-helm/), and [permissions](https://chaos-mesh.org/docs/manage-user-permissions/).

## Runtime and scope validation

A disposable canary server and client ran in `cct-app` with labels that no Boutique Service selects. A 10-second, 100 ms outgoing delay was injected into the canary only. Five HTTP measurements were taken in each phase:

| Phase | Median request duration |
| --- | --- |
| Baseline | 0.84 ms |
| Delay active | 204.41 ms |
| Recovered | 0.76 ms |

The controller recorded one successful injection and one recovery. AllInjected and AllRecovered conditions were observed, and the fault and canary resources were removed. TCP exchanges can cross the delayed outgoing path more than once; the configured delay is not a promise of an equal HTTP-latency increment. These small samples demonstrate effect and recovery, not performance percentiles.

All 28 authorization checks and 23 admission dry runs passed. Only the approved pilot and its allowed maximum bounds were accepted. Other names/apps, all-pod mode, namespace escape, explicit pod maps, unbounded/excessive duration, excessive latency/jitter, remote/external/additional targets, partition action, wrong direction/interface, and pause/finalizer overrides were rejected. These were server-side dry runs; no Boutique fault was injected. Teammate checks used host-administrator impersonation rather than her laptop.

Boutique's 11 pods and all three monitoring pods were Ready afterward, and private Boutique returned HTTPS 200. No active fault or canary resource remained.

## Komalika's pilot

Her individual identity can get/list/watch/create/delete NetworkChaos in `cct-app`. Admission limits it to:

- Resource `cct-productcatalog-delay`, selecting `app: productcatalogservice` in `cct-app`.
- `mode: one`, outgoing `action: delay` and only the normal application network interface.
- Integer duration 1–60 seconds, delay 1–500 ms, jitter 0–100 ms and correlation zero.
- No extra pod maps, external/remote targets, packet reordering or cleanup/pause overrides.

The separate `cct-runtime-check` name/selector is reserved for the disposable installation canary. A fixed pilot name prevents concurrent arbitrary experiment objects; save a unique run ID in each experiment receipt before deleting and recreating the object for the next run.

Her role cannot patch/update experiment specs, status or finalizers, and cannot use PodNetworkChaos directly, PodChaos, schedules, workflows, pod deletion/shells, workload/RBAC changes, Secrets or other namespaces. Monitoring port-forward access remains available. Further fault families require matching controller configuration, validated scope and roles.

On her laptop, with Tailscale connected and her project kubeconfig selected, first validate the file. The actual create belongs in an agreed experiment window because it affects the shared Boutique:

```sh
export KUBECONFIG="$HOME/.kube/cct-team.yaml"
kubectl auth whoami
kubectl auth can-i create networkchaos.chaos-mesh.org -n cct-app
kubectl create --dry-run=server -f chaos/network-delay.example.yaml

# In the reserved window, after workload/telemetry warm-up:
kubectl create -f chaos/network-delay.example.yaml
kubectl -n cct-app wait --for=condition=AllInjected networkchaos/cct-productcatalog-delay --timeout=40s
kubectl -n cct-app get networkchaos/cct-productcatalog-delay -o json
kubectl -n cct-app wait --for=condition=AllRecovered networkchaos/cct-productcatalog-delay --timeout=60s
kubectl -n cct-app get networkchaos/cct-productcatalog-delay -o json
kubectl -n cct-app delete networkchaos/cct-productcatalog-delay --wait=true --timeout=30s
```

Save JSON snapshots and independent workload/telemetry evidence to a unique run directory before cleanup. Record requested and observed fault times separately. Confirm application recovery, retain invalid/no-impact runs with reasons, and repeat three times for CCT-07. Use create/delete per run; apply/patch changes to an existing experiment are outside this initial role. Deleting an experiment requests controller cleanup. If recovery stalls, stop new runs and have the host owner inspect it rather than removing finalizers.

## Owner reproduction and recovery

From this repository on the prepared owner host:

```sh
source .local/env
umask 077
mkdir -p .local/chaos
helm pull chaos-mesh --repo https://charts.chaos-mesh.org --version 2.8.8 --destination .local/chaos
# Compare the SHA-256 with configs/chaos-mesh.lock.json and the official index.
kubectl --context kind-cloud-chaos-twin apply -f infra/chaos-mesh/namespace.json
helm install cct-chaos .local/chaos/chaos-mesh-2.8.8.tgz --namespace cct-chaos-system \
  --kube-context kind-cloud-chaos-twin -f infra/chaos-mesh/values.json --wait --timeout=5m
kubectl --context kind-cloud-chaos-twin apply -f infra/chaos-mesh/admission-policy.json
```

Review the controller's namespace flags, images, roles and admission policy before granting fault access. The [runtime checker](../chaos/check_runtime.py) repeats the disposable test using `--image` from the lock and a new `--output` directory. It performs a real fault on its canary and cleans up afterward. It does not target Boutique.

After runtime/scope verification, run `python3 scripts/prepare_fault_access.py`, review `.local/chaos/komal-access.review.json`, then server-dry-run/apply that file with the explicit project context. The preparer renders only the dedicated Role and identity binding; it applies nothing and publishes no login/token.

To stop new teammate experiments, delete RoleBinding `cct-komalika-networkchaos` in `cct-app`. Recover/delete active experiments and verify application health before stopping the controller. Only then, if ending the installation, run `helm uninstall cct-chaos -n cct-chaos-system --kube-context kind-cloud-chaos-twin`. Helm does not remove CRDs automatically. Keep CRDs while experiment objects or cleanup finalizers remain. Reinstalling regenerates private certificates unless retained deliberately.
