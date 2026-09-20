# Live infrastructure · Maitreya

Status: templates only; no cluster or application has been deployed by the starter.

1. Complete the hardware checklist and install Docker, kind, kubectl, and Helm.
2. Select a supported kind release and a node image digest from that release's official notes. Set `CCT_KIND_IMAGE` to that full image reference and record it in the version file.
3. From the repository root, create a project-specific cluster and kubeconfig:

```sh
mkdir -p .local
kind create cluster --name cloud-chaos-twin --config infra/kind.yaml --image "${CCT_KIND_IMAGE:?Set a supported pinned node image first}" --kubeconfig .local/kubeconfig --wait 120s
kubectl --kubeconfig .local/kubeconfig --context kind-cloud-chaos-twin apply -f infra/namespaces.yaml
```

4. Select an exact Online Boutique release/commit from the official upstream repository. Review and save its manifest outside tracked source (for example, `vendor/online-boutique.yaml`), record the commit and checksum, and verify image architecture support.
5. Apply the reviewed namespaced application manifest explicitly to `cct-app` with this kubeconfig/context. Check for upstream hard-coded namespaces before applying it. Verify all required services and a stable workload.
6. Add observability and validate one scoped fault before expanding the stack.

Do not apply a recursive directory of templates. Each component has a separate deployment gate. Namespaces separate names and selectors, not CPU/memory contention; reserve resources and isolate analytics when collecting fault data.

Reference: [kind quick start](https://kind.sigs.k8s.io/docs/user/quick-start/) and [Online Boutique](https://github.com/GoogleCloudPlatform/microservices-demo).
