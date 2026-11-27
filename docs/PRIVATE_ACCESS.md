# Private access to the shared Mac deployment

## Current state

- Tailscale 1.102.4 is installed and connected on Maitreya's Mac. The official universal Mac app passed Apple signature and notarization checks.
- Boutique is available through Tailscale Serve with a verified HTTPS certificate. Both the local URL and private URL returned the Boutique page with HTTP 200. Public Funnel exposure is disabled.
- The saved network policy permits only the five named project identities to reach the Mac website and Kubernetes proxy over HTTPS. The default unrestricted network rule, SSH rule, and Funnel permission were removed. Five policy tests cover allowed HTTPS and denied direct ports. The original effective policy is backed up locally.
- A per-user macOS LaunchAgent maintains the localhost connection to Boutique and retries after it exits. This connection is independent of the chat session.
- The official Tailscale Kubernetes operator 1.102.4 is installed with authenticated API proxying. The owner's private sign-in returned the correct individual identity and listed all 11 healthy application pods. The operator and application pods have no container restarts at the latest check.
- Team network rules and individual Kubernetes permissions are applied. All 40 authorization checks and 20 integration/admission checks passed; see [sanitized evidence](evidence/PRIVATE_ACCESS-maitreya.json). Teammate checks use host-administrator impersonation, not a teammate device.
- The owner sent all four approved Member invitations. The reloaded Tailscale Users page confirms Merul, Param, Mithil, and Komalika are each `Invited` with role `Member`. Acceptance is pending for all four. The console requires owner approval when invited users join.
- Cross-network verification with a teammate remains pending. A successful request from the host Mac does not prove another device can connect.
- On 2026-11-27, the owner authorized Komalika to port-forward monitoring pods. The dedicated permission and her individual binding are applied in `cct-observability`. Twenty-seven authorization checks and three actual service-forward tests passed using host-admin impersonation; see the [access receipt](evidence/komal-monitoring-portforward.json).

Personal login emails, tailnet addresses, rendered policy/bindings, and credentials belong in the ignored `.local/tailscale/` directory. They must not be committed to the public repository.

## Access design

Website traffic follows the private network to Tailscale Serve, then `127.0.0.1:8080`, then the Kubernetes frontend Service. The local listener is not bound to Wi-Fi or all interfaces. The Mac must remain awake, online, and running Docker and Tailscale. No router port forwarding is needed.

Kubernetes traffic uses the official operator's authenticated API proxy, with hostname `cct-kubernetes`. The proxy represents each caller's Tailscale login identity; Kubernetes RBAC determines their permissions. Owner credentials and the host's administrator kubeconfig are never distributed to teammates.

| Person | Applied cluster permissions, usable after joining |
| --- | --- |
| Maitreya | Administration through the private proxy |
| Merul | Read project workloads/logs; manage workloads/configuration in `cct-observability` |
| Param, Mithil | Read project workloads/logs in `cct-app` and `cct-observability` |
| Komalika | Read project workloads/logs; port-forward monitoring pods; run the bounded NetworkChaos pilot in `cct-app` |

The monitoring namespace enforces Kubernetes' restricted pod-security profile before granting workload-write access. The maintainer role does not grant secret access, RBAC changes, or cluster-wide installation. Instrumentation changes in the application namespace and cluster-wide monitoring components remain owner-applied. Komalika now has the initial NetworkChaos experiment role. Admission constrains its name, target, action and duration; a namespace alone cannot constrain every fault selector. See the [fault setup record](CCT-07_CHAOS_SETUP.md). Param and Mithil can develop with fixtures until their live data paths exist.

## Kubernetes connector and version record

The installed official operator uses an approved Tailscale OAuth client with write scopes for **General / Services**, **Devices / Core**, and **Keys / Auth Keys**, restricted to `tag:cct-operator` and tags it owns. It manages project devices and service registrations; it has no Tailscale policy-editing, user-management, DNS, or billing permissions. The credential remains valid until revoked.

Inside Kubernetes the official chart installs its controller roles, CRDs, and a role allowing the authenticated proxy to impersonate users/groups. This is a trusted infrastructure component. Teammates still receive only the separate RBAC bindings assigned to their identities.

- Chart/app version: 1.102.4.
- Chart SHA-256: `f298a860775749037dd6f42bf764369bbe2865bdfff0289e864851a4bc90c1e2`.
- Native ARM64 operator image: `tailscale/k8s-operator@sha256:25615d0bf5797bf8257d33d1753adf47243f368fce1b5119beb239b2c710083a`.
- Mac ZIP SHA-256: `9331cef28109464864cd90b2a91fd0cdac09b9b1355d2b7f84de1788863fc2f2`.
- Apple signing team: `W5364U7YZB` (Tailscale Inc.).
- Installed operator settings: [operator-values.yaml](../infra/tailscale/operator-values.yaml).
- Applied roles: [rbac-roles.yaml](../infra/tailscale/rbac-roles.yaml).

The owner approved the exact OAuth permissions and operator installation before they were applied. The credential was transferred without logging its value into owner-readable local files and the `tailscale/operator-oauth` Kubernetes Secret. The temporary loopback-only transfer form shut down after saving. Credentials are absent from Helm values/history and tracked files.

Run `python3 scripts/prepare_private_access.py` to regenerate local review files from `.local/tailscale/members.json`. The script applies nothing. New team policy must be merged with the actual policy after checking for intervening changes; do not overwrite unrelated tailnet configuration.

For a future reinstall, create the Secret from the private files and install the pinned chart with the saved values. Never put credentials in Git, chat, command arguments, or Helm values/history. The role manifest creates the restricted monitoring namespace before its Role and bindings. Verify identity and permissions again after any access change.

Apply the additional [monitoring port-forward Role](../infra/tailscale/monitoring-portforward-role.json) before restoring the generated Komalika binding. The private-access preparer reads her existing login from the ignored members file; no personal login is stored in the public Role definition. For an incremental change, review and apply only the intended Role and binding rather than replacing all project bindings or network policy.

## Teammate connection steps

1. Accept the Tailscale Member invitation with the exact invited email address. If the account is awaiting approval, the owner must approve that named account in Tailscale's Users page.
2. Install and sign in to Tailscale on the teammate's own computer. Select the owner's project network if more than one network is available.
3. Open the private Boutique HTTPS link supplied by the owner. Everyone uses the same deployed store.
4. For Kubernetes work, install `kubectl` and use the Tailscale CLI to create a separate connection file. macOS/Linux commands:

```sh
mkdir -p "$HOME/.kube"
export KUBECONFIG="$HOME/.kube/cct-team.yaml"
tailscale configure kubeconfig cct-kubernetes
kubectl auth whoami
kubectl get pods -n cct-app
kubectl logs -n cct-app deployment/frontend --tail=20
```

On a Mac with the standalone app, if `tailscale` is not on PATH, use `/Applications/Tailscale.app/Contents/MacOS/Tailscale` in its place. In Windows PowerShell, set `$env:KUBECONFIG` to a separate file under `$HOME\.kube`, then run the same Tailscale and kubectl commands. Re-select the separate configuration in each new terminal session.

The generated connection file contains the endpoint and a non-secret `unused` token placeholder. The network identity provides authentication. It does not contain the host's administrator certificate. `kubectl auth whoami` should show the teammate's own invited identity.

All teammates can inspect application and monitoring workloads/logs. Merul can also manage monitoring workloads and configuration in `cct-observability`. Other live data pipelines, monitoring dashboards, and fault permissions are separate tasks and are not available merely by joining the network.

## Komalika's monitoring access

The owner-authorized `cct-monitoring-portforward` Role grants `get` and `create` on `pods/portforward` only in `cct-observability`. The generated `cct-komalika-monitoring-portforward` binding names her existing individual identity. Her original observer bindings provide the pod/service reads needed to select a service's pod. Other teammates do not receive this extra permission.

With Tailscale connected, run each command in a separate terminal on her own computer. These commands explicitly select the project connection and bind only her laptop's loopback address:

```sh
kubectl --kubeconfig "$HOME/.kube/cct-team.yaml" -n cct-observability \
  port-forward --address 127.0.0.1 service/cct-jaeger 16686:16686
kubectl --kubeconfig "$HOME/.kube/cct-team.yaml" -n cct-observability \
  port-forward --address 127.0.0.1 service/cct-prometheus 9090:9090
```

Open `http://127.0.0.1:16686` for Jaeger or `http://127.0.0.1:9090` for Prometheus. Collector troubleshooting can use `service/cct-collector 8889:8889` for exported metrics or `8888:8888` for Collector telemetry. Stop a forwarding session with Ctrl+C. A pod replacement requires reconnecting.

This namespace-scoped permission permits connections to any listening port on monitoring pods, including Collector ingestion; it is not an HTTP read-only or per-port restriction. It grants no pod shell, Secret reads, deployment changes, or port-forwarding in the application/operator namespaces. Komalika remains a Tailscale Member. Kubernetes namespace permissions are separate from Tailscale administration.

Chaos Mesh 2.8.8 is now installed in `cct-chaos-system` with experiment scope restricted to `cct-app`. The canary verified injection, automatic recovery and cleanup. Komalika can create/read/watch/delete the approved bounded product-catalog delay through her additional Role; other fault families and in-place experiment edits are outside that initial scope. Follow the [CCT-07 setup record](CCT-07_CHAOS_SETUP.md) for pilot and owner reproduction steps.

The host verified real Jaeger, Prometheus and Collector HTTP requests through forwards authenticated as Komalika. Her own laptop must still confirm its project connection. For authorization checks, use the actual subresource flag, for example `kubectl auth can-i create pods --subresource=portforward -n cct-observability`; `pods/NAME` syntax can instead select a named pod.

To revoke only the added capability from the owner connection:

```sh
kubectl --context kind-cloud-chaos-twin -n cct-observability \
  delete rolebinding cct-komalika-monitoring-portforward
```

## Verified access boundaries

The host-side authorization matrix confirms project reads, denies application writes and container shell access, denies RBAC changes, and allows monitoring writes only for Merul. Actual requests also rejected all four teammates' monitoring Secret/RBAC reads and cluster-wide node reads. Server-side dry runs accepted Merul's monitoring configuration write, denied Param's write, and rejected Merul's privileged host-network pod through restricted pod security. No test workload was created.

The first connector request timed out while it was starting; subsequent authenticated identity and workload reads passed, including after an overnight pause. The private website remained available, and the persistent localhost connection previously passed a managed restart test. No full Docker restart or remote teammate test is claimed.

## Startup and recovery

After restarting the Mac, log in and start Docker and Tailscale. The LaunchAgent starts the local connection automatically and retries every ten seconds if it exits. Docker startup and pod readiness can take longer. The standalone Mac Tailscale app requires a user login after reboot; this is not an unattended server before login. The setup does not change the Mac's sleep settings or guarantee availability with the lid closed.

The LaunchAgent is named `edu.stonybrook.cct.boutique-forward` and resides in the owner's `~/Library/LaunchAgents/`. Its source connection script is [boutique_port_forward.sh](../scripts/boutique_port_forward.sh). Logs are `.local/tailscale/forward.log` and `forward.error.log`. Moving this repository requires updating the LaunchAgent's script/log paths.

Inspect or restart the managed connection from this repository:

```sh
source .local/env
launchctl print "gui/$(id -u)/edu.stonybrook.cct.boutique-forward"
launchctl kickstart -k "gui/$(id -u)/edu.stonybrook.cct.boutique-forward"
tailscale serve status
```

Do not start a second manual port-forward on port 8080 while this job runs. To stop private website sharing without stopping Boutique:

```sh
source .local/env
tailscale serve --https=443 off
```

To stop the managed local connection:

```sh
launchctl bootout "gui/$(id -u)/edu.stonybrook.cct.boutique-forward"
```

Deleting its own LaunchAgent plist after unloading prevents it from returning at login. Turning off sharing or the local connection leaves the application deployed in Kubernetes.

## Teammate acceptance checks

Each invited teammate must accept with the exact intended login, install Tailscale, and connect to the owner's team network. Creating a separate personal network does not automatically give access to this project. Send the private website URL and API-proxy hostname through the team channel after membership is confirmed.

On a different Internet connection, one teammate must verify website browsing and allowed Kubernetes reads. Also verify denied operations such as reading Secrets, changing RBAC, or modifying application deployments. Verify Merul's monitoring writes separately. Record who tested, their network, time, and results without collecting credentials or unrelated device details. Repeat connectivity after a planned Docker restart; reserve experiments so concurrent changes do not invalidate measurements.

Official references: [Mac installation](https://tailscale.com/docs/concepts/macos-variants), [private Serve](https://tailscale.com/docs/reference/tailscale-cli/serve), [operator installation](https://tailscale.com/docs/kubernetes-operator/install-operator), [API authentication and RBAC](https://tailscale.com/docs/kubernetes-operator/api-server-access/auth-and-rbac).
