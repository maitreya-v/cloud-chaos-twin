#!/bin/bash
# launchd restarts this loopback-only connection after Docker/pod restarts.
set -eu
CCT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
source "$CCT_ROOT/.local/env"
exec "$CCT_ROOT/.local/bin/kubectl" \
  --kubeconfig "$CCT_ROOT/.local/kubeconfig" \
  --context kind-cloud-chaos-twin -n cct-app \
  port-forward --address 127.0.0.1 --pod-running-timeout=60s \
  service/frontend 8080:80
