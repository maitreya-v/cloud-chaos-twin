#!/usr/bin/env python3
"""Prepare Komalika's private fault binding; apply nothing."""
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main():
    members = json.loads((ROOT / ".local/tailscale/members.json").read_text())
    user = members["teammates"]["Komalika"]
    if "@" not in user or any(c.isspace() for c in user):
        raise ValueError("Expected Komalika's individual Tailscale login")
    role = json.loads((ROOT / "infra/chaos-mesh/experiment-role.json").read_text())
    binding = {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding",
               "metadata": {"name": "cct-komalika-networkchaos", "namespace": "cct-app"},
               "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "Role",
                           "name": role["metadata"]["name"]},
               "subjects": [{"apiGroup": "rbac.authorization.k8s.io", "kind": "User", "name": user}]}
    output = ROOT / ".local/chaos/komal-access.review.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps({"apiVersion": "v1", "kind": "List", "items": [role, binding]}, indent=2) + "\n")
    output.chmod(0o600)
    print("Prepared the experiment role and Komalika binding. Nothing applied.")


if __name__ == "__main__":
    main()
