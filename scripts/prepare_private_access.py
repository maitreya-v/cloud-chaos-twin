#!/usr/bin/env python3
"""Render identity-specific access files locally; never publish or apply them."""

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LOCAL = ROOT / ".local/tailscale"


def save(name, data):
    path = LOCAL / name
    path.write_text(json.dumps(data, indent=2) + "\n")
    path.chmod(0o600)


def main():
    members = json.loads((LOCAL / "members.json").read_text())
    owner, teammates = members["owner"], members["teammates"]
    if set(teammates) != {"Merul", "Param", "Mithil", "Komalika"}:
        raise ValueError("Expected the four named team members")
    identities = [owner, *teammates.values()]
    if len(set(identities)) != 5 or any("@" not in x or any(c.isspace() for c in x) for x in identities):
        raise ValueError("Expected five distinct Tailscale login emails")
    policy = json.loads((LOCAL / "policy.json").read_text())
    policy["groups"]["group:cct-team"] = identities
    template = policy["tests"][0]
    policy["tests"] = [{**template, "src": identity} for identity in identities]
    save("policy.team.review.json", policy)
    subjects = [{"kind": "User", "name": x, "apiGroup": "rbac.authorization.k8s.io"}
                for x in teammates.values()]
    items = [{"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRoleBinding",
              "metadata": {"name": "cct-owner-admin"},
              "roleRef": {"kind": "ClusterRole", "name": "cluster-admin", "apiGroup": "rbac.authorization.k8s.io"},
              "subjects": [{"kind": "User", "name": owner, "apiGroup": "rbac.authorization.k8s.io"}]}]
    for namespace in ("cct-app", "cct-observability"):
        items.append({"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding",
                      "metadata": {"name": "cct-team-observers", "namespace": namespace},
                      "roleRef": {"kind": "ClusterRole", "name": "cct-observer", "apiGroup": "rbac.authorization.k8s.io"},
                      "subjects": subjects})
    items.append({"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding",
                  "metadata": {"name": "cct-merul-observability", "namespace": "cct-observability"},
                  "roleRef": {"kind": "Role", "name": "cct-observability-maintainer", "apiGroup": "rbac.authorization.k8s.io"},
                  "subjects": [{"kind": "User", "name": teammates["Merul"], "apiGroup": "rbac.authorization.k8s.io"}]})
    save("rbac-bindings.review.json", {"apiVersion": "v1", "kind": "List", "items": items})
    print("Prepared private network policy and Kubernetes bindings for five identities. Nothing applied.")


if __name__ == "__main__":
    main()
