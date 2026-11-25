#!/usr/bin/env python3
"""Build the pinned upstream release for this Mac and render a local manifest.

Build logs and downloaded upstream source stay outside tracked source. No
registry push is performed. Run from a shell with `source .local/env`.
"""

import hashlib
import json
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "vendor/online-boutique"
LOCAL = ROOT / ".local/boutique"
COMMIT = "5b608cb1dfe317d05e4f1bb8c99b7618452fde87"
MANIFEST_SHA = "ae3c20181c051048e6a5cae51fa530681affa4043ba60ae95d4f39e1262f2744"
NODE = "cloud-chaos-twin-control-plane"
KUBE = ["kubectl", "--kubeconfig", str(ROOT / ".local/kubeconfig"),
        "--context", "kind-cloud-chaos-twin"]


def run(args, **kwargs):
    return subprocess.check_output(args, text=True, **kwargs).strip()


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def registry_pin(image):
    manifest = json.loads(run(["docker", "buildx", "imagetools", "inspect",
                               image, "--format", "{{json .Manifest}}"] ))
    if manifest.get("manifests"):
        candidates = [m for m in manifest["manifests"]
                      if m.get("platform", {}).get("architecture") == "arm64"
                      and m["platform"].get("os") == "linux"]
        if not candidates:
            raise ValueError("No linux/arm64 image: " + image)
        digest = candidates[0]["digest"]
    else:
        config = json.loads(run(["docker", "buildx", "imagetools", "inspect",
                                 image, "--format", "{{json .Image}}"] ))
        if config["architecture"] != "arm64" or config["os"] != "linux":
            raise ValueError("Image is not native linux/arm64: " + image)
        digest = manifest["digest"]
    return image.split("@")[0] + "@" + digest


def main():
    LOCAL.mkdir(parents=True, exist_ok=True)
    if run(["git", "-C", str(SOURCE), "rev-parse", "HEAD"]) != COMMIT:
        raise ValueError("Upstream checkout is not the pinned commit")
    if run(["git", "-C", str(SOURCE), "status", "--porcelain"]):
        raise ValueError("Upstream checkout has unrecorded modifications")
    raw = (SOURCE / "release/kubernetes-manifests.yaml").read_bytes()
    if hashlib.sha256(raw).hexdigest() != MANIFEST_SHA:
        raise ValueError("Upstream manifest checksum mismatch")
    encoded = run(KUBE + ["create", "--dry-run=client", "--validate=false",
                          "-f", "-", "-o", "json"], input=raw.decode())
    items = []
    decoder = json.JSONDecoder()
    while encoded.strip():
        item, end = decoder.raw_decode(encoded.lstrip())
        items.append(item)
        encoded = encoded.lstrip()[end:]
    manifest = {"apiVersion": "v1", "kind": "List", "items": items}
    lock_path = ROOT / "configs/boutique.local.lock.json"
    lock = json.loads(lock_path.read_text()) if lock_path.exists() else {
        "schema_version": 1, "upstream_release": "v0.10.7", "upstream_commit": COMMIT,
        "upstream_manifest_sha256": MANIFEST_SHA, "platform": "linux/arm64",
        "build_args": {"BUILDPLATFORM": "linux/arm64", "TARGETARCH": "arm64", "TARGETOS": "linux"},
        "images": {}, "external_images": {}, "base_overrides": {},
    }
    if lock["upstream_commit"] != COMMIT:
        raise ValueError("Existing lock belongs to another release")
    static = lock["base_overrides"].get("gcr.io/distroless/static")
    if not static:
        static = registry_pin("gcr.io/distroless/static")
        lock["base_overrides"]["gcr.io/distroless/static"] = static
        write_json(lock_path, lock)
    deployments = [r for r in manifest["items"] if r["kind"] == "Deployment"]
    for resource in deployments:
        service = resource["metadata"]["name"]
        spec = resource["spec"]["template"]["spec"]
        for container in spec.get("initContainers", []) + spec["containers"]:
            image = container["image"]
            if "/microservices-demo/" not in image:
                if image not in lock["external_images"]:
                    lock["external_images"][image] = registry_pin(image)
                    write_json(lock_path, lock)
                container["image"] = lock["external_images"][image]
                continue
            context = SOURCE / "src" / service
            if service == "cartservice":
                context /= "src"
            dockerfile = (context / "Dockerfile").read_text().replace(
                "FROM gcr.io/distroless/static\n", "FROM " + static + "\n")
            local_dockerfile = LOCAL / (service + ".Dockerfile")
            local_dockerfile.write_text(dockerfile)
            tag = "docker.io/cct-local/{}:{}-arm64".format(service, COMMIT[:12])
            prior = lock["images"].get(service)
            current = subprocess.run(["docker", "image", "inspect", tag, "--format", "{{.Id}}"],
                                     capture_output=True, text=True)
            if not (prior and current.returncode == 0 and current.stdout.strip() == prior["image_id"]):
                print("Building native image: " + service, flush=True)
                command = ["docker", "buildx", "build", "--platform", "linux/arm64", "--load",
                           "--provenance=false", "--progress", "plain", "-t", tag,
                           "-f", str(local_dockerfile), "--metadata-file", str(LOCAL / (service + ".metadata.json"))]
                for key, value in lock["build_args"].items():
                    command += ["--build-arg", key + "=" + value]
                command.append(str(context))
                log = LOCAL / (service + ".build.log")
                with log.open("w") as output:
                    result = subprocess.run(command, stdout=output, stderr=subprocess.STDOUT)
                if result.returncode:
                    print("\n".join(log.read_text().splitlines()[-40:]), flush=True)
                    raise RuntimeError("Build failed; see " + str(log))
            info = json.loads(run(["docker", "image", "inspect", tag]))[0]
            if info["Architecture"] != "arm64" or info["Os"] != "linux":
                raise ValueError("Unexpected built platform: " + service)
            print("Loading native image into kind: " + service, flush=True)
            subprocess.run(["kind", "load", "docker-image", tag, "--name", "cloud-chaos-twin"], check=True)
            images = json.loads(run(["docker", "exec", NODE, "crictl", "images", "-o", "json"]))["images"]
            imported = next(i for i in images if tag in i.get("repoTags", []))
            digest = imported["repoDigests"][0].split("@", 1)[1]
            pinned = tag.split(":", 1)[0] + "@" + digest
            # kind imports the build tag; register the digest reference explicitly
            # so kubelet can resolve imagePullPolicy=Never by that exact name.
            subprocess.run(["docker", "exec", NODE, "ctr", "-n", "k8s.io", "images", "tag",
                            "--force", tag, pinned], check=True, stdout=subprocess.DEVNULL)
            lock["images"][service] = {
                "upstream_image": image, "build_tag": tag, "image_id": info["Id"],
                "image": pinned, "architecture": "arm64",
                "dockerfile_sha256": hashlib.sha256(dockerfile.encode()).hexdigest(),
            }
            write_json(lock_path, lock)
            container["image"] = pinned
            container["imagePullPolicy"] = "Never"
        if service == "loadgenerator":
            resource["spec"]["replicas"] = 0
            for item in spec["containers"][0]["env"]:
                if item["name"] == "USERS":
                    item["value"] = "5"
    for resource in manifest["items"]:
        if resource["kind"] not in {"Deployment", "Service", "ServiceAccount"}:
            raise ValueError("Unexpected upstream resource kind")
        resource["metadata"]["namespace"] = "cct-app"
        if resource["kind"] == "Service" and resource["metadata"]["name"] == "frontend-external":
            resource["spec"]["type"] = "ClusterIP"
    rendered = LOCAL / "manifest.json"
    order = {"ServiceAccount": 0, "Service": 1, "Deployment": 2}
    manifest["items"].sort(key=lambda item: order[item["kind"]])
    write_json(rendered, manifest)
    lock["rendered_manifest_sha256"] = hashlib.sha256(rendered.read_bytes()).hexdigest()
    lock["local_changes"] = ["native arm64 builds", "digest-pinned images", "namespace cct-app",
                             "frontend-external ClusterIP", "loadgenerator replicas 0, users 5"]
    write_json(lock_path, lock)
    print("Prepared " + str(rendered), flush=True)


if __name__ == "__main__":
    main()
