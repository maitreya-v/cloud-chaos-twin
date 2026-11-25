"""Owner-side build and kind import of six traced variants. Does not deploy them."""
import hashlib
import json
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
from prepare_boutique import COMMIT, NODE, SOURCE, run
from owner import REBUILT, NODE as NODE_SERVICES

PATCH = ROOT / "observability/boutique-v0.10.7.patch"
DEST = ROOT / ".local/telemetry"


def main():
    lock = json.loads((ROOT/"configs/boutique.local.lock.json").read_text())
    if run(["git", "-C", str(SOURCE), "rev-parse", COMMIT+"^{commit}"]) != COMMIT:
        raise ValueError("pinned Boutique source is required")
    if NODE not in run(["kind","get","nodes","--name","cloud-chaos-twin"]).splitlines():
        raise ValueError("expected the project kind node")
    DEST.mkdir(parents=True, exist_ok=True)
    pins = {}
    suffix = hashlib.sha256(PATCH.read_bytes()).hexdigest()[:12]
    with tempfile.TemporaryDirectory(dir=DEST) as temp:
        src = Path(temp)/"source"
        subprocess.run(["git","clone","--quiet","--no-hardlinks","--no-checkout",str(SOURCE),str(src)],check=True)
        run(["git","-C",str(src),"checkout","--detach",COMMIT])
        run(["git","-C",str(src),"apply","--check",str(PATCH)])
        run(["git","-C",str(src),"apply",str(PATCH)])
        for svc in REBUILT:
            context = src/"src"/svc
            if svc == "cartservice":
                context = context/"src"
            dockerfile = context/"Dockerfile"
            if svc in ("shippingservice", "checkoutservice"):
                dockerfile.write_text(dockerfile.read_text().replace("FROM gcr.io/distroless/static\n",
                                     "FROM "+lock["base_overrides"]["gcr.io/distroless/static"]+"\n"))
            tag = f"docker.io/cct-local/{svc}-otel:{suffix}"
            subprocess.run(["docker","buildx","build","--load","--platform","linux/arm64","--provenance=false",
                "--build-arg","BUILDPLATFORM=linux/arm64","--build-arg","TARGETARCH=arm64",
                "--build-arg","TARGETOS=linux","-t",tag,str(context)],check=True)
            info = json.loads(run(["docker","image","inspect",tag]))[0]
            if (info["Os"],info["Architecture"]) != ("linux","arm64"):
                raise ValueError("unexpected image platform: "+svc)
            if svc == "adservice":
                # Match the application's non-root Kubernetes UID. Docker ADD's
                # file mode can also affect a newly created parent directory.
                subprocess.run(["docker", "run", "--rm", "--user", "1000:1000",
                    "--cpus", "1", "--memory", "300m", "--entrypoint", "java",
                    "-e", "OTEL_TRACES_EXPORTER=none", "-e", "OTEL_METRICS_EXPORTER=none",
                    "-e", "OTEL_LOGS_EXPORTER=none", tag,
                    "-javaagent:/opt/otel/javaagent.jar", "-version"], check=True, timeout=60)
            if svc in NODE_SERVICES:
                subprocess.run(["docker", "run", "--rm", "--user", "1000:1000",
                    "--cpus", "1", "--memory", "128m", "--entrypoint", "node", tag,
                    "-e", (ROOT/"observability/check_node_exporter.js").read_text()], check=True, timeout=30)
            run(["kind","load","docker-image",tag,"--name","cloud-chaos-twin"])
            images = json.loads(run(["docker","exec",NODE,"crictl","images","-o","json"]))["images"]
            entry = next(i for i in images if tag in i.get("repoTags",[]))
            digest = entry["repoDigests"][0].split("@",1)[1]
            pins[svc] = tag.rsplit(":",1)[0] + "@" + digest
            run(["docker","exec",NODE,"ctr","-n","k8s.io","images","tag","--force",tag,pins[svc]])
    tmp = DEST/"image-lock.tmp"
    tmp.write_text(json.dumps(pins, indent=2)+"\n")
    tmp.replace(DEST/"image-lock.json")
    print("All six images imported. Review .local/telemetry/image-lock.json before deploying.")


if __name__ == "__main__":
    main()
