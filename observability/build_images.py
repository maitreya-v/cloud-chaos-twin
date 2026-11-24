"""Owner-side build and kind import of three traced variants. Does not deploy them."""
import hashlib
import json
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
from prepare_boutique import COMMIT, NODE, SOURCE, run
from owner import EXTRA

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
        for svc in EXTRA:
            context = src/"src"/svc
            if svc == "cartservice":
                context = context/"src"
            dockerfile = context/"Dockerfile"
            if svc == "shippingservice":
                dockerfile.write_text(dockerfile.read_text().replace("FROM gcr.io/distroless/static\n",
                                     "FROM "+lock["base_overrides"]["gcr.io/distroless/static"]+"\n"))
            tag = f"docker.io/cct-local/{svc}-otel:{suffix}"
            subprocess.run(["docker","buildx","build","--load","--platform","linux/arm64","--provenance=false",
                "--build-arg","BUILDPLATFORM=linux/arm64","--build-arg","TARGETARCH=arm64",
                "--build-arg","TARGETOS=linux","-t",tag,str(context)],check=True)
            info = json.loads(run(["docker","image","inspect",tag]))[0]
            if (info["Os"],info["Architecture"]) != ("linux","arm64"):
                raise ValueError("unexpected image platform: "+svc)
            run(["kind","load","docker-image",tag,"--name","cloud-chaos-twin"])
            images = json.loads(run(["docker","exec",NODE,"crictl","images","-o","json"]))["images"]
            entry = next(i for i in images if tag in i.get("repoTags",[]))
            digest = entry["repoDigests"][0].split("@",1)[1]
            pins[svc] = tag.rsplit(":",1)[0] + "@" + digest
            run(["docker","exec",NODE,"ctr","-n","k8s.io","images","tag","--force",tag,pins[svc]])
    tmp = DEST/"image-lock.tmp"
    tmp.write_text(json.dumps(pins, indent=2)+"\n")
    tmp.replace(DEST/"image-lock.json")
    print("All three images imported. Review .local/telemetry/image-lock.json before deploying.")


if __name__ == "__main__":
    main()
