#!/bin/sh
# Parse the rendered configs with the exact upstream binaries before applying.
set -eu
root=$(cd "$(dirname "$0")/.." && pwd)
cd "$root"
dir=$(mktemp -d)
trap 'rm -rf "$dir"' EXIT
chmod 755 "$dir"
python3 - "$dir" <<'PY'
import json,sys
from pathlib import Path
from observability.stack import collector_config,stack,IMAGES
out = Path(sys.argv[1])
for key,broker in (("base",None),("kafka","broker:9092")):
    (out / (key+".yaml")).write_text(json.dumps(collector_config(broker)))
cfg=next(i["data"]["config.yaml"] for i in stack() if i["kind"]=="ConfigMap" and i["metadata"]["name"]=="cct-prometheus")
(out/"prometheus.yaml").write_text(cfg)
(out/"collector-image").write_text(IMAGES["collector"])
(out/"prom-image").write_text(IMAGES["prometheus"])
PY
for cfg in base kafka; do
  docker run --rm -v "$dir:/conf:ro" "$(cat "$dir/collector-image")" validate "--config=/conf/$cfg.yaml"
done
docker run --rm --entrypoint /bin/promtool -v "$dir:/conf:ro" "$(cat "$dir/prom-image")" check config /conf/prometheus.yaml
