# Controlled productcatalog pilot

This owner-run pilot records an actual fault and recovery. It does not complete
CCT-10 calibration, CCT-07 repeat trials, or the paired telemetry-overhead study.
The application and monitoring stack must already be deployed and healthy.

Load `.local/env`, then run `python3 scripts/cct10_pilot.py --run-id UNIQUE_ID`.
Use a new `cct-probe-*` ID for each attempt, including failed attempts. Evidence
is saved under `outputs/experiments/UNIQUE_ID/`. Keep the host awake and online.
Touch that directory's `STOP` file to abort; Ctrl-C also requests cleanup.

The existing pinned ARM64 loadgenerator supplies five upstream users at one
user/second, seed 42, original task weights and 1–10 second waits. After all five
spawn, the protocol is 120 seconds warm-up, 120 seconds measured healthy traffic,
a 30-second outgoing network delay on one `productcatalogservice` pod, then at
least 120 seconds observation after confirmed automatic recovery. Delay is
100 ms, jitter 10 ms, correlation zero. No application image or deployment is
changed. Closed-loop users do not guarantee a constant arrival rate.

The runner stops on any failed HTTP request or checkout confirmation, observed
application/monitoring/controller readiness loss, restart or replacement, two
distinct failed monitoring scrapes, unsafe node memory headroom/pressure, capture
failure, or recovery not confirmed within 45 seconds of the injection request.
Readiness is sampled about once per second and can miss shorter transitions.
The controller's own 30-second duration also bounds the experiment if the host
runner disappears. Cleanup checks resource ownership, deletes the owned fault
normally and preserves recovery finalizers. It never deletes application pods.

Raw evidence includes each workload operation's sampled W3C trace ID, repeated
Jaeger snapshots, trace-derived SERVER records, Prometheus samples with their
original scrape times, fresh pod snapshots, selected redacted frontend/checkout
logs, node resources, and requested/observed fault status. Each query observation
retains its actual availability time. Raw captures stay ignored by Git.
Jaeger's controller event timestamps have one-second precision; polling times
bound observation latency and must not be described as millisecond-accurate
physical fault onset/end. HTTP redirects can yield multiple frontend roots under
one operation trace. The raw archive preserves these rather than forcing a
single-root interpretation.

The handoff's unchanged pod join constructs epoch-aligned five-second service
windows by SERVER-span end time. They remain `complete=false` without independently
reviewed backend request/duration accounting. Low-volume, missing or incomplete
windows cannot fit health thresholds. Prometheus spanmetrics and saved traces
share a source and do not independently establish completeness. Availability is
retrospective; no live prediction accuracy is claimed. Keep fault metadata apart
from model inputs and split whole run IDs before fitting thresholds.
