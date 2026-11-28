# Independent coverage and reserved-run validation

The owner-run campaign is implemented by `scripts/validate_cct10.py`. A campaign
is accepted only after its saved receipts pass; implementation alone does not
establish coverage, calibration or forecasting accuracy.

Before capture, the runner saves the exact application templates and Service
ports. A temporary untraced Envoy proxy in front of each of the ten services
records request starts and completions, W3C context, operation, status and
microsecond duration, without bodies or cookies. Separate proxy request and
completion counters must agree with every saved log event. Admin ports are
loopback-only. Proxy images are pinned to a verified native ARM64 digest.
Existing service connections are reset by a second rollout after port changes.

Coverage review requires stable pod identities, no in-flight requests or buffered
logs at snapshot boundaries, no logger failures, and one-to-one correspondence
between independent requests and native SERVER spans. Missing records, unknown
background requests, broken propagation, status mismatches and incomplete native
ancestry fail review. Full post-run reconciliation is repeated without the live
observer's caches. Actual review/availability timestamps are retained.

Healthy capture uses twelve bounded workers and an average of five complete
browse/cart/checkout journeys per second. Each epoch-aligned five-second group
starts 25 journeys over 2.8 seconds and then drains. Warm-up is 120 seconds;
healthy measurement is 300 seconds. A backlog over 500 ms or any failed request
stops capture. During this campaign SDK batching is temporarily set to 200 ms;
the overhead pair uses the original deployed batching cadence.

The split reserves one whole healthy run for training, one fault run for
validation and two fault runs for test. Before any held-out fault, both native
and independent channels must supply at least three complete five-second healthy
windows per service with at least 20 requests each. Each channel separately fits
the candidate latency limit of twice its training-window median p95; the error
limit remains 5%, persistence two windows. The channels have different duration
boundaries, so their fitted latency values need not match. The native start must
fit the independent request with 2 ms tolerance; post-response SDK completion
skew is retained rather than assumed to be zero.

All three trials use the bounded outgoing productcatalog delay: 100 ms, jitter
10 ms, 30 seconds, with 120 seconds healthy measurement before injection and
at least 120 seconds after confirmed recovery. The original pilot's health,
monitoring, memory, ownership, recovery-finalizer and cleanup guards remain in
force. Caller latency must show the declared effect and recovery, with at least
20 eligible calls in each phase. Controller events and polling observations are
separate timing evidence.

A live observer attempts independent closure before each next five-second
cutoff. Late or incomplete frames remain unscored. Prospective decisions retain
their actual emission times and are evaluated against the separately calibrated
independent outcome channel. Retrospective label agreement is reported separately
and is never described as advance warning. Fault targets/timestamps are not
predictor features. Negative findings and unknown outcomes remain in the report.

The owner performs automated technical checks; the campaign does not record or
substitute for Merul, Mithil, Param or Komalika's personal acceptance. Temporary
proxies, ports, batching and templates are restored after the campaign, including
failed attempts. Raw captures stay under ignored `outputs/validation/`.
