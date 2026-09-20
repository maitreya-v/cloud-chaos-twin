# Ingestion · Param

First deliverable: publish and consume a small real sample with verified counts. Start with separate `cct.traces`, `cct.metrics`, `cct.logs`, and `cct.experiments` topics. A single KRaft broker is sufficient for initial development; record retention/partitions and do not claim replicated availability.

Agree on Kafka payload encoding with Merul. OTLP JSON is nested; document normalization. Deduplicate trace identity by run/trace/span and metric identity by series/timestamp. Preserve source and ingestion timestamps. Measure drops, retries, and consumer lag.

Kafka/Spark are not started or implemented by the offline demo. Add their pinned configurations after host selection.
