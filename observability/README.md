# Observability · Merul

First output: complete traces across browsing/cart/checkout, timestamped service metrics, and selected logs. The example Collector config receives OTLP traces and emits basic debug output only; it is not a deployment manifest and does not yet export to Kafka or Jaeger.

Choose/pin the Collector image before deployment. Configure application instrumentation and context propagation, then add separate export branches for Jaeger and Kafka. Verify whether each service actually emits data. Add metrics/log pipelines after validating their schema and sources.

Record event and availability timestamps, units, sampling, dropped/rejected records, and missing services. Coordinate normalized fields with Param and health-label definitions with Mithil.

References: [Collector configuration](https://opentelemetry.io/docs/collector/configuration/) and [Kafka exporter](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/exporter/kafkaexporter).
