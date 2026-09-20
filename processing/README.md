# Processing · Param

First deliverable: normalized Parquet tables and service/edge window features from a small real sample. Match Spark and its Kafka connector versions. Keep raw data separately, use explicit units/schema, persist checkpoints, and verify restart/replay behavior.

Start by evaluating 5-second feature windows. Mark low-count latency estimates and missing metrics. Define a late-data policy and avoid using finalized future information at a historical prediction cutoff. Store event time and availability time.

The Python starter's phase summaries are illustrative and do not implement Spark, streaming windows, or Parquet.
