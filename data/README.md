# Data storage

Only small, explicitly synthetic examples are tracked in `data/samples/`. The starter fixture has three traces across healthy/fault/recovery phases with three normalized SERVER spans each. Reused span IDs in different traces are intentional: identity is `(run_id, trace_id, span_id)`.

Real datasets belong in separate storage. The ignored `data/raw/`, `data/processed/`, `data/features/`, and `data/experiments/` directories can be used locally. Agree on shared storage before the main experiment campaign. GitHub stores the code and dataset index, not high-volume telemetry.

An index should record run ID, storage location, checksum, schema version, code/config revision, workload, effective fault interval, trace sampling, missing-data statistics, and validity/recovery status. Keep credentials, customer data, and unrelated machine logs out of captures. Use synthetic workload inputs.
