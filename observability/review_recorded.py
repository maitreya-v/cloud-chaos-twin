"""Review the recorded shopping run against its replay. No cluster or broker is required."""
import argparse
from collections import defaultdict
from hashlib import sha256
import json
from pathlib import Path
from tempfile import TemporaryDirectory

from analytics.graph import percentile
from analytics.health import validate_windows
from analytics.io import read_json, table
from ingestion.contract import TOPICS, read_archive
from ingestion.recorded import adapt
from processing.normalize import normalize
from observability.overhead import compare


def indexed(rows, fields):
    result = {}
    for row in rows:
        key = tuple(row[field] for field in fields)
        if key in result:
            raise ValueError(f"Duplicate record for {fields}")
        result[key] = row
    return result


def source_rows(archive, meta):
    result = defaultdict(list)
    for message in read_archive(archive, meta["run_id"], False, meta["observed_ns"]):
        # normalize requires a transport timestamp. Only the payload fields below
        # come from this adapter. Actual availability is checked against Parquet.
        for name, row in normalize(message, 1, TOPICS[message["signal"]]):
            result[name].append(row)
    return result


def check_replay_table(source, replay):
    """Compare payloads, then verify availability using the saved Kafka append time."""
    by_id = indexed(source, ("identity",))
    actual = indexed(replay, ("identity",))
    if by_id.keys() != actual.keys():
        raise ValueError("Source and replay identities differ")
    for key, expected in by_id.items():
        got = actual[key]
        if any(got[k] != value for k, value in expected.items() if k not in ("broker_ms", "available_ms")):
            raise ValueError("Replay changed a normalized source record")
        broker_ms = got["broker_ms"]
        if type(broker_ms) is not int or broker_ms <= 0:
            raise ValueError("Missing recorded replay append time")
        if got["available_ms"] != max(expected["available_ms"], broker_ms):
            raise ValueError("Replay availability differs from event, observation or broker time")


def review(archive, replay):
    meta = read_json(archive / "archive.json")
    if meta["synthetic"] is not False:
        raise ValueError("Expected a measured shopping archive")
    source = source_rows(archive, meta)
    if not all(source[name] for name in ("spans", "metrics", "logs")):
        raise ValueError("Shopping archive is missing spans, metrics or logs")
    if {name: len(rows) for name, rows in source.items()} != meta["counts"]:
        raise ValueError("Archive counts differ from its manifest")
    spans = table(replay / "spans")
    check_replay_table(source["spans"], spans)
    for name in ("metrics", "logs"):
        check_replay_table(source[name], table(replay / name))

    groups = defaultdict(list)
    for span in spans:
        if span["kind"] == "SERVER":
            start = span["end_ns"] // 5_000_000_000 * 5000
            groups[(span["run_id"], span["service"], start)].append(span)
    windows = indexed(validate_windows(table(replay / "service_windows")), ("run_id", "service", "start_ms"))
    if not groups or groups.keys() != windows.keys():
        raise ValueError("Measured and replayed service-window keys differ")
    for key, group in groups.items():
        row = windows[key]
        expected = (len(group), sum(s["error"] for s in group), percentile([s["duration_ms"] for s in group]),
                    max(row["end_ms"], max(s["available_ms"] for s in group)))
        if tuple(row[k] for k in ("requests", "errors", "p95_ms", "available_ms")) != expected:
            raise ValueError("Window disagrees with its recorded SERVER spans")
        if (row["synthetic"] is not False or row["complete"] or row["ready"] is not None
                or row["restart_delta"] is not None):
            raise ValueError("Expected partial, measured windows before any independent coverage join")

    gauges = [r for r in source["metrics"] if r["metric"] in ("cct.pod.ready", "cct.pod.restarts")]
    if not gauges:
        raise ValueError("No pod gauges in the measured archive")
    counts = [row["requests"] for row in windows.values()]
    delays = [row["available_ms"] - row["end_ms"] for row in windows.values()]
    return dict(run_id=meta["run_id"], source_counts=meta["counts"], server_spans=sum(counts),
                services=len({key[1] for key in groups}), five_second_intervals=len({key[2] for key in groups}),
                partial_service_windows=len(windows), request_range=[min(counts), max(counts)],
                complete_windows=0, review_scope="recorded shopping probe before independent coverage join",
                replay_availability_delay_ms=[min(delays), max(delays)], pod_gauge_points=len(gauges),
                first_pod_gauge_ns_after_last_window_end=min(g["event_ns"] for g in gauges)
                    - max(row["end_ms"] for row in windows.values()) * 1_000_000)


def check_original(measured, archive):
    with TemporaryDirectory() as tmp:
        rebuilt = Path(tmp) / "archive"
        adapt(measured, rebuilt)
        for name in ("archive.json", "traces.json", "metrics.json", "logs.json"):
            if read_json(rebuilt / name) != read_json(archive / name):
                raise ValueError(f"Original shopping receipts do not reproduce {name}")


def review_pair(root):
    pair = read_json(root / "docs/evidence/CCT-04-paired-overhead-20261128.json")
    receipt = pair["measured_receipt"]
    off, on = [receipt["runs"][mode]["summary"] for mode in ("off", "on")]
    if off["measured_workload_sha256"] != sha256((root / "workload/baseline_locust.py").read_bytes()).hexdigest():
        raise ValueError("Measured workload differs from the versioned Locust file")
    reproduced = compare(off, on)
    if reproduced != receipt["comparison"] or any(row["failures"] for row in pair["checks"].values()):
        raise ValueError("Matched-pair comparison or failure counts differ")
    return reproduced


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=Path, default=Path("outputs/pipeline/recorded-archive"))
    parser.add_argument("--parquet", type=Path, default=Path("outputs/pipeline/recorded-parquet"))
    parser.add_argument("--measured", type=Path, help="Also verify the archive against Merul's original measured directory")
    args = parser.parse_args()
    if args.measured:
        check_original(args.measured, args.archive)
    result = review(args.archive, args.parquet)
    result["paired_tracing_overhead"] = review_pair(Path(__file__).resolve().parents[1])
    result["original_receipt_checked"] = args.measured is not None
    print(json.dumps(result, indent=2, sort_keys=True))
