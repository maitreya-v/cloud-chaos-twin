"""In-cluster OTLP relay. Measure exporter activity immediately around the selected batch."""
import json
import re
import time
from pathlib import Path
from urllib.request import urlopen

if __package__:
    from .signals import post
else:
    from signals import post  # Mounted alongside this script in the relay Job.

def exporter_total():
    with urlopen("http://cct-collector:8888/metrics", timeout=5) as response:
        lines = response.read().decode().splitlines()
    values = []
    for line in lines:
        m = re.fullmatch(r'otelcol_exporter_sent_log_records(?:_total)?\{([^}]*)\}\s+([^\s]+)(?:\s+\d+)?', line)
        if m and re.search(r'(?:^|,)exporter="file/selected"(?:,|$)', m[1]):
            values.append(float(m[2]))
    return sum(values) if values else None

def read_total(result):
    try:
        return exporter_total()
    except (OSError, ValueError) as exc:
        result["scrape_error"] = str(exc)
        return None

def main():
    log_data = json.loads(Path("/probe/logs.json").read_text())
    expected = sum(len(scope["logRecords"]) for g in log_data["resourceLogs"] for scope in g["scopeLogs"])
    result = {"selected": expected, "accepted": [], "exporter": "file/selected",
              "before": None, "after": None, "file_export_count_matches": False}
    try:
        result["before"] = read_total(result)
        for signal, data in (("logs",log_data),("metrics",json.loads(Path("/probe/metrics.json").read_text()))):
            post("http://cct-collector:4318", signal, data)
            result["accepted"].append(signal)
        deadline = time.monotonic() + 25
        while True:
            result["after"] = read_total(result)
            a,b = result["before"],result["after"]
            if a is not None and b is not None:
                result["delta"] = b-a
                result["file_export_count_matches"] = expected > 0 and b-a == expected
                if b-a >= expected:
                    break
            elif a is None and b is not None:
                break  # A first observation can prime the counter for the measured relay.
            if time.monotonic() >= deadline:
                break
            time.sleep(1)
    finally:
        print(json.dumps(result), flush=True)

if __name__ == "__main__":
    main()
