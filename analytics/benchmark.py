"""Measure graph construction on invented service chains; no failure-accuracy claim."""
import platform
from statistics import median
import time

from .graph import build_graph


def benchmark(sizes=(10, 100, 1000), repetitions=3):
    results = []
    if repetitions < 1 or any(n < 2 or n > 10000 for n in sizes):
        raise ValueError('use 2..10000 services and positive repetitions')
    for count in sizes:
        rows = []
        for i in range(count):
            rows.append(dict(run_id='synthetic-scale', synthetic=True, trace_id='1' * 32,
                             span_id=f'{i + 1:016x}', parent_span_id=f'{i:016x}' if i else None,
                             kind='SERVER', service=f'service-{i:05d}', start_ns=1000000,
                             end_ns=2000000, duration_ms=1.0, error=False, available_ms=3))
        durations = []
        for _ in range(repetitions):
            start = time.perf_counter()
            graph = build_graph(rows, 'synthetic-scale', 3)
            durations.append(time.perf_counter() - start)
            if len(graph['nodes']) != count or len(graph['edges']) != count - 1:
                raise RuntimeError('graph benchmark count mismatch')
        results.append(dict(services=count, spans=count, edges=count - 1,
                            seconds=durations, median_seconds=median(durations)))
    return dict(schema_version='1.0', synthetic=True, python=platform.python_version(),
                platform=platform.platform(), machine=platform.machine(), repetitions=repetitions,
                scope='local graph construction; invented chains; excludes Kafka, Spark and live application',
                results=results)
