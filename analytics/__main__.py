"""Run with python3 -m analytics. All operations read local files only."""
import argparse
import json
from pathlib import Path

from .demo import run as demo
from .benchmark import benchmark
from .evaluate import aggregate, control_alarms, evaluate
from .forecast import predict
from .graph import build_graph
from .health import fit
from .io import archive_spans, read_json, receipt_graph, table, write_json
from .report import write_report


def main():
    p = argparse.ArgumentParser(description=__doc__)
    commands = p.add_subparsers(dest='command', required=True)
    d = commands.add_parser('demo', help='Write a clearly labeled synthetic analysis report')
    d.add_argument('--output', type=Path, default=Path('outputs/analytics-demo'))
    g = commands.add_parser('graph', help='Reconstruct observed dependencies and service metrics')
    source = g.add_mutually_exclusive_group(required=True)
    source.add_argument('--spans', type=Path)
    source.add_argument('--receipt', type=Path, help='Merul journeys.json with server_records')
    source.add_argument('--archive', type=Path, help='Recorded adapter archive directory')
    g.add_argument('--run-id')
    g.add_argument('--cutoff-ms', type=int)
    g.add_argument('--output', type=Path, required=True)
    c = commands.add_parser('calibrate', help='Fit latency limits on declared healthy training intervals')
    c.add_argument('--windows', type=Path, required=True)
    c.add_argument('--split', type=Path, required=True)
    c.add_argument('--healthy-intervals', type=Path, required=True)
    c.add_argument('--min-windows', type=int, default=3)
    c.add_argument('--output', type=Path, required=True)
    f = commands.add_parser('predict', help='Save cutoff-specific replay predictions without outcome labels')
    for name in ('spans', 'windows', 'calibration', 'services', 'output'):
        f.add_argument('--' + name, type=Path, required=True)
    f.add_argument('--run-id', required=True)
    f.add_argument('--cutoff-ms', type=int, required=True)
    f.add_argument('--horizons-ms', type=int, nargs='+', default=[10000, 30000, 60000])
    e = commands.add_parser('evaluate', help='Score saved predictions with separate finalized health outcomes')
    for name in ('prediction', 'outcomes', 'calibration', 'output'):
        e.add_argument('--' + name, type=Path, required=True)
    e.add_argument('--outcome-source', required=True)
    e.add_argument('--root-target', help='Verified initiating intervention, used only in scoring')
    a = commands.add_parser('aggregate', help='Produce averages with equal weight per run')
    a.add_argument('evaluations', type=Path, nargs='+')
    a.add_argument('--output', type=Path, required=True)
    b = commands.add_parser('benchmark', help='Measure local graph construction on synthetic chains')
    b.add_argument('--sizes', type=int, nargs='+', default=[10, 100, 1000])
    b.add_argument('--repetitions', type=int, default=3)
    b.add_argument('--output', type=Path, required=True)
    h = commands.add_parser('control', help='Count alarms in a declared healthy no-fault interval')
    for name in ('windows', 'services', 'calibration', 'output'):
        h.add_argument('--' + name, type=Path, required=True)
    h.add_argument('--run-id', required=True)
    h.add_argument('--start-ms', type=int, required=True)
    h.add_argument('--end-ms', type=int, required=True)
    args = p.parse_args()
    try:
        if args.command == 'demo':
            print('Synthetic verification only. Report: ' + str(demo(args.output).resolve()))
        elif args.command == 'graph':
            if args.receipt:
                if args.cutoff_ms is not None or args.run_id is not None:
                    raise ValueError('receipt mode derives run identity and produces a full retrospective graph')
                graph = receipt_graph(args.receipt)
            elif args.archive:
                spans, meta = archive_spans(args.archive)
                if args.run_id and args.run_id != meta['run_id']:
                    raise ValueError('archive run ID mismatch')
                graph = build_graph(spans, meta['run_id'], args.cutoff_ms)
                graph['availability_source'] = 'source observation upper bound; no Kafka delivery inferred'
            else:
                if not args.run_id:
                    raise ValueError('--run-id is required with --spans')
                graph = build_graph(table(args.spans), args.run_id, args.cutoff_ms)
            print(write_report(args.output, graph).resolve())
        elif args.command == 'calibrate':
            result = fit(table(args.windows), read_json(args.split), read_json(args.healthy_intervals), args.min_windows)
            write_json(args.output, result)
            print(json.dumps(result['latency_limits_ms'], sort_keys=True))
        elif args.command == 'predict':
            result = predict(table(args.spans), table(args.windows), args.run_id, read_json(args.services),
                             args.cutoff_ms, read_json(args.calibration), args.horizons_ms)
            write_json(args.output, result)
            print(args.output.resolve())
        elif args.command == 'evaluate':
            result = evaluate(read_json(args.prediction), table(args.outcomes), read_json(args.calibration),
                              args.outcome_source, args.root_target)
            write_json(args.output, result)
            print(args.output.resolve())
        elif args.command == 'aggregate':
            write_json(args.output, aggregate([read_json(path) for path in args.evaluations]))
            print(args.output.resolve())
        elif args.command == 'benchmark':
            write_json(args.output, benchmark(args.sizes, args.repetitions))
            print(args.output.resolve())
        else:
            write_json(args.output, control_alarms(table(args.windows), args.run_id, read_json(args.services),
                                                  args.start_ms, args.end_ms, read_json(args.calibration)))
            print(args.output.resolve())
    except (OSError, ValueError, KeyError, TypeError, ImportError) as exc:
        p.exit(1, 'Analytics error: ' + str(exc) + '\n')


if __name__ == '__main__':
    main()
