"""Portable HTML analysis artifact: no network assets or runtime dependencies."""
import csv
from html import escape
from pathlib import Path

from .io import write_json


def write_report(output, graph, prediction=None, evaluation=None, timeline=None):
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    write_json(output / 'graph.json', graph)
    rows = graph['service_metrics']
    if rows:
        with (output / 'service_metrics.csv').open('w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
    def cell(value):
        colors = {'healthy': '#e2f3e9', 'affected': '#ffe0dc', 'unknown': '#fff0c2'}
        color = colors.get(value, 'white') if isinstance(value, str) else 'white'
        if isinstance(value, float):
            text = format(value, '.4g')
        elif isinstance(value, list):
            text = ', '.join(map(str, value)) or '—'
        else:
            text = str(value) if value is not None else '—'
        return '<td style="background:' + color + '">' + escape(text) + '</td>'

    def tabulate(records, columns):
        return '<table><thead><tr>' + ''.join('<th>' + escape(k.replace('_', ' ')) + '</th>' for k in columns) + '</tr></thead><tbody>' + ''.join(
            '<tr>' + ''.join(cell(r.get(k, '')) for k in columns) + '</tr>' for r in records) + '</tbody></table>'
    # Put callers before their callees when possible; service-level cycles remain valid graphs.
    remaining, nodes = set(graph['nodes']), []
    while remaining:
        roots = sorted(s for s in remaining if not any(e['target'] == s and e['source'] in remaining for e in graph['edges']))
        if not roots:
            roots = sorted(remaining)
        nodes.extend(roots)
        remaining.difference_update(roots)
    positions = {s: (180 + (i % 3) * 330, 55 + (i // 3) * 150) for i, s in enumerate(nodes)}
    svg = ['<svg role="img" aria-label="Observed caller to callee dependency graph" viewBox="0 0 1020 %s">' % max(200, ((len(nodes) + 2) // 3) * 150),
           '<defs><marker id="arrow" markerWidth="8" markerHeight="8" refX="7" refY="3" orient="auto"><path d="M0,0 L0,6 L8,3 z" fill="#486581"/></marker></defs>']
    for edge in graph['edges']:
        x1, y1 = positions[edge['source']]
        x2, y2 = positions[edge['target']]
        dx, dy = x2 - x1, y2 - y1
        fraction = min(125 / abs(dx) if dx else float('inf'), 20 / abs(dy) if dy else float('inf'))
        svg.append(f'<path d="M{x1+dx*fraction},{y1+dy*fraction} L{x2-dx*fraction},{y2-dy*fraction}" fill="none" stroke="#486581" stroke-width="2" marker-end="url(#arrow)"/>')
    for s, (x, y) in positions.items():
        svg.append(f'<rect x="{x-125}" y="{y-20}" width="250" height="40" rx="8" fill="#e6f0fa" stroke="#486581"/><text x="{x}" y="{y+5}" text-anchor="middle">{escape(s)}</text>')
    svg.append('</svg>')
    sections = ['<h2>Observed dependencies</h2><p>Arrows point from caller to callee. Failure impact may travel in the reverse direction.</p>', ''.join(svg),
                tabulate(graph['edges'], ['source', 'target', 'call_count', 'errors', 'p95_ms']),
                '<h2>Service baseline metrics</h2><p>SERVER spans only; p95 uses nearest rank. Counts below 20 are flagged as low sample size.</p>',
                tabulate(rows, ['service', 'call_count', 'error_fraction', 'mean_ms', 'p95_ms', 'low_count'])]
    if prediction:
        write_json(output / 'prediction.json', prediction)
        sections.extend(['<h2>Health at prediction cutoff</h2>',
                         tabulate([dict(service=s, state=v) for s, v in prediction['states'].items()], ['service', 'state']),
                         '<h2>Independent root rankings</h2>',
                         tabulate([dict(method=m, ranked_roots=', '.join(v)) for m, v in prediction['roots'].items()], ['method', 'ranked_roots'])])
    if timeline:
        write_json(output / 'timeline.json', timeline)
        origin = timeline[0]['end_ms'] - 5000
        display = [dict(r, elapsed_seconds=(r['end_ms'] - origin) / 1000) for r in timeline]
        sections += ['<h2>Observed health timeline</h2><p>Seconds from the first displayed window start; exact UTC millisecond times are saved in timeline.json.</p>', tabulate(display, ['elapsed_seconds'] + nodes)]
    if evaluation:
        write_json(output / 'evaluation.json', evaluation)
        sections += ['<h2>Forecast comparison</h2><p>Empty/empty cases have no F1. Unknown outcomes are censored. Retrospective emission times do not establish live warning.</p>',
                     tabulate(evaluation['forecasts'], ['method', 'horizon_ms', 'predicted_new', 'actual_new', 'f1', 'coverage', 'censored'])]
    label = 'SYNTHETIC FIXTURE — software verification only' if graph['synthetic'] else 'RECORDED TELEMETRY — retrospective analysis'
    page = '<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>Cloud Chaos Twin — Mithil analytics</title><style>body{font:16px/1.5 system-ui;color:#16324f;background:#f4f7fb;margin:0}main{max-width:1100px;margin:auto;padding:32px}table{border-collapse:collapse;width:100%;font-size:14px;margin:20px 0;background:white}td,th{border:1px solid #d9e2ec;padding:9px;text-align:left}h2{margin-top:36px}.notice{padding:16px;background:#fff0c2}svg{width:100%;background:white}section{overflow:auto}a{color:#075985}</style><main><h1>Cloud Chaos Twin: dependency and forecast analysis</h1><p class="notice">' + label + '</p><p>Run: ' + escape(graph['run_id']) + '. Missing parent spans: ' + str(graph['missing_parent_spans']) + '.</p><section>' + ''.join(sections) + '</section><p><a href="graph.json">Graph JSON</a> · <a href="service_metrics.csv">Service metrics CSV</a></p></main></html>'
    (output / 'report.html').write_text(page, encoding='utf-8')
    return output / 'report.html'
