"""Build an offline demonstration from the same reserved productcatalog delay run."""
import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
from hashlib import sha256
from html import escape
import json
from pathlib import Path

from analytics.io import read_json, write_json
from observability.lineage import EXPECTED

ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = ROOT / "docs/evidence/CCT-10-validation-20261128.json"


def verified_edges(examples):
    edges = defaultdict(list)
    for journey, required in EXPECTED.items():
        check = examples[journey]
        pairs = [(a, b) for a, services in required.items() for b in services]
        if (check["passes_declared_path"] is not True or check["missing_edges"]
                or check["matched_edges"] != len(pairs) or check["expected_edges"] != len(pairs)
                or not {node for pair in pairs for node in pair} <= set(check["services"])):
            raise ValueError("Journey evidence does not verify the declared graph: " + journey)
        for pair in pairs:
            edges[pair].append(journey)
    return [dict(caller=a, callee=b, journeys=names) for (a, b), names in sorted(edges.items())]


def build(evidence, campaign=None):
    run_id, = evidence["outcome-evaluation-plan.v2"]["reserved_test_runs"]
    run = evidence["runs"][run_id]
    phase = run["phase-latency"]
    if (not run["passed"] or not phase["effect_and_recovery_passed"] or run["remaining_fault"]
            or run["cleanup_errors"] or not run["coverage_review"]["verified"]):
        raise ValueError("Run is missing verified effect, recovery or coverage")
    supp = run["supplementary_confirmed_outcomes"]
    if supp["run_id"] != run_id or supp["scope"] != "held-out supplementary test":
        raise ValueError("Supplementary score and trial differ")
    fault = run["fault"]
    bounds = run["measurement_interval_utc"]
    def timestamp(ms):
        return datetime.fromtimestamp(ms / 1000, timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    phases = []
    for name, start, end in [
        ("healthy", bounds[0], timestamp(fault["request_started_ms"])),
        ("injection_observed", timestamp(fault["injected_observed_ms"]), timestamp(fault["recovered_observed_ms"])),
        ("recovery", timestamp(fault["recovered_observed_ms"]), bounds[1])
    ]:
        phases.append(dict(phase=name, start_utc=start, end_utc=end, **phase[name]))
    live = run["live_observations"]
    delays = live["missed_or_retried_windows"]
    scores = [s for s in supp["summary"] if s["horizon_ms"] == 30000]
    baseline = next(s for s in scores if s["method"] == "no_additional")
    result = dict(run_id=run_id, target="productcatalogservice", phase_latency=phases,
                  controller_events=run["controller_events"][0], controller_precision_ms=run["controller_timestamp_precision_ms"],
                  edges=verified_edges(run["capture"]["journey_examples"]), decisions=live["prospective_decisions"],
                  label_counts=run["heldout-label-validation"]["counts"], health_controls=run["independent_reference"]["retrospective_no_fault_control_alarms"],
                  observed_deadline_reasons=dict(Counter(delays.values())),
                  flagged_during_injection=sum(fault["injected_observed_ms"] <= int(t) < fault["recovered_observed_ms"] for t in delays),
                  nonempty_no_additional_cutoffs=live["prospective_decisions"]-baseline["empty_empty"],
                  forecasts=[{k:s[k] for k in ("method","f1_run_mean","coverage_run_mean","empty_empty","censored_services")} for s in scores],
                  five_second_labels=None)
    if campaign:
        relative = "runs/" + run_id + "/heldout-label-validation.json"
        payload = (campaign / relative).read_bytes()
        if sha256(payload).hexdigest() != evidence["artifact_hashes"][relative]:
            raise ValueError("Detailed health timeline SHA-256 differs from the published receipt")
        detail = json.loads(payload)
        if detail["run_id"] != run_id or detail["counts"] != result["label_counts"]:
            raise ValueError("Detailed labels do not match the selected trial")
        result["five_second_labels"] = detail["timeline"]
    return result


def table(headings, rows):
    def cells(tag, items):
        return "".join(f"<{tag}>{escape(str(x))}</{tag}>" for x in items)
    return ("<table><thead><tr>" + cells("th", headings) + "</tr></thead><tbody>"
            + "".join("<tr>" + cells("td", r) + "</tr>" for r in rows) + "</tbody></table>")


def graph_view(edges):
    groups = defaultdict(list)
    for edge in edges:
        groups[edge["caller"]].append(edge["callee"])
    cards = []
    for caller, targets in sorted(groups.items()):
        items = "".join("<li>" + escape(name) + "</li>" for name in targets)
        cards.append("<div class='fan'><strong>" + escape(caller) + "</strong><ul>" + items + "</ul></div>")
    return "<div class='graph' aria-label='Caller to callee dependencies'>" + "".join(cards) + "</div>"


def render(data):
    phases = data["phase_latency"]
    names = {"healthy":"Before injection","injection_observed":"Delay observed","recovery":"After recovery"}
    sections = ["<h1>A catalog delay during checkout</h1><p>Recorded Online Boutique shopping run · "
                + escape(data["run_id"]) + "</p><h2>Fault and recovery</h2>"
                "<p>One productcatalog replica experienced a 30-second outgoing delay of 100 ms with 10 ms jitter. "
                "Phase medians use caller RPCs wholly inside the recorded intervals. Times below are UTC.</p>",
                table(("Phase","Start","End","Calls","Median RPC latency"), [
                    (names[p["phase"]],p["start_utc"],p["end_utc"],p["n"],f'{p["median_ms"]:.3f} ms') for p in phases]),
                table(("Controller event","Recorded UTC (one-second precision)"), [(e["operation"],e["timestamp"]) for e in data["controller_events"]]),
                "<h2>Service graph</h2><p>Each row is a directed, verified caller-to-callee edge. "
                "The three recorded journey examples establish this subset of the run’s graph.</p>",
                graph_view(data["edges"]),
                "<details><summary>Journey evidence for each edge</summary>",
                table(("Caller →","Callee","Journey"),[(e["caller"],e["callee"],", ".join(e["journeys"])) for e in data["edges"]]),
                "</details><h2>Service health over time</h2><p>Independent-reference alarm starts in the two no-fault controls. "
                "An unknown frame contributes no observed control time.</p>",
                table(("Interval","UTC start","UTC end","Alarm starts","Unknown frames"),[
                    (key.replace("_"," "),datetime.fromtimestamp(v["start_ms"]/1000,timezone.utc).strftime("%H:%M:%S"),
                     datetime.fromtimestamp(v["end_ms"]/1000,timezone.utc).strftime("%H:%M:%S"),v["alarm_incidents"],v["unknown_windows"])
                    for key,v in sorted(data["health_controls"].items(),key=lambda x:x[1]["start_ms"])]),
                table(("Retrospective agreement","Service-windows"),data["label_counts"].items())]
    if data["five_second_labels"] is None:
        sections.append("<p>The public receipt gives phase measurements and control counts. "
                        "Supply the campaign handoff with <code>--campaign-dir</code> to display each service’s five-second labels.</p>")
    else:
        frames = data["five_second_labels"]
        services = sorted(frames[0]["independent_reference_labels"])
        sections.append(table(["Window end UTC"] + services, [
            [datetime.fromtimestamp(f["end_ms"]/1000,timezone.utc).strftime("%H:%M:%S")]
            + [f["independent_reference_labels"][s] for s in services] for f in frames]))
    sections += ["<h2>Could the system warn 30 seconds ahead?</h2>",
                 table(("Forecast method","Mean F1","Mean scorable coverage","Empty / empty cutoffs"), [
                     (r["method"],r["f1_run_mean"],f'{100*r["coverage_run_mean"]:.1f}%',r["empty_empty"]) for r in data["forecasts"]]),
                 f'<p>{data["nonempty_no_additional_cutoffs"]} of {data["decisions"]} cutoffs had at least one scorable new impact '
                 'under the no-additional-impact baseline. Empty cases have undefined F1. The independently frozen '
                 'supplementary outcome policy applied to this one test. <a href="summary.json">Machine-readable results</a>.</p>']
    style = ("body{font:1rem/1.5 system-ui;margin:auto;padding:2rem;max-width:64rem;color:#172e36}"
             "h1,h2{color:#185747}h2{margin-top:2rem}table{border-collapse:collapse;margin:1rem 0;display:block;overflow:auto}"
             "th,td{padding:.45rem .8rem;border-bottom:1px solid #cadbd7;text-align:left}th{background:#ecf4f1}")
    style += (".graph{display:flex;gap:2rem;flex-wrap:wrap}.fan{display:flex;align-items:center}.fan strong{padding:.45rem;"
              "background:#154e43;color:white}.fan ul{list-style:none;border-left:2px solid #19816b;margin:0;padding:0}"
              ".fan li{padding:.18rem .35rem}.fan li::before{content:'→ ';color:#186251}.fan strong::after{content:' →'}")
    return ('<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">'
            '<title>Cloud Chaos Twin · saved fault run</title><style>'+style+'</style><main>'+"".join(sections)+'</main></html>\n')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("outputs/cct11-demo"))
    parser.add_argument("--campaign-dir", type=Path, help="Extracted cct-validation-20261128-04 directory")
    args = parser.parse_args()
    result = build(read_json(EVIDENCE), args.campaign_dir)
    result["receipt_sha256"] = sha256(EVIDENCE.read_bytes()).hexdigest()
    write_json(args.output / "summary.json", result)
    path = args.output / "index.html"
    path.write_text(render(result), encoding="utf-8")
    print(path.resolve())


if __name__ == "__main__":
    main()

