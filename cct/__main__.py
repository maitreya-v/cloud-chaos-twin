"""Run with python3 -m cct from the repository root."""

import argparse
import json
from pathlib import Path

from .demo import load_spans, write_report
from .doctor import environment_report

ROOT = Path(__file__).resolve().parent.parent


def main():
    parser = argparse.ArgumentParser(description="Cloud Chaos Twin offline starter")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("doctor", help="Check local prerequisites without installing anything")
    for name in ("check", "demo"):
        sub = commands.add_parser(name, help="Validate normalized spans" if name == "check" else "Write an offline demo report")
        sub.add_argument("--input", type=Path, default=ROOT / "data/samples/spans.json")
        if name == "demo":
            sub.add_argument("--output", type=Path, default=ROOT / "outputs/demo")
    args = parser.parse_args()
    if args.command == "doctor":
        print(json.dumps(environment_report(ROOT), indent=2))
        return
    try:
        dataset = load_spans(args.input)
        if args.command == "check":
            print("Validated {} normalized synthetic spans.".format(len(dataset["spans"])))
        else:
            result = write_report(dataset, args.output)
            print("Synthetic demo only; no real faults or predictions.")
            print("Report: {}".format(result.resolve()))
    except (OSError, ValueError, KeyError, TypeError) as error:
        parser.exit(1, "Error: {}\n".format(error))


if __name__ == "__main__":
    main()
