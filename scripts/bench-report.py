#!/usr/bin/env python3
"""Tabulate Gungraun instruction counts as Markdown, one row per benchmark.

Reads every summary.json that `cargo bench -- --save-summary=json` wrote under
the Gungraun home directory. With --limit, exits 2 when any benchmark's
instruction count grew by more than that percentage over its baseline, so the
table and the gate always agree. A benchmark without a baseline is listed as
new and never fails.
"""
import argparse
import json
import sys
from pathlib import Path


def instructions(summary):
    """(new, old, regressed) Callgrind instruction counts; old is None when new."""
    for profile in summary.get("profiles", []):
        if profile.get("tool") != "Callgrind":
            continue
        total = profile["data"]["total"]
        values = total["metrics"]["Ir"]["values"]
        return values.get("new"), values.get("old"), bool(total.get("regressions"))
    raise SystemExit(f"no Callgrind profile in {summary.get('module_path')}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("home", type=Path, help="Gungraun home (GUNGRAUN_HOME)")
    parser.add_argument("--limit", type=float, help="fail above this growth in percent")
    parser.add_argument("--baseline", help="what the old counts were measured on")
    args = parser.parse_args()

    rows = []
    for path in sorted(args.home.rglob("summary.json")):
        summary = json.loads(path.read_text())
        name = summary["module_path"]
        if summary.get("id"):
            name += f" {summary['id']}"
        new, old, flagged = instructions(summary)
        delta = None if old in (None, 0) else (new - old) * 100 / old
        regressed = flagged or (
            args.limit is not None and delta is not None and delta > args.limit
        )
        rows.append((name, new, old, delta, regressed))
    if not rows:
        raise SystemExit(f"no benchmark summaries under {args.home}")
    rows.sort()

    regressed = sum(row[4] for row in rows)
    new = sum(row[2] is None for row in rows)
    print("## Instruction counts\n")
    if args.baseline:
        print(f"Baseline: {args.baseline}.", end=" ")
    if args.limit is not None:
        print(f"The gate fails when a benchmark grows by more than {args.limit:g}%.", end="")
    print("\n")
    print(f"{len(rows)} benchmarks, {regressed} over the limit, {new} without a baseline.\n")
    print("| Benchmark | Base | Head | Change | |")
    print("| --- | ---: | ---: | ---: | --- |")
    for name, new_count, old, delta, over in rows:
        base = "new" if old is None else f"{old:,}"
        change = "" if delta is None else f"{delta:+.2f}%"
        mark = "regressed" if over else ""
        print(f"| `{name}` | {base} | {new_count:,} | {change} | {mark} |")
    return 2 if regressed else 0


if __name__ == "__main__":
    sys.exit(main())
