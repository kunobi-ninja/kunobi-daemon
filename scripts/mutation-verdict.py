#!/usr/bin/env python3
"""Decide whether a cargo-mutants run in CI passes.

Usage: mutation-verdict.py OUTPUT_DIR EXIT_STATUS [EXPECTED_MUTANTS]

OUTPUT_DIR is the `--output` directory; EXIT_STATUS is cargo-mutants' exit
code. When EXPECTED_MUTANTS is given, the run must have tested exactly that
many mutants.

A run fails when a mutant is missed, when cargo-mutants did not finish (any
exit other than 0, 2 or 3), or when it tested fewer mutants than planned. A
mutant that makes the tests time out changed behavior they observe, so
timeouts are reported as warnings. cargo-mutants' exit code 3 (timeouts)
takes precedence over 2 (missed), so missed.txt is read directly.
"""
import json
import os
from pathlib import Path
import sys

EXIT_MEANINGS = {
    1: "usage error",
    4: "the tests fail without any mutation",
    5: "the diff does not match this tree",
    6: "the diff could not be parsed",
    70: "internal error",
}


def fail(message):
    print(f"::error title=Mutation testing::{message}")
    raise SystemExit(1)


def lines(path):
    return path.read_text().splitlines() if path.exists() else []


def main():
    if len(sys.argv) not in (3, 4):
        raise SystemExit(__doc__)
    report = Path(sys.argv[1]) / "mutants.out"
    status = int(sys.argv[2])
    expected = int(sys.argv[3]) if len(sys.argv) == 4 else None

    if status not in (0, 2, 3):
        meaning = EXIT_MEANINGS.get(status, "unexpected exit code")
        fail(f"cargo mutants exited {status}: {meaning}")

    outcomes = json.loads((report / "outcomes.json").read_text())
    tested = outcomes["total_mutants"]
    if tested == 0:
        fail("cargo mutants tested no mutants")
    if expected is not None and tested != expected:
        fail(f"cargo mutants tested {tested} mutants; the plan has {expected}")

    missed = lines(report / "missed.txt")
    timeouts = lines(report / "timeout.txt")
    summary = (
        f"{tested} mutants: {outcomes['caught']} caught, {len(missed)} missed, "
        f"{len(timeouts)} timed out, {outcomes['unviable']} unviable"
    )
    print(summary)
    if path := os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(path, "a") as out:
            out.write(f"{summary}\n\n")
            for title, names in (("Missed", missed), ("Timed out", timeouts)):
                if names:
                    out.write(f"{title}:\n\n```\n" + "\n".join(names) + "\n```\n\n")

    for name in timeouts:
        print(f"::warning title=Mutant timed out::{name}")
    if missed:
        print("\n".join(missed))
        fail(f"missed mutants: {len(missed)}; no test failed when they were applied")


main()
