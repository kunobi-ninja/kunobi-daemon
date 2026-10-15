#!/usr/bin/env python3
"""Build two crate revisions and check health and drain in every pairing, and
that a retiring server's refusal reaches new clients without breaking old ones."""
import argparse
import os
from pathlib import Path
import re
import subprocess
import tempfile
import time


def build(source, target):
    subprocess.run(
        ["cargo", "build", "--locked", "--all-features", "--example", "lifecycle_server", "--example", "blocking_health"],
        cwd=source,
        env={**os.environ, "CARGO_TARGET_DIR": str(target)},
        check=True,
        timeout=300,
    )
    return target / "debug" / "examples"


def additive(old, new):
    """Every line of the old text survives, in order. The schema may gain fields,
    values and messages; it may not change or remove any."""
    lines = iter(new.splitlines())
    return all(line in lines for line in old.splitlines())


def schema_problems(old, new):
    """Why the new schema text cannot replace the old one; empty if it can."""
    return [] if additive(old, new) else ["the schema changed or lost a line"]


# Edits of the released schema, and whether a peer built from the edited one
# still interoperates with the released one. The checker must agree with each.
SCHEMA_EDITS = [
    ("  uint64 sequence = 2;\n", "  /*\n  uint64 sequence = 2;\n  */\n  fixed64 sequence = 2;\n", False),
    ("bytes token = 4;", "bytes attempt = 4;", True),
    ("  bool retiring = 10;\n}", "  bool retiring = 10;\n  string reason = 11;\n}", True),
    ("uint32 process_id = 1;\n  uint64 generation = 2;", "string process_id = 1;\n  uint64 generation = 2;", False),
    ("optional uint64 offset = 5;", "uint64 offset = 5;", False),
    ("bytes payload = 6;", "repeated bytes payload = 6;", False),
    ("  string build = 3;\n", "  reserved 3;\n", False),
    ("  RETIRING = 4;\n", "", False),
    ("MessageKind kind = 7;", "LifecycleChange kind = 7;", False),
]


def check_schema_checker(released):
    for before, after, compatible in SCHEMA_EDITS:
        assert released.count(before) == 1, f"edit no longer applies: {before!r}"
        problems = schema_problems(released, released.replace(before, after))
        assert (not problems) == compatible, f"checker misjudged {before!r} -> {after!r}: {problems}"


def frozen_contract(old, new):
    schema = "proto/lifecycle.proto"
    released = (old / schema).read_text()
    check_schema_checker(released)
    problems = schema_problems(released, (new / schema).read_text())
    assert not problems, f"lifecycle schema is incompatible: {problems}"
    for file, pattern in [
        ("src/wire.rs", r"pub const VERSION: u32 = \d+;"),
        ("src/readiness/channel.rs", r'pub const ENV: &str = "[^"]+";'),
        ("src/identity.rs", r'pub const CONTROL_SOCKET:.*'),
        ("src/publication.rs", r'lock.push\("\.record.lock"\);'),
        ("src/local/unix_socket.rs", r'path.push\("\.bind.lock"\);'),
    ]:
        before = re.search(pattern, (old / file).read_text())
        after = re.search(pattern, (new / file).read_text())
        assert before and after and before.group() == after.group(), f"contract changed: {file}"


def check(server, client):
    with tempfile.TemporaryDirectory(prefix="kdaemon-interop-") as directory:
        root = Path(directory)
        with (root / "server.log").open("w+") as log:
            process = subprocess.Popen([str(server / "lifecycle_server"), directory], stdout=log, stderr=log)
            try:
                deadline = time.monotonic() + 15
                while True:
                    result = subprocess.run([str(client / "blocking_health"), directory], capture_output=True, text=True, timeout=5)
                    if result.returncode == 0:
                        break
                    if process.poll() is not None or time.monotonic() >= deadline:
                        log.seek(0)
                        raise AssertionError(f"server did not become ready: {log.read()} {result.stderr}")
                    time.sleep(0.05)
                expected = f"pid={process.pid} ready=true draining=false active=0"
                assert result.stdout.strip() == expected, result.stdout
                drained = subprocess.run([str(client / "blocking_health"), directory, "drain"], capture_output=True, text=True, check=True, timeout=5)
                expected = f"pid={process.pid} ready=false draining=true active=0"
                assert drained.stdout.strip() == expected, drained.stdout
                assert process.wait(timeout=10) == 0, "server failed after drain"
                assert not (root / "control.sock").exists(), "endpoint survived shutdown"
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=5)


def refusal(server, client, reads_refusal):
    """A client that reads refusals learns the server is retiring. Any other
    client sees the connection close, as it always has."""
    with tempfile.TemporaryDirectory(prefix="kdaemon-refusal-") as directory:
        root = Path(directory)
        with (root / "server.log").open("w+") as log:
            process = subprocess.Popen([str(server / "lifecycle_server"), directory, "retiring"], stdout=log, stderr=log)
            try:
                deadline = time.monotonic() + 15
                while not (root / "control.sock").exists():
                    if process.poll() is not None or time.monotonic() >= deadline:
                        log.seek(0)
                        raise AssertionError(f"server did not bind: {log.read()}")
                    time.sleep(0.05)
                result = subprocess.run([str(client / "blocking_health"), directory], capture_output=True, text=True, timeout=5)
                assert result.returncode != 0, result.stdout
                if reads_refusal:
                    assert "ConnectionRefused" in result.stderr and "Retiring" in result.stderr, result.stderr
                else:
                    assert "UnexpectedEof" in result.stderr, result.stderr
            finally:
                process.kill()
                process.wait(timeout=5)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("old", type=Path)
    parser.add_argument("new", type=Path)
    args = parser.parse_args()
    old, new = args.old.resolve(), args.new.resolve()
    frozen_contract(old, new)
    with tempfile.TemporaryDirectory(prefix="kdaemon-build-") as directory:
        binaries = {"v0.8.0": build(old, Path(directory) / "old"), "current": build(new, Path(directory) / "new")}
        for server_name, server in binaries.items():
            for client_name, client in binaries.items():
                check(server, client)
                print(f"PASS server={server_name} client={client_name}", flush=True)
        # Only the current server can retire; the old one ignores the argument.
        for client_name, client in binaries.items():
            refusal(binaries["current"], client, reads_refusal=client_name == "current")
            print(f"PASS refusal server=current client={client_name}", flush=True)


if __name__ == "__main__":
    main()
