#!/usr/bin/env python3
"""Build two crate revisions and check health and drain in every pairing."""
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


def frozen_contract(old, new):
    assert (old / "proto/lifecycle.proto").read_bytes() == (new / "proto/lifecycle.proto").read_bytes(), "lifecycle schema changed"
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


if __name__ == "__main__":
    main()
