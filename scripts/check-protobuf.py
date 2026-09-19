#!/usr/bin/env python3
"""Check Rust's golden bodies against the independent protoc encoder."""
from pathlib import Path
import subprocess

root = Path(__file__).resolve().parents[1]
for name, message in [("hello", "Hello"), ("control", "Control")]:
    fixture = root / "tests/fixtures/wire" / name
    encoded = subprocess.run(
        [
            "protoc",
            f"--proto_path={root / 'proto'}",
            f"--encode=kunobi.daemon.v2.{message}",
            "lifecycle.proto",
        ],
        input=fixture.with_suffix(".textproto").read_bytes(),
        stdout=subprocess.PIPE,
        check=True,
    ).stdout
    if encoded != fixture.with_suffix(".bin").read_bytes():
        raise SystemExit(f"Protobuf compatibility fixture differs: {message}")
print("Protoc matches the Hello and Control golden bodies")
