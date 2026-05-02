#!/usr/bin/env python3
# =============================================================================
# publish-local.py — Build oshconnect and publish to the local PyPI server.
#
# One-command dev loop: edit code -> run this -> downstream picks up the new
# version via `pip install --index-url http://localhost:8090/simple/ oshconnect`.
#
# The local pypiserver container must be running (started automatically below
# via `docker compose up -d pypi` if it isn't). pypiserver is configured with
# `-o` so re-uploading the same version overwrites — no version bump needed.
#
# Usage:
#   ./scripts/publish-local.py             # build + upload
#   ./scripts/publish-local.py --no-build  # upload existing wheel(s) in dist/
#   LOCAL_PYPI_URL=http://host:port ./scripts/publish-local.py    # override URL
# =============================================================================
from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PYPI_URL = os.environ.get("LOCAL_PYPI_URL", "http://localhost:8090")

CYAN = "\033[0;36m"
GREEN = "\033[0;32m"
RED = "\033[0;31m"
NC = "\033[0m"


def info(msg: str) -> None:
    print(f"{CYAN}[INFO]{NC}  {msg}")


def ok(msg: str) -> None:
    print(f"{GREEN}[OK]{NC}    {msg}")


def fail(msg: str, code: int = 1) -> None:
    print(f"{RED}[FAIL]{NC}  {msg}", file=sys.stderr)
    sys.exit(code)


def pypi_ready(url: str) -> bool:
    """Return True iff the URL responds with a 2xx or 3xx status."""
    try:
        with urllib.request.urlopen(url, timeout=3) as resp:
            return 200 <= resp.status < 400
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, OSError):
        return False


def ensure_pypi(url: str) -> None:
    info(f"Checking local PyPI at {url}")
    if pypi_ready(url):
        ok("Local PyPI is already running")
        return

    info("Local PyPI not running — starting container...")
    res = subprocess.run(
        ["docker", "compose", "up", "-d", "pypi"], cwd=PROJECT_ROOT
    )
    if res.returncode != 0:
        fail("docker compose up failed")

    for i in range(1, 11):
        time.sleep(1)
        if pypi_ready(url):
            ok("Local PyPI started")
            return
        info(f"  waiting... ({i}/10)")

    fail("Could not start local PyPI")


def build_wheel() -> None:
    info("Building wheel...")
    for sub in ("dist", "build"):
        shutil.rmtree(PROJECT_ROOT / sub, ignore_errors=True)
    for egg in (PROJECT_ROOT / "src").glob("*.egg-info"):
        shutil.rmtree(egg, ignore_errors=True)

    res = subprocess.run(["uv", "build"], cwd=PROJECT_ROOT)
    if res.returncode != 0:
        fail("uv build failed")


def find_wheels() -> list[Path]:
    return sorted((PROJECT_ROOT / "dist").glob("*.whl"))


def publish(url: str, wheels: list[Path]) -> None:
    # pypiserver runs with `-a . -P .` (auth disabled), but `uv publish`/
    # pypiserver still issue a Basic-Auth challenge that triggers an
    # interactive prompt. Pass empty credentials to satisfy it.
    info(f"Uploading to {url}")
    cmd = [
        "uv", "publish",
        "--publish-url", url,
        "--username", "",
        "--password", "",
        *[str(w) for w in wheels],
    ]
    res = subprocess.run(cmd, cwd=PROJECT_ROOT)
    if res.returncode != 0:
        fail("uv publish failed")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build oshconnect and publish it to the local PyPI server.",
    )
    parser.add_argument(
        "--no-build",
        action="store_true",
        help="Skip wheel build; upload whatever is in dist/.",
    )
    args = parser.parse_args()

    info(f"Project root: {PROJECT_ROOT}")
    ensure_pypi(PYPI_URL)

    if not args.no_build:
        build_wheel()

    wheels = find_wheels()
    if not wheels:
        fail(
            f"No wheel found in {PROJECT_ROOT}/dist/. "
            "Build first or remove --no-build."
        )

    ok(f"Wheel(s): {' '.join(str(w.relative_to(PROJECT_ROOT)) for w in wheels)}")
    publish(PYPI_URL, wheels)

    ok("Published to local PyPI")
    print()
    print(f"  Browse:    {PYPI_URL}/simple/")
    print(f"  Install:   pip install --index-url {PYPI_URL}/simple/ oshconnect")
    print(f"  uv:        uv pip install --index-url {PYPI_URL}/simple/ oshconnect")
    print(f"  uv sync:   uv sync  (if pyproject.toml has [[tool.uv.index]] configured)")
    return 0


if __name__ == "__main__":
    sys.exit(main())