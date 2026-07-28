"""Library logging hygiene — GitHub issue #48.

OSHConnect used to log via the module-level `logging.warning(...)` /
`logging.error(...)` functions, which write to the **root** logger. Two
consequences for anything embedding the library:

1. An application could not raise OSHConnect to DEBUG or silence its
   warnings without reconfiguring the root logger, affecting every other
   library in the process.
2. Calling `logging.warning(...)` on the root logger auto-installs a
   stderr handler when none exists — a library writing to its consumer's
   stderr uninvited.

These tests pin the fix: every module logs to `oshconnect.<module>`, and
the package logger carries a NullHandler so the library never configures
logging on the application's behalf.
"""
from __future__ import annotations

import logging
import pkgutil
import re
from pathlib import Path

import oshconnect

SRC_ROOT = Path(__file__).resolve().parents[1] / "src" / "oshconnect"

# Matches a root-logger call: `logging.warning(`, `logging.error(`, etc.,
# but not `logger.warning(` / `self.logging.error(`.
ROOT_LOGGER_CALL = re.compile(
    r"(?:^|[^.\w])logging\.(debug|info|warning|error|exception)\s*\(")


def test_no_module_logs_to_the_root_logger():
    """Regression guard: a new `logging.warning(...)` anywhere under
    src/oshconnect would silently reintroduce the root-logger leak, and
    nothing else in the suite would notice."""
    offenders = []
    for path in SRC_ROOT.rglob("*.py"):
        for lineno, line in enumerate(path.read_text().splitlines(), 1):
            stripped = line.strip()
            if stripped.startswith("#"):
                continue
            if ROOT_LOGGER_CALL.search(line):
                rel = path.relative_to(SRC_ROOT.parent.parent)
                offenders.append(f"{rel}:{lineno}: {stripped}")

    assert not offenders, (
        "These call the root logger directly; use a module-level "
        "`logger = logging.getLogger(__name__)` instead:\n  "
        + "\n  ".join(offenders)
    )


def test_package_logger_has_a_null_handler():
    """Without this, the first OSHConnect warning installs a stderr handler
    on the root logger for the whole host application."""
    handlers = logging.getLogger("oshconnect").handlers
    assert any(isinstance(h, logging.NullHandler) for h in handlers), (
        f"oshconnect package logger has no NullHandler (handlers={handlers})"
    )


def test_logging_modules_use_their_own_dunder_name():
    """Each module's logger must be `oshconnect.<module path>` so that
    configuring `oshconnect` controls all of them and nothing else."""
    mismatched = []
    for path in SRC_ROOT.rglob("*.py"):
        text = path.read_text()
        if "logging.getLogger(" not in text:
            continue
        for lineno, line in enumerate(text.splitlines(), 1):
            if "logging.getLogger(" not in line or line.strip().startswith("#"):
                continue
            # __name__ is the only correct argument; a hardcoded string
            # drifts the moment a module is renamed or moved.
            if "__name__" not in line:
                rel = path.relative_to(SRC_ROOT.parent.parent)
                mismatched.append(f"{rel}:{lineno}: {line.strip()}")

    assert not mismatched, (
        "getLogger() should be passed __name__:\n  " + "\n  ".join(mismatched)
    )


def test_consumer_can_silence_oshconnect_without_touching_root(caplog):
    """The actual user-facing win: one setLevel on the package logger
    controls the whole library, and OSHConnect records never land on a
    handler the consumer didn't ask for."""
    pkg_logger = logging.getLogger("oshconnect")
    child = logging.getLogger("oshconnect.resources.base")

    original_level = pkg_logger.level
    try:
        pkg_logger.setLevel(logging.CRITICAL)
        # `caplog.set_level` would override the very thing under test, so
        # attach at the root and assert on propagation instead.
        with caplog.at_level(logging.DEBUG, logger="oshconnect"):
            pass
        pkg_logger.setLevel(logging.CRITICAL)
        caplog.clear()
        child.warning("should be suppressed by the package-level setLevel")
        assert not [r for r in caplog.records if r.name.startswith("oshconnect")]

        pkg_logger.setLevel(logging.DEBUG)
        caplog.clear()
        with caplog.at_level(logging.DEBUG):
            child.warning("should now be visible")
        assert any(r.name == "oshconnect.resources.base"
                   for r in caplog.records)
    finally:
        pkg_logger.setLevel(original_level)


def test_every_submodule_imports_cleanly():
    """The NullHandler wiring lives in `oshconnect/__init__.py`; make sure
    adding it didn't break import of any submodule (e.g. via a cycle)."""
    failures = []
    for mod in pkgutil.walk_packages(oshconnect.__path__,
                                     prefix="oshconnect."):
        name = mod.name
        try:
            __import__(name)
        except Exception as e:  # noqa: BLE001 - reporting, not handling
            # Transport/codec extras raise a helpful RuntimeError when the
            # optional dep is absent; that's by design, not an import break.
            if isinstance(e, ImportError) and "oshconnect[" in str(e):
                continue
            failures.append(f"{name}: {type(e).__name__}: {e}")
    assert not failures, "submodules failed to import:\n  " + "\n  ".join(failures)
