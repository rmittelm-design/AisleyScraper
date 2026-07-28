"""The pre-crawl orphan-storage preflight must never wedge or abort the crawl.

It walks the ENTIRE Supabase Storage bucket, so on a large bucket it can run
effectively forever. It must be (a) skippable via config, (b) hard-bounded by a
wall-clock cap, and (c) non-fatal on timeout/error — always letting scraping run.
"""
from __future__ import annotations

import time
from types import SimpleNamespace

from aisley_scraper import cli


def _settings(*, enabled: bool = True, timeout: int = 120) -> SimpleNamespace:
    return SimpleNamespace(
        crawl_orphan_preflight_enabled=enabled,
        crawl_orphan_preflight_timeout_sec=timeout,
    )


def test_preflight_skipped_when_disabled(monkeypatch) -> None:
    calls = []
    monkeypatch.setattr(cli, "detect_orphan_storage_objects", lambda s: calls.append(1))
    cli._run_orphan_preflight(_settings(enabled=False))
    assert calls == []  # audit never even started


def test_preflight_bounded_when_detect_hangs(monkeypatch) -> None:
    def _hang(_settings_arg):
        time.sleep(30)  # simulate a huge bucket / slow-streamed storage list
        return {"orphan_paths": [], "linked_paths": 0, "stored_paths": 0}

    deleted = []
    monkeypatch.setattr(cli, "detect_orphan_storage_objects", _hang)
    monkeypatch.setattr(
        cli, "delete_orphan_storage_objects", lambda *a, **k: deleted.append(1)
    )

    started = time.monotonic()
    cli._run_orphan_preflight(_settings(enabled=True, timeout=1))  # must NOT hang 30s
    elapsed = time.monotonic() - started

    assert elapsed < 10, f"preflight was not bounded (took {elapsed:.1f}s)"
    assert deleted == []  # timed-out audit must not trigger the destructive delete


def test_preflight_nonfatal_on_error(monkeypatch) -> None:
    def _boom(_settings_arg):
        raise RuntimeError("storage API 500")

    monkeypatch.setattr(cli, "detect_orphan_storage_objects", _boom)
    # Must return normally (best-effort), not propagate the error and fail the crawl.
    cli._run_orphan_preflight(_settings(enabled=True, timeout=5))


def test_preflight_passes_cleanly_when_no_orphans(monkeypatch) -> None:
    deleted = []
    monkeypatch.setattr(
        cli,
        "detect_orphan_storage_objects",
        lambda s: {"orphan_paths": [], "linked_paths": 5, "stored_paths": 5},
    )
    monkeypatch.setattr(
        cli, "delete_orphan_storage_objects", lambda *a, **k: deleted.append(1)
    )
    cli._run_orphan_preflight(_settings(enabled=True, timeout=5))
    assert deleted == []  # nothing to delete when there are no orphans
