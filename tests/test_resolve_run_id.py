"""Regression tests for `_resolve_run_id` — the run-state pointer resolver.

Guards the "crawl starts from scratch" fix: a launch must RESUME an existing
non-empty pointer (never silently mint a new run), and the origin tag must
correctly classify every path so the RESUMING-vs-STARTING-NEW startup log is
accurate. See cli.py:_resolve_run_id and the completion-block retention change.
"""
from __future__ import annotations

import uuid

from aisley_scraper.cli import _resolve_run_id


def _is_uuid(value: str) -> bool:
    try:
        uuid.UUID(value)
        return True
    except (ValueError, TypeError, AttributeError):
        return False


def test_new_when_state_file_absent(tmp_path):
    state = tmp_path / ".aisley_active_run_id"
    resolved, old, origin = _resolve_run_id(str(state), None, False)
    assert origin == "new"
    assert old is None
    assert _is_uuid(resolved)
    # brand-new id is persisted for the next launch to resume
    assert state.read_text(encoding="utf-8").strip() == resolved


def test_new_when_state_file_empty(tmp_path):
    state = tmp_path / ".aisley_active_run_id"
    state.write_text("   \n", encoding="utf-8")  # exists but blank
    resolved, old, origin = _resolve_run_id(str(state), None, False)
    assert origin == "new"
    assert old is None
    assert _is_uuid(resolved)
    assert state.read_text(encoding="utf-8").strip() == resolved


def test_resume_when_state_file_present(tmp_path):
    # This is the guarantee behind the fix: a retained pointer RESUMES.
    state = tmp_path / ".aisley_active_run_id"
    state.write_text("run-abc-123", encoding="utf-8")
    resolved, old, origin = _resolve_run_id(str(state), None, False)
    assert origin == "resume"
    assert resolved == "run-abc-123"
    assert old is None
    # resume must not rewrite/mutate the pointer
    assert state.read_text(encoding="utf-8") == "run-abc-123"


def test_adopted_with_explicit_run_id(tmp_path):
    state = tmp_path / ".aisley_active_run_id"
    state.write_text("run-old", encoding="utf-8")
    resolved, old, origin = _resolve_run_id(str(state), "run-pinned", False)
    assert origin == "adopted"
    assert resolved == "run-pinned"
    assert old is None
    assert state.read_text(encoding="utf-8").strip() == "run-pinned"


def test_fresh_mints_new_and_returns_old_for_purge(tmp_path):
    state = tmp_path / ".aisley_active_run_id"
    state.write_text("run-old", encoding="utf-8")
    resolved, old, origin = _resolve_run_id(str(state), None, True)
    assert origin == "fresh"
    assert old == "run-old"  # returned so the caller can purge it
    assert _is_uuid(resolved)
    assert resolved != "run-old"
    assert state.read_text(encoding="utf-8").strip() == resolved


def test_fresh_with_explicit_run_id_uses_it(tmp_path):
    state = tmp_path / ".aisley_active_run_id"
    state.write_text("run-old", encoding="utf-8")
    resolved, old, origin = _resolve_run_id(str(state), "run-forced", True)
    assert origin == "fresh"
    assert resolved == "run-forced"
    assert old == "run-old"
    assert state.read_text(encoding="utf-8").strip() == "run-forced"


def test_fresh_with_no_prior_file_has_no_old(tmp_path):
    state = tmp_path / ".aisley_active_run_id"
    resolved, old, origin = _resolve_run_id(str(state), None, True)
    assert origin == "fresh"
    assert old is None
    assert _is_uuid(resolved)
