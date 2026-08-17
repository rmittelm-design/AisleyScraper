"""Tests for _batched_upsert_with_retry — the connection-timeout robustness fix.

A transient DB connection blip (psycopg.OperationalError: connection timeout /
pooler saturation) must be retried with backoff instead of failing the store
(that's what killed `bohme` at 90%). A data error must NOT be retried (it can't
recover) and instead trigger the per-product fallback. A sustained outage must
stop after `attempts` and report conn_outage (so the caller fails the page rather
than hammering per-product).
"""
from __future__ import annotations

import psycopg

from aisley_scraper.cli import _batched_upsert_with_retry


class _Logger:
    def warning(self, *args, **kwargs):
        pass


class _Repo:
    """upsert_products_batch raises the next queued exception (None = success)."""

    def __init__(self, script):
        self.script = list(script)
        self.calls = 0

    def upsert_products_batch(self, store_id, products):
        i = min(self.calls, len(self.script) - 1)
        self.calls += 1
        exc = self.script[i]
        if exc is not None:
            raise exc


def _run(script, **kw):
    slept = []
    repo = _Repo(script)
    outcome, exc = _batched_upsert_with_retry(
        repo, 1, [1, 2, 3], logger=_Logger(), sleep=slept.append, **kw
    )
    return outcome, exc, repo.calls, slept


def test_success_first_try_no_sleep():
    outcome, exc, calls, slept = _run([None])
    assert outcome == "ok" and exc is None
    assert calls == 1 and slept == []


def test_transient_connection_error_then_success():
    err = psycopg.OperationalError("connection timeout expired")
    outcome, exc, calls, slept = _run([err, err, None])
    assert outcome == "ok"           # recovered — store is NOT failed
    assert calls == 3
    assert slept == [1.0, 2.0]       # exponential backoff between the two retries


def test_sustained_connection_outage_reports_conn_outage():
    err = psycopg.OperationalError("pooler down")
    outcome, exc, calls, slept = _run([err], attempts=4)
    assert outcome == "conn_outage"
    assert isinstance(exc, psycopg.OperationalError)
    assert calls == 4                # tried `attempts` times
    assert slept == [1.0, 2.0, 4.0]  # backoff between each, none after the last


def test_data_error_is_not_retried():
    outcome, exc, calls, slept = _run([ValueError("poison row")])
    assert outcome == "data_error"
    assert isinstance(exc, ValueError)
    assert calls == 1                # a data error can't recover — no retry
    assert slept == []


def test_backoff_is_capped():
    err = psycopg.OperationalError("down")
    outcome, exc, calls, slept = _run([err], attempts=8)
    assert outcome == "conn_outage"
    assert calls == 8
    # 1,2,4,8,15(capped),15(capped),15(capped) — no runaway backoff
    assert slept == [1.0, 2.0, 4.0, 8.0, 15.0, 15.0, 15.0]
