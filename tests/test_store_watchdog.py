"""Tests for the per-store idle (no-progress) watchdog that replaced the fixed
20-min wall-clock cap. Guarantees: a big-but-progressing store is never killed;
a store making no forward progress IS aborted; the optional hard ceiling still
works; and with the hard ceiling disabled a long-but-progressing task finishes.
"""
from __future__ import annotations

import asyncio

import pytest

from aisley_scraper.cli import _StoreWatchdogTimeout, _await_task_with_idle_watchdog


async def _progress_n_times(progress: dict, n: int, step: float, result):
    """Bump progress every `step` seconds, `n` times, then return `result`."""
    loop = asyncio.get_running_loop()
    for _ in range(n):
        await asyncio.sleep(step)
        progress["ts"] = loop.time()
        progress["ticks"] += 1
    return result


async def _never_progress(_progress: dict):
    await asyncio.sleep(30)  # simulates a wedge — never bumps progress
    return True


async def _progress_forever(progress: dict, step: float):
    loop = asyncio.get_running_loop()
    while True:
        await asyncio.sleep(step)
        progress["ts"] = loop.time()
        progress["ticks"] += 1


def _run(coro_factory, *, idle, hard, poll):
    async def runner():
        loop = asyncio.get_running_loop()
        progress = {"ts": loop.time(), "ticks": 0}
        task = asyncio.create_task(coro_factory(progress))
        return await _await_task_with_idle_watchdog(
            task, progress, idle=idle, hard=hard, poll=poll
        )

    return asyncio.run(runner())


def test_progressing_task_is_not_killed():
    # bumps every 0.05s; idle window 0.2s is never exhausted -> completes
    result = _run(
        lambda p: _progress_n_times(p, 4, 0.05, "DONE"), idle=0.2, hard=0, poll=0.03
    )
    assert result == "DONE"


def test_long_but_progressing_task_finishes_with_hard_disabled():
    # runs ~0.3s, LONGER than the idle window (0.15s), but keeps progressing and
    # hard=0 -> must finish, proving a big store is never falsely killed
    result = _run(
        lambda p: _progress_n_times(p, 10, 0.03, "OK"), idle=0.15, hard=0, poll=0.02
    )
    assert result == "OK"


def test_stalled_task_is_aborted():
    with pytest.raises(_StoreWatchdogTimeout) as ei:
        _run(_never_progress, idle=0.1, hard=0, poll=0.03)
    assert "idle" in str(ei.value)


def test_hard_ceiling_fires_even_while_progressing():
    # keeps progressing so idle never trips (idle=10), but hard=0.2 aborts it
    with pytest.raises(_StoreWatchdogTimeout) as ei:
        _run(lambda p: _progress_forever(p, 0.02), idle=10, hard=0.2, poll=0.03)
    assert "timeout" in str(ei.value)


def test_config_defaults(monkeypatch):
    monkeypatch.delenv("CRAWL_STORE_IDLE_TIMEOUT_SEC", raising=False)
    monkeypatch.delenv("CRAWL_STORE_TOTAL_TIMEOUT_SEC", raising=False)
    from aisley_scraper.config import Settings

    s = Settings()
    assert s.crawl_store_idle_timeout_sec == 600
    assert s.crawl_store_total_timeout_sec == 0  # absolute ceiling disabled by default


def test_config_idle_env_override(monkeypatch):
    monkeypatch.setenv("CRAWL_STORE_IDLE_TIMEOUT_SEC", "123")
    from aisley_scraper.config import Settings

    assert Settings().crawl_store_idle_timeout_sec == 123
