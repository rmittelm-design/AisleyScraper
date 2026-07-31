from __future__ import annotations

import random

from aisley_scraper.cli import _select_shard


def test_select_shard_no_sharding_returns_all() -> None:
    sites = ["https://a.com", "https://b.com"]
    assert _select_shard(sites, 0, 1) == sites


def test_select_shard_partition_is_disjoint_and_complete() -> None:
    sites = [f"https://store{i}.example.com" for i in range(200)]
    shard_count = 7
    shards = [_select_shard(sites, k, shard_count) for k in range(shard_count)]

    flat = [s for shard in shards for s in shard]
    # Every site appears in exactly one shard.
    assert sorted(flat) == sorted(sites)
    assert len(flat) == len(set(flat))


def test_select_shard_is_order_independent_and_deterministic() -> None:
    sites = [f"https://store{i}.example.com" for i in range(200)]
    shuffled = sites[:]
    random.shuffle(shuffled)

    # Same membership regardless of input order or process (stable hash, not the
    # PYTHONHASHSEED-randomized built-in hash()).
    for k in range(4):
        assert set(_select_shard(sites, k, 4)) == set(_select_shard(shuffled, k, 4))
