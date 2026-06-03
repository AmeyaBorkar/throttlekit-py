"""Cross-language conformance for the async direct door: Python (redis.asyncio) → vendored Lua → Redis.

The async twin of ``tests/test_redis_backend.py``: it replays the **same** full, time-parametrized
golden-vector suites through :class:`~throttlekit.AsyncRedisBackend` and asserts every reply field is
bit-identical to the Node oracle (``resetAt`` shifts by the same window-aligned ``BASE``). Skipped unless
``redis-py`` is installed and a Redis is reachable; each suite runs in its own ``asyncio.run``.
"""

from __future__ import annotations

import asyncio
import json
import os
import pathlib
from typing import Any

import pytest

redis = pytest.importorskip("redis")
import redis.asyncio as aioredis  # noqa: E402  (after importorskip)

from throttlekit import (  # noqa: E402
    AsyncRedisBackend,
    from_spec,
)

pytestmark = pytest.mark.redis

CONTRACT = pathlib.Path(__file__).resolve().parent.parent / "contract"
_DOC = json.loads((CONTRACT / "golden-vectors.json").read_text(encoding="utf-8"))
_RATE_SUITES = [s for s in _DOC["suites"] if s["primitive"] == "rateLimit"]

# See tests/test_redis_backend.py: the smallest window-aligned offset that clears the now=0 sentinel and
# keeps the fractional-rate GCRA cold-path arithmetic float-exact.
BASE = 1000


def _redis_url() -> str:
    return os.environ.get("THROTTLEKIT_REDIS_URL", "redis://localhost:6380")


@pytest.fixture(scope="module")
def url() -> str:
    # Reachability is checked with the *sync* client so an unreachable Redis skips cleanly (no event loop).
    conn = redis.Redis.from_url(_redis_url())
    try:
        conn.ping()
    except Exception as exc:  # redis.exceptions.ConnectionError and friends
        pytest.skip(f"no Redis reachable at {_redis_url()} ({exc}); set THROTTLEKIT_REDIS_URL")
    finally:
        conn.close()
    return _redis_url()


@pytest.mark.parametrize("suite", _RATE_SUITES, ids=[s["name"] for s in _RATE_SUITES])
def test_vector_suite_is_bit_identical_to_oracle(url: str, suite: dict[str, Any]) -> None:
    async def go() -> None:
        client = aioredis.Redis.from_url(url)
        try:
            strategy = from_spec(suite["strategy"]["kind"], suite["strategy"]["options"])
            prefix = f"tkpy-atest:{suite['name']}"
            key = suite["key"]
            full_key = f"{prefix}:{key}"
            await client.delete(full_key)  # cold state
            backend = AsyncRedisBackend(client, strategy, prefix=prefix)
            try:
                for i, op in enumerate(suite["ops"]):
                    decision = await backend.check(key, op["cost"], now=BASE + op["now"])
                    expect = op["expect"]
                    where = f"{suite['name']} op[{i}] now={op['now']} cost={op['cost']}"
                    assert decision.allowed == expect["allowed"], where
                    assert decision.limit == expect["limit"], where
                    assert decision.remaining == expect["remaining"], where
                    assert decision.retry_after_ms == expect["retryAfterMs"], where
                    # resetAt is absolute epoch-ms → shifts rigidly with the clock offset.
                    assert decision.reset_at == expect["resetAt"] + BASE, where
            finally:
                await client.delete(full_key)
        finally:
            await client.aclose()

    asyncio.run(go())


def test_all_five_vectored_strategies_are_exercised() -> None:
    """Guard: the async replay covers every strategy that ships a wire script (no silent gap)."""
    covered = {s["strategy"]["kind"] for s in _RATE_SUITES}
    assert covered == {"gcra", "tokenBucket", "fixedWindow", "slidingWindow", "slidingWindowLog"}


def test_noscript_fallback_recaches_and_succeeds(url: str) -> None:
    """After SCRIPT FLUSH, EVALSHA raises NOSCRIPT and the backend must fall back to EVAL (then succeed)."""

    async def go() -> None:
        client = aioredis.Redis.from_url(url)
        try:
            await client.script_flush()  # empty the cache so the first EVALSHA misses
            backend = AsyncRedisBackend(
                client,
                from_spec("gcra", {"limit": 5, "periodMs": 1000, "burst": 5}),
                prefix="tkpy-atest:noscript",
            )
            await client.delete("tkpy-atest:noscript:k")
            decision = await backend.check("k", 1, now=BASE)  # EVALSHA → NOSCRIPT → EVAL
            assert decision.allowed
            await client.delete("tkpy-atest:noscript:k")
        finally:
            await client.aclose()

    asyncio.run(go())
