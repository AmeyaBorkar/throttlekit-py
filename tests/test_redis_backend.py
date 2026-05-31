"""Rigorous cross-language conformance: Python → vendored Lua → real Redis  ≡  the Node oracle.

Unlike the service-door integration test (which, over a cross-process connection, can only assert
*clock-independent* behavior because the server uses wall-clock time), the direct ``RedisBackend``
puts an explicit ``now`` in ARGV — so it can replay the **full, time-parametrized** golden vectors and
match every reply field. This is the capstone proof of the polyglot design: a Python client running
the core's own Lua reproduces an embedded Node library bit-for-bit.

Skipped unless ``redis-py`` is installed and a Redis is reachable (``THROTTLEKIT_REDIS_URL`` or the
project default ``redis://localhost:6380``).

The clock offset (BASE)
-----------------------
The vectors were produced by the in-process Node oracle at ``now`` values starting at ``0``. But on the
wire ``ARGV[1] == 0`` is the **server-time sentinel** ("use the Redis ``TIME`` clock"), so a faithful
Lua replay must drive the clock with a non-zero offset. We add a single ``BASE`` to every op's ``now``.

``BASE = 1000`` is the *smallest* offset that works, and that is exactly why it is the right choice:

* It clears the sentinel (``BASE + 0 != 0``).
* It is a multiple of every window/bucket width in the suites (fixedWindow ``1000`` ms, slidingWindow
  bucket ``100`` ms), so each strategy's ``floor(now / w)`` window index shifts rigidly — the relative
  window arithmetic is unchanged.
* **Keeping the offset minimal keeps the reproduction float-exact.** GCRA's cold-path ``remaining``
  computes ``floor((tau - (new_tat - now)) / T)``, where ``new_tat - now`` should equal ``inc`` but, in
  IEEE-754, ``(now + inc) - now != inc`` once ``now`` is large enough to crowd ``inc``'s low mantissa
  bits — which flips a value sitting exactly on a floor boundary (the fractional-rate ``gcra/fractional-T``
  suite, ``T = 1000/3``). A large offset (e.g. ``1_000_000``) trips this; ``BASE = 1000`` stays well
  inside the exact range. (A real Node fleet running ``useServerTime`` at real epochs sees the same
  GCRA property — it is inherent, not a client defect; we simply replay at a clock where the committed
  vectors reproduce exactly.)

The decision math is otherwise shift-invariant (``allowed`` / ``remaining`` / ``retryAfterMs`` depend
only on time *differences*); ``resetAt`` is the one **absolute**-epoch field and shifts by exactly
``BASE`` (the literal meaning of an absolute timestamp under a clock offset). So we assert all five
reply fields against the oracle: four equal, ``resetAt == oracle.resetAt + BASE``. Empirically this is
bit-identical for every op of every suite. Only the ``rateLimit`` primitive has extracted Lua; the
``tokenBudget`` suites (no wire script) are covered by the Node in-process path and skipped here.
"""

from __future__ import annotations

import json
import os
import pathlib
from typing import Any

import pytest

redis = pytest.importorskip("redis")

from throttlekit import (  # noqa: E402  (intentionally after importorskip)
    RedisBackend,
    _contract,
    from_spec,
)

pytestmark = pytest.mark.redis

CONTRACT = pathlib.Path(__file__).resolve().parent.parent / "contract"
_DOC = json.loads((CONTRACT / "golden-vectors.json").read_text(encoding="utf-8"))
_RATE_SUITES = [s for s in _DOC["suites"] if s["primitive"] == "rateLimit"]

# See the module docstring: smallest window-aligned offset that clears the now=0 sentinel and keeps the
# fractional-rate GCRA cold-path arithmetic float-exact.
BASE = 1000


@pytest.fixture(scope="module")
def client() -> Any:
    url = os.environ.get("THROTTLEKIT_REDIS_URL", "redis://localhost:6380")
    conn = redis.Redis.from_url(url)
    try:
        conn.ping()
    except Exception as exc:  # redis.exceptions.ConnectionError and friends
        pytest.skip(f"no Redis reachable at {url} ({exc}); set THROTTLEKIT_REDIS_URL")
    yield conn
    conn.close()


@pytest.mark.parametrize("suite", _RATE_SUITES, ids=[s["name"] for s in _RATE_SUITES])
def test_vector_suite_is_bit_identical_to_oracle(client: Any, suite: dict[str, Any]) -> None:
    strategy = from_spec(suite["strategy"]["kind"], suite["strategy"]["options"])
    prefix = f"tkpy-test:{suite['name']}"
    key = suite["key"]
    full_key = f"{prefix}:{key}"
    client.delete(full_key)  # cold state: the suite accumulates from an empty key
    backend = RedisBackend(client, strategy, prefix=prefix)

    try:
        for i, op in enumerate(suite["ops"]):
            decision = backend.check(key, op["cost"], now=BASE + op["now"])
            expect = op["expect"]
            where = f"{suite['name']} op[{i}] now={op['now']} cost={op['cost']}"
            assert decision.allowed == expect["allowed"], where
            assert decision.limit == expect["limit"], where
            assert decision.remaining == expect["remaining"], where
            assert decision.retry_after_ms == expect["retryAfterMs"], where
            # resetAt is absolute epoch-ms → shifts rigidly with the clock offset.
            assert decision.reset_at == expect["resetAt"] + BASE, where
    finally:
        client.delete(full_key)


def test_all_five_vectored_strategies_are_exercised() -> None:
    """Guard: the replay covers every strategy that ships a wire script (no silent gap)."""
    covered = {s["strategy"]["kind"] for s in _RATE_SUITES}
    assert covered == {"gcra", "tokenBucket", "fixedWindow", "slidingWindow", "slidingWindowLog"}


def test_client_sha1_matches_the_redis_script_cache(client: Any) -> None:
    """The SHA-1 the client computes for EVALSHA is exactly the one Redis caches the script under.

    This is the cross-language cache-sharing property: a Node fleet's EVALSHA and this Python client's
    address the *same* cached script, because both compute sha1 over the identical script bytes.
    """
    script = _contract.script("gcra", "check")
    backend = RedisBackend(
        client, from_spec("gcra", {"limit": 5, "periodMs": 1000, "burst": 5}), prefix="tkpy-test:sha"
    )
    client.delete("tkpy-test:sha:k")
    backend.check("k", 1, now=BASE)  # populates the script cache via EVALSHA→EVAL fallback
    assert client.script_exists(script.sha1) == [True]
    client.delete("tkpy-test:sha:k")
