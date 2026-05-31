"""The direct ``RedisBackend`` — the second delivery door: vendored Lua, straight to Redis.

Where :class:`~throttlekit.ServiceBackend` reaches the Node core over gRPC, this backend talks to the
**same Redis** a Node fleet uses and runs the **same vendored Lua** the core ships — so its decisions
are bit-identical to an embedded Node library. That equivalence is *proven*, not asserted: the golden
vectors replay through real Redis in ``tests/test_redis_backend.py`` and every reply field must match
the Node oracle.

It re-implements **no** rate-limiting math: the decision is computed server-side, in Lua. Accordingly
it exposes ``check`` **only** — the contract-vectored, Lua-computed decision. ``peek`` / ``forecast`` /
``check_many`` deliberately route through the service door, where the core (not a re-derived port)
computes them; reproducing them here would mean porting the read→decision math client-side and so
re-deriving the decision in a second place, which the design forbids.

The backend is client-agnostic: pass any object with ``evalsha`` / ``eval`` (``redis-py`` satisfies it
structurally), exactly as the Node ``RedisStore`` accepts any ``RedisClientLike``.

    import redis
    from throttlekit import RedisBackend, Gcra

    client = redis.Redis.from_url("redis://localhost:6379")
    api = RedisBackend(client, Gcra(limit=100, period_ms=60_000, burst=20), prefix="prod")
    d = api.check(api_key)            # now defaults to the Redis server clock (skew-free)
    if not d.allowed:
        ...                          # 429; retry after d.retry_after_ms
"""

from __future__ import annotations

from typing import Protocol, cast, runtime_checkable

from . import _contract
from .decision import Decision
from .strategies import Strategy


@runtime_checkable
class RedisClientLike(Protocol):
    """The minimal Redis surface the backend needs. ``redis-py`` (and ioredis-shaped clients) match."""

    def evalsha(self, sha: str, numkeys: int, *keys_and_args: str | int) -> object: ...

    def eval(self, script: str, numkeys: int, *keys_and_args: str | int) -> object: ...


def _is_noscript(err: Exception) -> bool:
    # Redis returns NOSCRIPT when the script cache is empty (first EVALSHA, or after a restart/failover);
    # the client should then EVAL to re-cache it. Detected client-agnostically: redis-py raises
    # ``NoScriptError`` ("No matching script. Please use EVAL.", code folded into the class name),
    # while other clients may surface the raw ``NOSCRIPT …`` server error. Mirrors the Node store's
    # ``isNoScript``.
    blob = f"{type(err).__name__}: {err}".upper()
    return "NOSCRIPT" in blob or "NO MATCHING SCRIPT" in blob


def _decode(raw: object) -> Decision:
    # The reply tuple is five integers: [allowed, limit, remaining, resetAt, retryAfterMs].
    tup = cast("list[int]", raw)
    return Decision(
        allowed=tup[0] == 1,
        limit=tup[1],
        remaining=tup[2],
        reset_at=tup[3],
        retry_after_ms=tup[4],
    )


class RedisBackend:
    """Runs a strategy's vendored Lua against Redis; the returned :class:`Decision` is authoritative.

    :param client: any object exposing ``evalsha`` / ``eval`` (e.g. ``redis.Redis``).
    :param strategy: the configured :class:`~throttlekit.strategies.Strategy` (``Gcra`` / ``TokenBucket``
        / ``FixedWindow`` / ``SlidingWindow`` / ``SlidingWindowLog``).
    :param prefix: optional key namespace, joined as ``f"{prefix}:{key}"`` — the **same** scheme the
        core uses, so a Python and a Node client on one limit address the same Redis key.
    """

    def __init__(self, client: RedisClientLike, strategy: Strategy, *, prefix: str = "") -> None:
        self._client = client
        self._strategy = strategy
        self._prefix = prefix

    def check(self, key: str, cost: int = 1, *, now: int | None = None) -> Decision:
        """Consume ``cost`` units against ``key``.

        ``now`` is epoch-ms; ``None`` (the default) sends the sentinel ``0`` so the script derives time
        from the **Redis server clock** — the skew-free default a distributed fleet must use. Pass an
        explicit ``now`` only for deterministic replay against a known clock.
        """
        script = _contract.script(self._strategy.kind, "check")
        now_arg = 0 if now is None else now
        values: dict[str, int] = {"now": now_arg, "cost": cost, **self._strategy.params()}
        argv: list[int] = [values[name] for name in script.argv]
        full_key = f"{self._prefix}:{key}" if self._prefix else key
        return _decode(self._eval(script, [full_key], argv))

    def _eval(self, script: _contract.Script, keys: list[str], argv: list[int]) -> object:
        flat: list[str | int] = [*keys, *argv]
        try:
            return self._client.evalsha(script.sha1, len(keys), *flat)
        except Exception as err:
            if _is_noscript(err):
                # EVAL re-caches the script for next time, then runs it.
                return self._client.eval(script.source, len(keys), *flat)
            raise
