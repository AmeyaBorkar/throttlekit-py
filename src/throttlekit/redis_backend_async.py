"""The asyncio ``AsyncRedisBackend`` — the :mod:`redis.asyncio` twin of
:class:`~throttlekit.RedisBackend`.

A line-for-line mirror of the synchronous direct door: it marshals ARGV from the vendored manifest,
runs the **same** vendored Lua against the **same** Redis a Node fleet uses, and decodes the reply — only
the calls are awaited. Decisions are therefore bit-identical to an embedded Node library and to the sync
backend; it re-implements no rate-limiting math (the decision is computed server-side, in Lua) and so
exposes ``check`` **only**, exactly like the sync door.

Like the sync backend it is **client-agnostic**: pass any object whose ``evalsha`` / ``eval`` are
awaitables (``redis.asyncio.Redis`` matches structurally), so importing ThrottleKit needs no redis client.

    import redis.asyncio as redis
    from throttlekit import AsyncRedisBackend, Gcra

    client = redis.Redis.from_url("redis://localhost:6379")
    api = AsyncRedisBackend(client, Gcra(limit=100, period_ms=60_000, burst=20), prefix="prod")
    d = await api.check(api_key)            # defaults to the Redis server clock (skew-free)
    if not d.allowed:
        ...                                # 429; retry after d.retry_after_ms
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from . import _contract
from .decision import Decision

# The reply decoder and the NOSCRIPT detector are transport-independent — reuse the sync backend's.
from .redis_backend import _decode, _is_noscript
from .strategies import Strategy


@runtime_checkable
class AsyncRedisClientLike(Protocol):
    """The minimal async Redis surface the backend needs. ``redis.asyncio.Redis`` matches structurally."""

    async def evalsha(self, sha: str, numkeys: int, *keys_and_args: str | int) -> object: ...

    async def eval(self, script: str, numkeys: int, *keys_and_args: str | int) -> object: ...


class AsyncRedisBackend:
    """Runs a strategy's vendored Lua against Redis over an async client; the :class:`Decision` is
    authoritative — the async twin of :class:`~throttlekit.RedisBackend`.

    :param client: any object whose ``evalsha`` / ``eval`` are awaitables (e.g. ``redis.asyncio.Redis``).
    :param strategy: the configured :class:`~throttlekit.strategies.Strategy` (``Gcra`` / ``TokenBucket``
        / ``FixedWindow`` / ``SlidingWindow`` / ``SlidingWindowLog``).
    :param prefix: optional key namespace, joined as ``f"{prefix}:{key}"`` — the **same** scheme the
        core uses, so a Python and a Node client on one limit address the same Redis key.
    """

    def __init__(
        self, client: AsyncRedisClientLike, strategy: Strategy, *, prefix: str = ""
    ) -> None:
        self._client = client
        self._strategy = strategy
        self._prefix = prefix

    async def check(self, key: str, cost: int = 1, *, now: int | None = None) -> Decision:
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
        return _decode(await self._eval(script, [full_key], argv))

    async def _eval(self, script: _contract.Script, keys: list[str], argv: list[int]) -> object:
        flat: list[str | int] = [*keys, *argv]
        try:
            return await self._client.evalsha(script.sha1, len(keys), *flat)
        except Exception as err:
            if _is_noscript(err):
                # EVAL re-caches the script for next time, then runs it.
                return await self._client.eval(script.source, len(keys), *flat)
            raise
