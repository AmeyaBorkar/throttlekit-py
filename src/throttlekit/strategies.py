"""Rate-limit strategies for the direct :class:`~throttlekit.RedisBackend`.

A strategy is **pure configuration**. It declares its wire ``kind`` and supplies the *named*
parameter values the script's ARGV references — it contains **no rate-limiting math**, because the
decision is computed entirely server-side in the vendored Lua (see :mod:`throttlekit.redis_backend`).

The ARGV *order* is read from the vendored ``_scripts/manifest.json`` at call time, never hard-coded
here, so a reordering in the core flows through on re-vendoring with no client change. Each
strategy's :meth:`params` therefore returns the **manifest ARGV names** (camelCase, e.g. ``periodMs``)
mapped to its configured values; the backend fills in ``now`` and ``cost`` and resolves the rest by
name. The Python constructors take idiomatic snake_case; only the dict keys mirror the wire.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import ClassVar, Protocol, runtime_checkable


@runtime_checkable
class Strategy(Protocol):
    """A configured strategy: its wire ``kind`` and the named ARGV parameters it contributes."""

    kind: ClassVar[str]

    def params(self) -> dict[str, int]:
        """Named ARGV values (besides ``now``/``cost``), keyed by their manifest ARGV name."""
        ...


@dataclass(frozen=True)
class Gcra:
    """GCRA: ``burst`` cells of headroom, draining one cell every ``period_ms / limit``."""

    kind: ClassVar[str] = "gcra"
    limit: int
    period_ms: int
    burst: int

    def params(self) -> dict[str, int]:
        return {"limit": self.limit, "periodMs": self.period_ms, "burst": self.burst}


@dataclass(frozen=True)
class TokenBucket:
    """Token bucket: a bucket of ``capacity`` tokens refilling at ``refill_per_sec`` tokens/second."""

    kind: ClassVar[str] = "tokenBucket"
    capacity: int
    refill_per_sec: int

    def params(self) -> dict[str, int]:
        return {"capacity": self.capacity, "refillPerSec": self.refill_per_sec}


@dataclass(frozen=True)
class FixedWindow:
    """Fixed window: at most ``limit`` units per epoch-aligned ``window_ms`` window."""

    kind: ClassVar[str] = "fixedWindow"
    limit: int
    window_ms: int

    def params(self) -> dict[str, int]:
        return {"limit": self.limit, "windowMs": self.window_ms}


@dataclass(frozen=True)
class SlidingWindow:
    """Sliding window: a ``buckets``-bucketed estimate of the trailing ``window_ms``, ceiling ``limit``."""

    kind: ClassVar[str] = "slidingWindow"
    limit: int
    window_ms: int
    buckets: int

    def params(self) -> dict[str, int]:
        return {"limit": self.limit, "windowMs": self.window_ms, "buckets": self.buckets}


@dataclass(frozen=True)
class SlidingWindowLog:
    """Sliding window log: exact count of accepted units in the trailing ``window_ms``, ceiling ``limit``."""

    kind: ClassVar[str] = "slidingWindowLog"
    limit: int
    window_ms: int

    def params(self) -> dict[str, int]:
        return {"limit": self.limit, "windowMs": self.window_ms}


def from_spec(kind: str, options: Mapping[str, int]) -> Strategy:
    """Build a strategy from a golden-vector ``strategy.{kind, options}`` spec (camelCase keys).

    This is the bridge the conformance harness uses to replay the language-neutral vectors through
    the Python client; it is also a convenient constructor from config.
    """
    if kind == "gcra":
        return Gcra(limit=options["limit"], period_ms=options["periodMs"], burst=options["burst"])
    if kind == "tokenBucket":
        return TokenBucket(capacity=options["capacity"], refill_per_sec=options["refillPerSec"])
    if kind == "fixedWindow":
        return FixedWindow(limit=options["limit"], window_ms=options["windowMs"])
    if kind == "slidingWindow":
        return SlidingWindow(
            limit=options["limit"], window_ms=options["windowMs"], buckets=options["buckets"]
        )
    if kind == "slidingWindowLog":
        return SlidingWindowLog(limit=options["limit"], window_ms=options["windowMs"])
    raise ValueError(f"unknown strategy kind: {kind!r}")
