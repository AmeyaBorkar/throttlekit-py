"""ThrottleKit — Python client for distributed rate limiting.

Two pluggable backends reach the **one** Node core, both proven against the same golden vectors:

* :class:`ServiceBackend` — gRPC to a ``throttlekit-server`` (the lead door; the core computes the
  decision, the client never touches the raw wire). ``import``-light: grpc is loaded lazily.
* :class:`RedisBackend` — runs the vendored Lua against the **same Redis** a Node fleet uses (the
  direct door; decisions are bit-identical to an embedded library). ``check`` only — by design.

    from throttlekit import ServiceBackend
    with ServiceBackend("localhost:50051") as rl:
        d = rl.check("api", api_key)
        if not d.allowed:
            ...  # 429; retry after d.retry_after_ms

    import redis
    from throttlekit import RedisBackend, Gcra
    api = RedisBackend(redis.Redis.from_url("redis://localhost:6379"),
                       Gcra(limit=100, period_ms=60_000, burst=20))
    d = api.check(api_key)
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from ._version import __version__
from .decision import Decision, Forecast
from .errors import (
    OperationNotSupportedError,
    PolicyNotFoundError,
    ServiceUnavailableError,
    ThrottleKitError,
)
from .headers import decision_headers
from .ratelimit import Checker, OnUnavailable, RateLimited, bind_policy, rate_limit
from .strategies import (
    FixedWindow,
    Gcra,
    SlidingWindow,
    SlidingWindowLog,
    Strategy,
    TokenBucket,
    from_spec,
)

if TYPE_CHECKING:
    from .redis_backend import RedisBackend, RedisClientLike
    from .service_backend import Admission, ServiceBackend

__all__ = [
    # Backends (lazily imported — neither grpc nor a redis client is needed to import throttlekit).
    "ServiceBackend",
    "Admission",
    "RedisBackend",
    "RedisClientLike",
    # Strategies for the direct RedisBackend.
    "Strategy",
    "Gcra",
    "TokenBucket",
    "FixedWindow",
    "SlidingWindow",
    "SlidingWindowLog",
    "from_spec",
    # Domain types + errors.
    "Decision",
    "Forecast",
    "ThrottleKitError",
    "PolicyNotFoundError",
    "OperationNotSupportedError",
    "ServiceUnavailableError",
    # Framework-agnostic ergonomics (dependency-light — eagerly importable).
    "decision_headers",
    "rate_limit",
    "RateLimited",
    "bind_policy",
    "Checker",
    "OnUnavailable",
    "__version__",
]

_LAZY = {
    "ServiceBackend": "service_backend",
    "Admission": "service_backend",
    "RedisBackend": "redis_backend",
    "RedisClientLike": "redis_backend",
}


def __getattr__(name: str) -> object:
    """Lazily import the backends so ``import throttlekit`` needs neither grpc nor a redis client."""
    module = _LAZY.get(name)
    if module is not None:
        import importlib

        return getattr(importlib.import_module(f".{module}", __name__), name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
