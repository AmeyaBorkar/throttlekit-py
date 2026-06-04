"""Framework-native integrations for ThrottleKit — import only the one you use.

Each submodule depends on its framework (an *optional* extra), so nothing here is imported by
``import throttlekit``; ``import throttlekit.contrib.fastapi`` (etc.) is what pulls the framework in.
Install the matching extra: ``pip install "throttlekit-py[fastapi]"`` / ``[starlette]`` / ``[django]`` /
``[flask]``.

The shared, framework-agnostic helpers are re-exported here for convenience — every adapter turns a
backend into a :class:`~throttlekit.ratelimit.Checker` (via :func:`~throttlekit.bind_policy` for the
service door, or a ``RedisBackend.check`` bound method for the direct door) and renders denials with
:func:`~throttlekit.decision_headers`.
"""

from __future__ import annotations

from ..headers import decision_headers
from ..ratelimit import Checker, OnUnavailable, RateLimited, bind_policy, rate_limit

__all__ = [
    "decision_headers",
    "Checker",
    "OnUnavailable",
    "RateLimited",
    "bind_policy",
    "rate_limit",
]
