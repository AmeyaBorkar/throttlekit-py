"""Turn a :class:`~throttlekit.Decision` into standard rate-limit HTTP response headers.

Pure stdlib — no grpc, redis, or framework imports — so it is safe to import eagerly from the package
root and reuse from any adapter (the ``contrib`` integrations all stamp their 429s through it).

Two header styles, because the ecosystem never settled on one:

* ``"ietf"`` (the default) — the widely-deployed ``RateLimit-*`` triple from the IETF
  *draft-ietf-httpapi-ratelimit-headers* lineage. ``RateLimit-Reset`` is **delta-seconds remaining**
  (clock-skew-safe: the client needs no synchronized clock to use it).
* ``"legacy"`` — the GitHub / Twitter ``X-RateLimit-*`` triple, where ``X-RateLimit-Reset`` is an
  **absolute epoch-seconds** timestamp.
* ``"both"`` — emit both triples (their keys are disjoint) plus the single shared ``Retry-After``.

``Retry-After`` (RFC 9110 §10.2.3, the ``delay-seconds`` form) is emitted in every style, but only when
the decision actually advises a wait (``retry_after_ms > 0`` — i.e. a denial).

The draft-current *structured* ``RateLimit`` / ``RateLimit-Policy`` field form is intentionally **not**
emitted: its syntax is still churning and deployment is thin, so the discrete triple is the safe default.
"""

from __future__ import annotations

import math
import time
from typing import Literal

from .decision import Decision

Style = Literal["ietf", "legacy", "both"]


def decision_headers(
    decision: Decision,
    style: Style = "ietf",
    *,
    now_ms: int | None = None,
) -> dict[str, str]:
    """Return the rate-limit response headers for ``decision``.

    :param decision: the :class:`~throttlekit.Decision` to render (allowed or denied).
    :param style: ``"ietf"`` (default), ``"legacy"``, or ``"both"`` — see the module docstring.
    :param now_ms: epoch-ms "now" used only to compute the ietf ``RateLimit-Reset`` delta; defaults to
        the wall clock. Pass it for deterministic output (e.g. in tests).
    :returns: a fresh ``dict[str, str]`` of header name → value, ready to merge onto a response.
    """
    if style not in ("ietf", "legacy", "both"):
        raise ValueError(f"unknown header style: {style!r} (use 'ietf', 'legacy', or 'both')")

    now = now_ms if now_ms is not None else int(time.time() * 1000)
    remaining = max(0, decision.remaining)
    headers: dict[str, str] = {}

    if style in ("ietf", "both"):
        # Delta-seconds until full replenishment, rounded up (advertise ≥ the real wait), clamped to 0.
        reset_delta = max(0, math.ceil((decision.reset_at - now) / 1000))
        headers["RateLimit-Limit"] = str(decision.limit)
        headers["RateLimit-Remaining"] = str(remaining)
        headers["RateLimit-Reset"] = str(reset_delta)

    if style in ("legacy", "both"):
        # Absolute epoch-seconds (GitHub/Twitter convention), floored — independent of `now`.
        headers["X-RateLimit-Limit"] = str(decision.limit)
        headers["X-RateLimit-Remaining"] = str(remaining)
        headers["X-RateLimit-Reset"] = str(decision.reset_at // 1000)

    # Retry-After only when a wait is actually advised (a denial); ceil so the advertised wait ≥ real.
    if decision.retry_after_ms > 0:
        headers["Retry-After"] = str(math.ceil(decision.retry_after_ms / 1000))

    return headers
