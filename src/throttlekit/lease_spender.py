"""``LeaseSpender`` — the Python port of the core's Tier-2 client-side lease spend.

A high-throughput client leases a chunk of a global budget from the service's ``Fleet.Reserve`` door and
spends it **locally**, round-tripping only to refresh — collapsing the per-request network hop to roughly
one per batch. This is a **verbatim port** of the Node core's ``twoTier(leased, windowCoupled)`` L1 path
(``applyLease`` + ``spend`` + the window-coupled discard): the **server is the one oracle** for the grant
*size*; this only subtracts from the granted balance and synthesizes an allow. It never invents a denial —
out of credits, :meth:`LeaseSpender.spend` returns ``None`` ("refresh needed") and the caller surfaces the
server's verdict from the next ``Reserve``. Pinned **byte-for-byte** against the core by the golden ``lease``
vectors (``tests/test_lease_spender.py``).
"""

from __future__ import annotations

import math
from dataclasses import dataclass

from .decision import Decision


@dataclass(frozen=True)
class LeaseGrant:
    """A grant the service returned: ``capacity`` units valid until the ``expires_at`` window boundary."""

    capacity: int
    """The GRANTED units (may be ``< wants`` — a partial grant is legitimate). Never the requested amount."""
    expires_at: int
    """Epoch-ms window boundary the grant is coupled to; invalid after this instant."""


def _require_cost(cost: float) -> None:
    if not math.isfinite(cost) or cost <= 0:
        raise ValueError(f"cost must be a positive finite number, got {cost!r}")


class LeaseSpender:
    """Spends a window-coupled lease locally; one instance tracks one key's credits + their window.

    Pure and synchronous — ``now`` (epoch-ms) is injected per call, like every core algorithm — so it is
    deterministic and portable. The refresh round trip (``Fleet.Reserve``) lives in the client (e.g.
    :class:`~throttlekit.LeasedLimiter`), not here, so this stays transport-free and conformance-checkable.
    """

    __slots__ = ("_credits", "_expires_at", "_limit", "_ttl_ms", "_window_coupled")

    def __init__(self, *, limit: int, ttl_ms: int = 0, window_coupled: bool = True) -> None:
        self._limit = limit
        self._ttl_ms = ttl_ms
        self._window_coupled = window_coupled
        self._credits = 0
        self._expires_at: int | None = None

    @property
    def credits(self) -> int:
        """Local leased credits currently available (the window-coupled discard applies on the next spend)."""
        return self._credits

    @property
    def expires_at(self) -> int | None:
        """Epoch-ms window boundary the current credits are coupled to, or ``None`` before the first grant."""
        return self._expires_at

    def apply_lease(self, grant: LeaseGrant) -> None:
        """Add a grant's ``capacity`` to local credits and couple them to its ``expires_at`` window.

        Mirrors the core leased path's ``credits += leaseAmount; lastDecision = d`` on an admitted lease.
        """
        self._credits += grant.capacity
        self._expires_at = grant.expires_at

    def spend(self, now: int, cost: int = 1) -> Decision | None:
        """Serve ``cost`` (default 1) from local credits at ``now``; return the allow, or ``None`` to refresh.

        A synthesized allow when credits suffice (byte-identical to the core L1 ``synthAllow``); otherwise
        ``None`` — the caller must ``Reserve`` more budget. Never synthesizes a denial; never performs I/O.
        """
        _require_cost(cost)
        if (
            self._window_coupled
            and self._expires_at is not None
            and now >= self._expires_at
            and self._credits > 0
        ):
            # The granting window has rolled — discard the remainder rather than carry it across (the sole
            # source of leased overshoot, removed).
            self._credits = 0
        if self._credits >= cost:
            self._credits -= cost
            reset_at = self._expires_at if self._expires_at is not None else now + self._ttl_ms
            return Decision(
                allowed=True,
                limit=self._limit,
                remaining=max(0, math.floor(self._credits)),
                reset_at=reset_at,
                retry_after_ms=0,
            )
        return None

    def reset(self) -> None:
        """Forget all local credits and the current window coupling (e.g. on a hard reset / reconnect)."""
        self._credits = 0
        self._expires_at = None


__all__ = ["LeaseGrant", "LeaseSpender"]
