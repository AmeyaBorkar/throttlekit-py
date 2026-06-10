"""The gRPC ``FleetBackend`` — the Tier-2 client-held lease client (``throttlekit.v1.Fleet``).

``reserve`` leases a chunk of a federated policy's global per-window budget; :class:`LeasedLimiter` composes
it with a :class:`~throttlekit.LeaseSpender` so a high-throughput client serves locally and round-trips only
to **refresh**, not once per request. The **server is the one oracle** — it sizes every grant (a partial,
window-coupled ``Lease``); this client only spends it. A ``capacity`` of 0 is the server's denial, surfaced
verbatim — the client never invents one.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from types import TracebackType

import grpc

from ._grpc import map_rpc_error, pb, pb_grpc
from .decision import Decision
from .errors import ThrottleKitError
from .lease_spender import LeaseGrant, LeaseSpender

# Map a friendly axis name to the proto enum. v1 leases windowed-credit budgets; "concurrency" is accepted
# (the server answers UNIMPLEMENTED) so a caller can probe it without a magic number.
_AXIS = {
    "rate": pb.AXIS_RATE,
    "token_budget": pb.AXIS_TOKEN_BUDGET,
    "concurrency": pb.AXIS_CONCURRENCY,
}

# A defensive bound on refresh rounds for one request (mirrors the core's maxRounds) — a grant always makes
# progress within a window, so this only trips on a misbehaving server that neither grants nor denies.
_MAX_ROUNDS = 1024


@dataclass(frozen=True)
class Lease:
    """A granted lease: spend ``capacity`` locally until ``expiry_ms``, then re-lease."""

    capacity: int
    """GRANTED units (>= 0; may be < wants — a partial grant; 0 = refused)."""
    expiry_ms: int
    """Epoch-ms window boundary; the client discards leftover credits at this instant."""
    refresh_interval_ms: int
    """Hint: re-lease around here (the time remaining to ``expiry_ms``)."""
    safe_capacity: int
    """Capacity safe to spend under client clock uncertainty (v1: equals ``capacity``)."""
    retry_after_ms: int
    """When ``capacity == 0``: ms until the budget refreshes; else 0."""
    limit: int
    """The policy's global per-window ceiling (for the client's synthesized Decision)."""

    @property
    def granted(self) -> bool:
        """Whether any budget was granted (``capacity > 0``)."""
        return self.capacity > 0

    def denied_decision(self) -> Decision:
        """The server's authoritative denial when ``capacity == 0`` (surfaced verbatim — never synthesized)."""
        return Decision(
            allowed=False,
            limit=self.limit,
            remaining=0,
            reset_at=self.expiry_ms,
            retry_after_ms=self.retry_after_ms,
        )


def _lease(msg: pb.Lease) -> Lease:
    return Lease(
        capacity=msg.capacity,
        expiry_ms=msg.expiry_ms,
        refresh_interval_ms=msg.refresh_interval_ms,
        safe_capacity=msg.safe_capacity,
        retry_after_ms=msg.retry_after_ms,
        limit=msg.limit,
    )


def _now_ms() -> int:
    return time.time_ns() // 1_000_000


class FleetBackend:
    """A thin synchronous client for the ``Fleet`` lease door.

    Loopback-only on the server unless a fleet secret is configured; pass that secret here (sent as
    ``x-fleet-secret`` metadata) plus TLS ``credentials`` for a remote, exposed door.

        with FleetBackend("localhost:50051") as fleet:
            limiter = fleet.leased("global-api", domain="acme")
            d = limiter.check()                 # serves locally, refreshing only when the chunk is spent
            if not d.allowed: backoff(d.retry_after_ms)
    """

    def __init__(
        self,
        target: str = "localhost:50051",
        *,
        credentials: grpc.ChannelCredentials | None = None,
        secret: str | None = None,
    ) -> None:
        self._channel = (
            grpc.secure_channel(target, credentials)
            if credentials is not None
            else grpc.insecure_channel(target)
        )
        self._stub = pb_grpc.FleetStub(self._channel)
        self._metadata = (("x-fleet-secret", secret),) if secret else None

    def reserve(
        self,
        policy: str,
        *,
        domain: str = "",
        wants: int = 1,
        has: int = 0,
        used: int = 0,
        axis: str = "rate",
    ) -> Lease:
        """Lease up to ``wants`` units of ``policy``'s global per-window budget for ``domain``.

        ``domain`` selects which budget within the policy to lease (e.g. a tenant id); empty leases the
        policy as a whole. ``has`` / ``used`` are advisory client state. Raises ``PolicyNotFoundError`` for
        an unknown policy and ``OperationNotSupportedError`` for an unsupported axis.
        """
        req = pb.ReserveRequest(
            policy=policy,
            caller=pb.Caller(domain=domain),
            wants=wants,
            has=has,
            used=used,
            axis=_AXIS.get(axis, pb.AXIS_UNSPECIFIED),
        )
        try:
            resp = self._stub.Reserve(req, metadata=self._metadata)
        except grpc.RpcError as err:
            raise map_rpc_error(err) from err
        return _lease(resp.lease)

    def leased(
        self, policy: str, *, domain: str = "", window_coupled: bool = True
    ) -> LeasedLimiter:
        """A :class:`LeasedLimiter` that leases ``policy`` (for ``domain``) and spends it locally."""
        return LeasedLimiter(self, policy, domain=domain, window_coupled=window_coupled)

    def close(self) -> None:
        """Close the underlying channel."""
        self._channel.close()

    def __enter__(self) -> FleetBackend:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()


class LeasedLimiter:
    """Lease a chunk of a policy's budget and spend it locally, refreshing only on a shortfall.

    The ergonomic high-throughput client: :meth:`check` serves from local credits (no round trip) and only
    ``Fleet.Reserve``s when the chunk is spent — roughly one round trip per batch instead of one per request.
    The :class:`~throttlekit.LeaseSpender` is built from the first grant's ``limit`` + window, so the client
    needs no out-of-band policy config. One instance tracks one ``(policy, domain)`` budget.
    """

    def __init__(
        self,
        backend: FleetBackend,
        policy: str,
        *,
        domain: str = "",
        window_coupled: bool = True,
    ) -> None:
        self._backend = backend
        self._policy = policy
        self._domain = domain
        self._window_coupled = window_coupled
        self._spender: LeaseSpender | None = None

    def check(self, cost: int = 1, *, now: int | None = None) -> Decision:
        """Serve one request of ``cost`` (default 1) — locally if credits remain, else after a refresh.

        Returns a local allow, or the server's denial verbatim when the global budget is spent. ``now``
        (epoch-ms) is injectable for tests; it defaults to the wall clock.
        """
        ts = _now_ms() if now is None else now
        for _ in range(_MAX_ROUNDS):
            if self._spender is not None:
                decision = self._spender.spend(ts, cost)
                if decision is not None:
                    return decision
            lease = self._backend.reserve(self._policy, domain=self._domain, wants=cost)
            if self._spender is None:
                self._spender = LeaseSpender(
                    limit=lease.limit,
                    ttl_ms=max(1, lease.expiry_ms),
                    window_coupled=self._window_coupled,
                )
            if not lease.granted:
                return lease.denied_decision()  # server-authoritative denial, surfaced verbatim
            self._spender.apply_lease(
                LeaseGrant(capacity=lease.capacity, expires_at=lease.expiry_ms)
            )
        raise ThrottleKitError(
            f"Fleet lease refresh exceeded {_MAX_ROUNDS} rounds for one request (cost={cost})"
        )


__all__ = ["FleetBackend", "Lease", "LeasedLimiter"]
