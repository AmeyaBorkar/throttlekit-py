"""The asyncio twin of :mod:`throttlekit.fleet_backend` — ``AsyncFleetBackend`` over ``grpc.aio``.

Same surface as the sync client (``reserve`` + a local-spend :class:`AsyncLeasedLimiter`), awaited. The
lease spend is the same pure :class:`~throttlekit.LeaseSpender`, so the conformance proof covers both.
"""

from __future__ import annotations

from types import TracebackType

import grpc

from ._grpc import map_rpc_error, pb, pb_grpc
from .decision import Decision
from .errors import ThrottleKitError
from .fleet_backend import _AXIS, _MAX_ROUNDS, Lease, _lease, _now_ms
from .lease_spender import LeaseGrant, LeaseSpender


class AsyncFleetBackend:
    """An asyncio client for the ``Fleet`` lease door (see :class:`~throttlekit.FleetBackend`)."""

    def __init__(
        self,
        target: str = "localhost:50051",
        *,
        credentials: grpc.ChannelCredentials | None = None,
        secret: str | None = None,
    ) -> None:
        self._channel = (
            grpc.aio.secure_channel(target, credentials)
            if credentials is not None
            else grpc.aio.insecure_channel(target)
        )
        self._stub = pb_grpc.FleetStub(self._channel)
        self._metadata = (("x-fleet-secret", secret),) if secret else None

    async def reserve(
        self,
        policy: str,
        *,
        domain: str = "",
        wants: int = 1,
        has: int = 0,
        used: int = 0,
        axis: str = "rate",
    ) -> Lease:
        """Lease up to ``wants`` units of ``policy``'s global per-window budget for ``domain``."""
        req = pb.ReserveRequest(
            policy=policy,
            caller=pb.Caller(domain=domain),
            wants=wants,
            has=has,
            used=used,
            axis=_AXIS.get(axis, pb.AXIS_UNSPECIFIED),
        )
        try:
            resp = await self._stub.Reserve(req, metadata=self._metadata)
        except grpc.RpcError as err:
            raise map_rpc_error(err) from err
        return _lease(resp.lease)

    def leased(
        self, policy: str, *, domain: str = "", window_coupled: bool = True
    ) -> AsyncLeasedLimiter:
        """An :class:`AsyncLeasedLimiter` that leases ``policy`` (for ``domain``) and spends it locally."""
        return AsyncLeasedLimiter(self, policy, domain=domain, window_coupled=window_coupled)

    async def close(self) -> None:
        """Close the underlying channel."""
        await self._channel.close()

    async def __aenter__(self) -> AsyncFleetBackend:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close()


class AsyncLeasedLimiter:
    """Lease a chunk of a policy's budget and spend it locally, refreshing only on a shortfall (awaited)."""

    def __init__(
        self,
        backend: AsyncFleetBackend,
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

    async def check(self, cost: int = 1, *, now: int | None = None) -> Decision:
        """Serve one request of ``cost`` — locally if credits remain, else after a refresh round trip."""
        ts = _now_ms() if now is None else now
        for _ in range(_MAX_ROUNDS):
            if self._spender is not None:
                decision = self._spender.spend(ts, cost)
                if decision is not None:
                    return decision
            lease = await self._backend.reserve(self._policy, domain=self._domain, wants=cost)
            if self._spender is None:
                self._spender = LeaseSpender(
                    limit=lease.limit,
                    ttl_ms=max(1, lease.expiry_ms),
                    window_coupled=self._window_coupled,
                )
            if not lease.granted:
                return lease.denied_decision()
            self._spender.apply_lease(
                LeaseGrant(capacity=lease.capacity, expires_at=lease.expiry_ms)
            )
        raise ThrottleKitError(
            f"Fleet lease refresh exceeded {_MAX_ROUNDS} rounds for one request (cost={cost})"
        )


__all__ = ["AsyncFleetBackend", "AsyncLeasedLimiter"]
