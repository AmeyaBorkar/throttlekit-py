"""``FleetBackend`` / ``LeasedLimiter`` — request building, Lease decoding, the local-spend loop, auth.

Driven against a fake stub (no server) so the transport mapping + the spend/refresh control flow are pinned
deterministically; the lease *arithmetic* is proven separately by the golden vectors (test_lease_spender).
"""

from __future__ import annotations

import asyncio

import grpc
import pytest

from throttlekit import Decision, PolicyNotFoundError
from throttlekit._grpc import pb
from throttlekit.fleet_backend import FleetBackend, Lease, _now_ms
from throttlekit.fleet_backend_async import AsyncFleetBackend


class _RpcError(grpc.RpcError):
    def __init__(self, code: grpc.StatusCode, details: str) -> None:
        self._code = code
        self._details = details

    def code(self) -> grpc.StatusCode:
        return self._code

    def details(self) -> str:
        return self._details


def _lease_msg(**kw: int) -> object:
    base = {
        "capacity": 0,
        "expiry_ms": 0,
        "refresh_interval_ms": 0,
        "safe_capacity": 0,
        "retry_after_ms": 0,
        "limit": 0,
    }
    base.update(kw)
    return pb.Lease(**base)


class _FakeFleetStub:
    def __init__(
        self, *, leases: list[object] | None = None, error: Exception | None = None
    ) -> None:
        self._leases = list(leases or [])
        self._error = error
        self.calls: list[tuple[object, object]] = []

    def Reserve(self, req: object, metadata: object = None) -> object:  # noqa: N802 (gRPC stub name)
        self.calls.append((req, metadata))
        if self._error is not None:
            raise self._error
        return pb.ReserveResponse(lease=self._leases.pop(0))


class _FakeAsyncFleetStub(_FakeFleetStub):
    async def Reserve(self, req: object, metadata: object = None) -> object:  # noqa: N802
        return super().Reserve(req, metadata)


def _fleet(stub: object, *, secret: str | None = None) -> FleetBackend:
    backend = FleetBackend("localhost:1", secret=secret)  # a lazy channel; the stub is overridden
    backend._stub = stub  # type: ignore[assignment]
    return backend


def test_reserve_builds_the_request_and_decodes_the_lease() -> None:
    stub = _FakeFleetStub(
        leases=[
            _lease_msg(
                capacity=3, expiry_ms=1000, refresh_interval_ms=1000, safe_capacity=3, limit=5
            )
        ]
    )
    backend = _fleet(stub)
    try:
        lease = backend.reserve("api", domain="acme", wants=3)
        assert lease == Lease(
            capacity=3,
            expiry_ms=1000,
            refresh_interval_ms=1000,
            safe_capacity=3,
            retry_after_ms=0,
            limit=5,
        )
        req, md = stub.calls[0]
        assert req.policy == "api"
        assert req.caller.domain == "acme"
        assert req.wants == 3
        assert req.axis == pb.AXIS_RATE
        assert md is None  # no secret ⇒ no metadata
    finally:
        backend.close()


def test_reserve_sends_the_fleet_secret_as_metadata() -> None:
    stub = _FakeFleetStub(leases=[_lease_msg(capacity=1, expiry_ms=1000, limit=5)])
    backend = _fleet(stub, secret="s3cret")
    try:
        backend.reserve("api")
        _, md = stub.calls[0]
        assert md == (("x-fleet-secret", "s3cret"),)
    finally:
        backend.close()


def test_reserve_maps_not_found_to_policy_error() -> None:
    backend = _fleet(_FakeFleetStub(error=_RpcError(grpc.StatusCode.NOT_FOUND, "no such policy")))
    try:
        with pytest.raises(PolicyNotFoundError):
            backend.reserve("ghost")
    finally:
        backend.close()


def test_leased_limiter_spends_locally_then_refreshes_then_surfaces_the_denial() -> None:
    stub = _FakeFleetStub(
        leases=[
            _lease_msg(
                capacity=2, expiry_ms=60_000, refresh_interval_ms=60_000, safe_capacity=2, limit=5
            ),
            _lease_msg(
                capacity=0, expiry_ms=60_000, retry_after_ms=60_000, limit=5
            ),  # budget spent
        ]
    )
    backend = _fleet(stub)
    try:
        limiter = backend.leased("api", domain="acme")
        d1 = limiter.check(now=0)
        assert d1 == Decision(allowed=True, limit=5, remaining=1, reset_at=60_000, retry_after_ms=0)
        d2 = limiter.check(now=0)
        assert d2.allowed and d2.remaining == 0
        d3 = limiter.check(now=0)  # local credits spent → refresh → cap 0 → server denial verbatim
        assert not d3.allowed
        assert d3.retry_after_ms == 60_000
        assert len(stub.calls) == 2  # one bootstrap lease, one on the shortfall — not one per check
    finally:
        backend.close()


def test_now_ms_is_a_positive_int() -> None:
    assert isinstance(_now_ms(), int)
    assert _now_ms() > 0


def test_async_reserve_decodes_lease_and_async_leased_loop() -> None:
    async def run() -> tuple[Lease, Decision, Decision]:
        stub = _FakeAsyncFleetStub(
            leases=[
                _lease_msg(
                    capacity=1,
                    expiry_ms=60_000,
                    refresh_interval_ms=60_000,
                    safe_capacity=1,
                    limit=9,
                ),
                _lease_msg(capacity=0, expiry_ms=60_000, retry_after_ms=42, limit=9),
            ]
        )
        backend = AsyncFleetBackend("localhost:1")
        backend._stub = stub  # type: ignore[assignment]
        try:
            limiter = backend.leased("api")
            first = await limiter.check(now=0)  # bootstrap reserve (cap 1) + local spend
            second = await limiter.check(now=0)  # spent → refresh → cap 0 → denial
            # also exercise reserve() directly via a fresh stub
            stub2 = _FakeAsyncFleetStub(leases=[_lease_msg(capacity=4, expiry_ms=1000, limit=10)])
            backend._stub = stub2  # type: ignore[assignment]
            lease = await backend.reserve("api", wants=4)
            return lease, first, second
        finally:
            await backend.close()

    lease, first, second = asyncio.run(run())
    assert lease.capacity == 4
    assert first.allowed and first.remaining == 0
    assert not second.allowed and second.retry_after_ms == 42


class _BudgetFleetStub:
    """A budget-aware fake: grants min(wants, remaining budget) per Reserve, like a real fixed-window server.

    The plain _FakeFleetStub returns queued leases IGNORING wants, which masks whether the client actually
    batches; this honours wants so a batch lever can be proven against it.
    """

    def __init__(self, *, budget: int, limit: int, expiry_ms: int = 60_000) -> None:
        self.budget = budget
        self.limit = limit
        self.expiry_ms = expiry_ms
        self.reserves = 0

    def Reserve(self, req: object, metadata: object = None) -> object:  # noqa: N802 (gRPC stub name)
        self.reserves += 1
        want = req.wants if getattr(req, "wants", 0) > 0 else 1  # type: ignore[attr-defined]
        grant = min(want, self.budget)
        self.budget -= grant
        return pb.ReserveResponse(
            lease=pb.Lease(
                capacity=grant,
                expiry_ms=self.expiry_ms,
                refresh_interval_ms=self.expiry_ms,
                safe_capacity=grant,
                retry_after_ms=self.expiry_ms if grant == 0 else 0,
                limit=self.limit,
            )
        )


def test_leased_limiter_amortizes_reserves_with_batch() -> None:
    stub = _BudgetFleetStub(budget=1000, limit=1000)
    backend = _fleet(stub)
    try:
        limiter = backend.leased("api", domain="acme", batch=100)
        for i in range(250):
            assert limiter.check(now=0).allowed, f"request {i} denied unexpectedly"
        # 250 requests served from three 100-unit leases (~83 req/trip) — NOT one round trip per request.
        assert stub.reserves == 3
    finally:
        backend.close()


def test_leased_limiter_default_batch_round_trips_per_request() -> None:
    stub = _BudgetFleetStub(budget=1000, limit=1000)
    backend = _fleet(stub)
    try:
        limiter = backend.leased(
            "api"
        )  # default batch=1 ⇒ honest per-request behaviour, no amortization
        for _ in range(5):
            assert limiter.check(now=0).allowed
        assert stub.reserves == 5
    finally:
        backend.close()


def test_leased_rejects_non_positive_batch() -> None:
    backend = _fleet(_FakeFleetStub())
    try:
        with pytest.raises(ValueError):
            backend.leased("api", batch=0)
    finally:
        backend.close()
