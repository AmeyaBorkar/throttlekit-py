"""The asyncio ``AsyncServiceBackend`` — the :mod:`grpc.aio` twin of :class:`~throttlekit.ServiceBackend`.

It is a line-for-line mirror of the synchronous client: same RPCs, same reply decoding, same error
mapping — every method is ``async`` and awaits the call on a ``grpc.aio`` channel. No rate-limiting math
lives here either; the Node core (in the service) computes every decision and this client transports it,
so an ``await rl.check(...)`` from an async web handler reaches the *same* oracle a Node fleet does.

    from throttlekit import AsyncServiceBackend
    async with AsyncServiceBackend("localhost:50051") as rl:
        d = await rl.check("api", api_key)
        if not d.allowed:
            ...  # 429; retry after d.retry_after_ms

The pure reply decoders and the gRPC→exception mapping are shared with the sync backend (a
``grpc.aio.AioRpcError`` is a ``grpc.RpcError``, so the same mapping applies); only the transport differs.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Sequence
from types import TracebackType

import grpc
import grpc.aio  # opt-in submodule: `import grpc` alone does not bind grpc.aio

# Reuse the sync backend's pure helpers — decoding a proto reply and mapping a gRPC status to a
# ThrottleKitError are transport-independent (AioRpcError subclasses RpcError, exposing code()/details()).
from .decision import Decision, Forecast
from .service_backend import _decision, _forecast, _mapped

try:
    from ._generated import throttlekit_pb2 as pb
    from ._generated import throttlekit_pb2_grpc as pb_grpc
except ImportError as exc:  # pragma: no cover - exercised only when stubs are absent
    raise ImportError(
        "ThrottleKit gRPC stubs are not generated. Run `python scripts/gen_proto.py` "
        "(after `pip install -e .[dev]`) to generate them from the vendored contract."
    ) from exc


class AsyncAdmission:
    """The async twin of :class:`~throttlekit.service_backend.Admission` — a held (or denied) concurrency
    / unified admission, used as an **async** context manager so the slot is always returned::

        async with backend.admit("checkout", user_id) as adm:
            if not adm.allowed:
                return 429            # adm.binding_axis names the axis that denied
            await do_work()           # raising inside ⇒ release(dropped=True)

    A denied admission holds no slot (``held`` is False) and releasing it is a no-op. ``release`` is
    idempotent. For a hold longer than the server's lease TTL, pass ``heartbeat=True`` to ``admit`` so a
    background task renews it; if a beat is missed the server reclaims the slot and :attr:`reclaimed`
    becomes True.
    """

    def __init__(
        self,
        backend: AsyncServiceBackend,
        decision: Decision,
        lease_id: str,
        lease_expires_at: int,
        binding_axis: str,
        policy_denied: bool,
        *,
        heartbeat: bool,
    ) -> None:
        self.decision = decision
        self.lease_id = lease_id
        self.lease_expires_at = lease_expires_at
        self.binding_axis = binding_axis
        self.policy_denied = policy_denied
        self._backend = backend
        self._heartbeat = heartbeat
        self._released = False
        self._reclaimed = False

    @property
    def allowed(self) -> bool:
        """Whether the work may proceed (the combined decision across the policy's axes)."""
        return self.decision.allowed

    @property
    def held(self) -> bool:
        """True iff this admission holds a server-side slot that must be released."""
        return bool(self.lease_id)

    @property
    def reclaimed(self) -> bool:
        """True iff the server reclaimed this lease (a missed heartbeat) — treat the work as dropped."""
        return self._reclaimed

    async def release(self, *, dropped: bool = False) -> None:
        """Return the held slot. Idempotent, and a no-op for a denied admission (no slot is held).

        Pass ``dropped=True`` for a request that failed or timed out — the adaptive concurrency limit
        contracts on a drop. Release is best-effort: if the call fails the server still reclaims the slot
        once the lease TTL lapses.
        """
        if self._released:
            return
        self._released = True
        if self.lease_id:
            await self._backend._release_lease(self.lease_id, dropped, heartbeat=self._heartbeat)

    def _mark_reclaimed(self) -> None:
        self._reclaimed = True
        self._released = True  # the server already freed the slot; nothing left to release

    async def __aenter__(self) -> AsyncAdmission:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        # Return None (not False): release the slot but never suppress the block's own exception.
        await self.release(dropped=exc_type is not None)


class _AsyncHeartbeatPump:
    """An ``asyncio.Task`` that renews every open (opt-in) lease in one batched ``Heartbeat``.

    The async analogue of the sync :class:`~throttlekit.service_backend._HeartbeatPump`: no lock is
    needed (the event loop never preempts a coroutine between awaits — we only snapshot ``_open`` across
    the one ``await``). Short holds need no heartbeat; this exists only for long-lived holds.
    """

    def __init__(self, stub: pb_grpc.RateLimiterStub, interval_s: float) -> None:
        self._stub = stub
        self._interval = interval_s
        self._open: dict[str, AsyncAdmission] = {}
        self._task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()

    def register(self, adm: AsyncAdmission) -> None:
        self._open[adm.lease_id] = adm
        if self._task is None:
            # Lazily started inside the running loop (register is only reached from `await admit`).
            self._task = asyncio.ensure_future(self._run())

    def deregister(self, lease_id: str) -> None:
        self._open.pop(lease_id, None)

    async def _run(self) -> None:
        while True:
            try:
                # Wakes early (returns) when stopped; otherwise times out and beats every interval.
                await asyncio.wait_for(self._stop.wait(), timeout=self._interval)
                return
            except (TimeoutError, asyncio.TimeoutError):
                pass
            ids = list(self._open)
            if not ids:
                continue
            try:
                resp = await self._stub.Heartbeat(pb.HeartbeatRequest(lease_ids=ids))
                for rid in resp.reclaimed_ids:
                    adm = self._open.pop(rid, None)
                    if adm is not None:
                        adm._mark_reclaimed()
            except Exception:
                # Best-effort: a transient RPC error (or any unexpected one) must not kill the pump —
                # the next beat retries while the lease TTL holds, and the server reclaims on expiry.
                # CancelledError is a BaseException, so aclose()'s cancel still tears the task down.
                continue

    async def close(self) -> None:
        self._stop.set()
        task = self._task
        self._task = None
        if task is None:
            return
        # Cancel + await so the task is fully torn down BEFORE the caller closes the channel (avoids a
        # "task destroyed but pending" warning and a beat racing a closing channel). A normal-finished
        # task makes cancel a no-op; our cancellation surfaces as CancelledError, which we suppress.
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


class AsyncServiceBackend:
    """An asyncio client for a running ``throttlekit-server`` (the :mod:`grpc.aio` twin of
    :class:`~throttlekit.ServiceBackend`).

    :param target: ``host:port`` of the service (default ``localhost:50051``).
    :param credentials: gRPC channel credentials for TLS/mTLS; ``None`` uses an **insecure** channel
        (loopback/dev only — front anything exposed with mTLS).
    :param heartbeat_interval: seconds between batched heartbeats for ``admit(..., heartbeat=True)`` leases
        (default 1.0 — the core node↔coordinator cadence; the server's lease TTL is twice this).

    Construct and use an instance within a **single event loop** — a ``grpc.aio`` channel binds to the
    loop it is first used on (the standard one-client-per-loop pattern; share one per process/loop).
    """

    def __init__(
        self,
        target: str = "localhost:50051",
        *,
        credentials: grpc.ChannelCredentials | None = None,
        heartbeat_interval: float = 1.0,
    ) -> None:
        self._channel = (
            grpc.aio.secure_channel(target, credentials)
            if credentials is not None
            else grpc.aio.insecure_channel(target)
        )
        self._stub = pb_grpc.RateLimiterStub(self._channel)
        self._heartbeat_interval = heartbeat_interval
        self._pump: _AsyncHeartbeatPump | None = None

    async def check(self, policy: str, key: str, cost: int = 1) -> Decision:
        """Consume ``cost`` units against ``policy`` for ``key``; the returned decision is authoritative."""
        try:
            resp = await self._stub.Check(pb.CheckRequest(policy=policy, key=key, cost=cost))
        except grpc.RpcError as err:
            raise _mapped(err) from err
        return _decision(resp.decision)

    async def check_many(self, policy: str, keys: Sequence[str], cost: int = 1) -> list[Decision]:
        """Consume ``cost`` units against ``policy`` for many keys at one instant; one decision per key."""
        try:
            resp = await self._stub.CheckMany(
                pb.CheckManyRequest(policy=policy, keys=list(keys), cost=cost)
            )
        except grpc.RpcError as err:
            raise _mapped(err) from err
        return [_decision(d) for d in resp.decisions]

    async def peek(self, policy: str, key: str) -> Decision:
        """Non-consuming peek for ``key`` under ``policy``."""
        try:
            resp = await self._stub.Peek(pb.PeekRequest(policy=policy, key=key))
        except grpc.RpcError as err:
            raise _mapped(err) from err
        return _decision(resp.decision)

    async def forecast(self, policy: str, key: str, cost: int = 1) -> Forecast:
        """Non-consuming capacity forecast for ``key`` under ``policy``."""
        try:
            resp = await self._stub.Forecast(pb.ForecastRequest(policy=policy, key=key, cost=cost))
        except grpc.RpcError as err:
            raise _mapped(err) from err
        return _forecast(resp.forecast)

    async def debit(self, policy: str, key: str, tokens: int = 1) -> Decision:
        """Debit ``tokens`` of post-hoc cost against a token-budget ``policy`` for ``key``.

        For the LLM-gateway problem: debit the actual tokens a stream produces as they are produced. A
        debit is admitted while budget remains; the crossing debit is counted in full and later debits in
        the window are refused (``allowed == False``). ``policy`` must be a token-budget meter, not a rate
        limiter (else :class:`OperationNotSupportedError`).
        """
        try:
            resp = await self._stub.Debit(pb.DebitRequest(policy=policy, key=key, tokens=tokens))
        except grpc.RpcError as err:
            raise _mapped(err) from err
        return _decision(resp.decision)

    async def admit(
        self,
        policy: str,
        key: str,
        cost: int = 1,
        *,
        hold: int = 0,
        value: int = 1,
        heartbeat: bool = False,
    ) -> AsyncAdmission:
        """Admit one unit of work against a concurrency / unified ``policy`` (the GALE concurrency axis).

        Returns an :class:`AsyncAdmission` async context manager. When admitted against a policy with a
        concurrency axis it **holds a slot** that must be returned (``await adm.release()`` or the
        ``async with`` block); the server reclaims it on lease expiry if the client crashes.
        ``hold`` / ``value`` are the (experimental) joint-LP terms. Pass ``heartbeat=True`` for a hold
        longer than the server's lease TTL. ``policy`` must be a concurrency / unified admitter, not a
        rate limiter / meter (else :class:`OperationNotSupportedError`).
        """
        try:
            resp = await self._stub.Admit(
                pb.AdmitRequest(policy=policy, key=key, cost=cost, hold=hold, value=value)
            )
        except grpc.RpcError as err:
            raise _mapped(err) from err
        adm = AsyncAdmission(
            self,
            _decision(resp.decision),
            resp.lease_id,
            resp.lease_expires_at,
            resp.binding_axis,
            resp.policy_denied,
            heartbeat=heartbeat,
        )
        if heartbeat and adm.lease_id:
            self._ensure_pump().register(adm)
        return adm

    def _ensure_pump(self) -> _AsyncHeartbeatPump:
        if self._pump is None:
            self._pump = _AsyncHeartbeatPump(self._stub, self._heartbeat_interval)
        return self._pump

    async def _release_lease(self, lease_id: str, dropped: bool, *, heartbeat: bool) -> None:
        if heartbeat and self._pump is not None:
            self._pump.deregister(lease_id)
        try:
            await self._stub.Release(pb.ReleaseRequest(lease_id=lease_id, dropped=dropped))
        except grpc.RpcError:
            # Best-effort: a lost Release is backstopped by the server reclaiming the slot on lease expiry.
            pass

    async def aclose(self) -> None:
        """Stop the heartbeat task (if any) and close the underlying channel."""
        if self._pump is not None:
            await self._pump.close()
        await self._channel.close()

    async def __aenter__(self) -> AsyncServiceBackend:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.aclose()
