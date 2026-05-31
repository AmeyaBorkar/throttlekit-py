"""The gRPC ``ServiceBackend`` — a thin client for the ThrottleKit service door.

It speaks ``throttlekit.v1.RateLimiter`` and decodes the reply into :class:`Decision` / :class:`Forecast`.
No rate-limiting math lives here: the Node core (running in the service) computes every decision, and
the golden vectors prove this client transports them faithfully. A *denial* is a normal ``Decision``
(``allowed == False``), never an error; gRPC errors map to the operational exceptions in
:mod:`throttlekit.errors`.
"""

from __future__ import annotations

import threading
from collections.abc import Sequence
from types import TracebackType

import grpc

from .decision import Decision, Forecast
from .errors import (
    OperationNotSupportedError,
    PolicyNotFoundError,
    ServiceUnavailableError,
    ThrottleKitError,
)

try:
    from ._generated import throttlekit_pb2 as pb
    from ._generated import throttlekit_pb2_grpc as pb_grpc
except ImportError as exc:  # pragma: no cover - exercised only when stubs are absent
    raise ImportError(
        "ThrottleKit gRPC stubs are not generated. Run `python scripts/gen_proto.py` "
        "(after `pip install -e .[dev]`) to generate them from the vendored contract."
    ) from exc


def _decision(msg: pb.Decision) -> Decision:
    return Decision(
        allowed=msg.allowed,
        limit=msg.limit,
        remaining=msg.remaining,
        reset_at=msg.reset_at,
        retry_after_ms=msg.retry_after_ms,
    )


def _forecast(msg: pb.Forecast) -> Forecast:
    return Forecast(
        spendable_now=msg.spendable_now,
        next_replenish_at=msg.next_replenish_at,
        full_at=msg.full_at,
    )


def _mapped(err: grpc.RpcError) -> ThrottleKitError:
    code = err.code()
    details = err.details() or ""
    if code == grpc.StatusCode.NOT_FOUND:
        return PolicyNotFoundError(details)
    if code == grpc.StatusCode.UNIMPLEMENTED:
        return OperationNotSupportedError(details)
    if code == grpc.StatusCode.UNAVAILABLE:
        return ServiceUnavailableError(details)
    return ThrottleKitError(f"{code.name}: {details}")


class Admission:
    """A held (or denied) admission returned by :meth:`ServiceBackend.admit` — the concurrency / unified
    axis lifecycle handle.

    Use it as a context manager so the held slot is always returned::

        with backend.admit("checkout", user_id) as adm:
            if not adm.allowed:
                return 429            # adm.binding_axis names the axis that denied
            do_work()                # raising inside ⇒ release(dropped=True)

    A denied admission holds no slot (``held`` is False) and releasing it is a no-op. Releasing is
    idempotent. For a hold longer than the server's lease TTL, pass ``heartbeat=True`` to ``admit`` so a
    background thread renews it; if a beat is missed the server reclaims the slot and :attr:`reclaimed`
    becomes True.
    """

    def __init__(
        self,
        backend: ServiceBackend,
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

    def release(self, *, dropped: bool = False) -> None:
        """Return the held slot. Idempotent, and a no-op for a denied admission (no slot is held).

        Pass ``dropped=True`` for a request that failed or timed out — the adaptive concurrency limit
        contracts on a drop. Release is best-effort: if the call fails the server still reclaims the slot
        once the lease TTL lapses.
        """
        if self._released:
            return
        self._released = True
        if self.lease_id:
            self._backend._release_lease(self.lease_id, dropped, heartbeat=self._heartbeat)

    def _mark_reclaimed(self) -> None:
        self._reclaimed = True
        self._released = True  # the server already freed the slot; nothing left to release

    def __enter__(self) -> Admission:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        # Return None (not False): release the slot but never suppress the block's own exception.
        self.release(dropped=exc_type is not None)


class _HeartbeatPump:
    """A lazily-started daemon thread that renews every open (opt-in) lease in one batched ``Heartbeat``.

    Short holds (under the server's lease TTL) need no heartbeat; this exists only for long-lived holds.
    A lease the server reports as ``reclaimed`` is marked on its :class:`Admission` and dropped.
    """

    def __init__(self, stub: pb_grpc.RateLimiterStub, interval_s: float) -> None:
        self._stub = stub
        self._interval = interval_s
        self._lock = threading.Lock()
        self._open: dict[str, Admission] = {}
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()

    def register(self, adm: Admission) -> None:
        with self._lock:
            self._open[adm.lease_id] = adm
            if self._thread is None:
                self._thread = threading.Thread(
                    target=self._run, name="throttlekit-heartbeat", daemon=True
                )
                self._thread.start()

    def deregister(self, lease_id: str) -> None:
        with self._lock:
            self._open.pop(lease_id, None)

    def _run(self) -> None:
        # `Event.wait` returns True when stopped, False on timeout — so this beats every `interval_s`.
        while not self._stop.wait(self._interval):
            with self._lock:
                ids = list(self._open)
            if not ids:
                continue
            try:
                resp = self._stub.Heartbeat(pb.HeartbeatRequest(lease_ids=ids))
            except grpc.RpcError:
                continue  # transient; the next beat retries while the lease TTL still holds
            if resp.reclaimed_ids:
                with self._lock:
                    for rid in resp.reclaimed_ids:
                        adm = self._open.pop(rid, None)
                        if adm is not None:
                            adm._mark_reclaimed()

    def close(self) -> None:
        self._stop.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout=self._interval + 1.0)


class ServiceBackend:
    """A client for a running ``throttlekit-server``.

    :param target: ``host:port`` of the service (default ``localhost:50051``).
    :param credentials: gRPC channel credentials for TLS/mTLS; ``None`` uses an **insecure** channel
        (loopback/dev only — front anything exposed with mTLS).
    :param heartbeat_interval: seconds between batched heartbeats for ``admit(..., heartbeat=True)`` leases
        (default 1.0 — the core node↔coordinator cadence; the server's lease TTL is twice this).
    """

    def __init__(
        self,
        target: str = "localhost:50051",
        *,
        credentials: grpc.ChannelCredentials | None = None,
        heartbeat_interval: float = 1.0,
    ) -> None:
        self._channel = (
            grpc.secure_channel(target, credentials)
            if credentials is not None
            else grpc.insecure_channel(target)
        )
        self._stub = pb_grpc.RateLimiterStub(self._channel)
        self._heartbeat_interval = heartbeat_interval
        self._pump: _HeartbeatPump | None = None

    def check(self, policy: str, key: str, cost: int = 1) -> Decision:
        """Consume ``cost`` units against ``policy`` for ``key``; the returned decision is authoritative."""
        try:
            resp = self._stub.Check(pb.CheckRequest(policy=policy, key=key, cost=cost))
        except grpc.RpcError as err:
            raise _mapped(err) from err
        return _decision(resp.decision)

    def check_many(self, policy: str, keys: Sequence[str], cost: int = 1) -> list[Decision]:
        """Consume ``cost`` units against ``policy`` for many keys at one instant; one decision per key."""
        try:
            resp = self._stub.CheckMany(
                pb.CheckManyRequest(policy=policy, keys=list(keys), cost=cost)
            )
        except grpc.RpcError as err:
            raise _mapped(err) from err
        return [_decision(d) for d in resp.decisions]

    def peek(self, policy: str, key: str) -> Decision:
        """Non-consuming peek for ``key`` under ``policy``."""
        try:
            resp = self._stub.Peek(pb.PeekRequest(policy=policy, key=key))
        except grpc.RpcError as err:
            raise _mapped(err) from err
        return _decision(resp.decision)

    def forecast(self, policy: str, key: str, cost: int = 1) -> Forecast:
        """Non-consuming capacity forecast for ``key`` under ``policy``."""
        try:
            resp = self._stub.Forecast(pb.ForecastRequest(policy=policy, key=key, cost=cost))
        except grpc.RpcError as err:
            raise _mapped(err) from err
        return _forecast(resp.forecast)

    def debit(self, policy: str, key: str, tokens: int = 1) -> Decision:
        """Debit ``tokens`` of post-hoc cost against a token-budget ``policy`` for ``key``.

        For the LLM-gateway problem: debit the actual tokens a stream produces as they are produced. A
        debit is admitted while budget remains; the crossing debit is counted in full and later debits in
        the window are refused (``allowed == False``). ``policy`` must be a token-budget meter, not a rate
        limiter (else :class:`OperationNotSupportedError`).
        """
        try:
            resp = self._stub.Debit(pb.DebitRequest(policy=policy, key=key, tokens=tokens))
        except grpc.RpcError as err:
            raise _mapped(err) from err
        return _decision(resp.decision)

    def admit(
        self,
        policy: str,
        key: str,
        cost: int = 1,
        *,
        hold: int = 0,
        value: int = 1,
        heartbeat: bool = False,
    ) -> Admission:
        """Admit one unit of work against a concurrency / unified ``policy`` (the GALE concurrency axis).

        Returns an :class:`Admission` context manager. When admitted against a policy with a concurrency
        axis it **holds a slot** that must be returned (``adm.release()`` or the ``with`` block); the
        server reclaims it on lease expiry if the client crashes. ``hold`` / ``value`` are the
        (experimental) joint-LP terms. Pass ``heartbeat=True`` for a hold longer than the server's lease
        TTL. ``policy`` must be a concurrency / unified admitter, not a rate limiter / meter (else
        :class:`OperationNotSupportedError`).
        """
        try:
            resp = self._stub.Admit(
                pb.AdmitRequest(policy=policy, key=key, cost=cost, hold=hold, value=value)
            )
        except grpc.RpcError as err:
            raise _mapped(err) from err
        adm = Admission(
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

    def _ensure_pump(self) -> _HeartbeatPump:
        if self._pump is None:
            self._pump = _HeartbeatPump(self._stub, self._heartbeat_interval)
        return self._pump

    def _release_lease(self, lease_id: str, dropped: bool, *, heartbeat: bool) -> None:
        if heartbeat and self._pump is not None:
            self._pump.deregister(lease_id)
        try:
            self._stub.Release(pb.ReleaseRequest(lease_id=lease_id, dropped=dropped))
        except grpc.RpcError:
            # Best-effort: a lost Release is backstopped by the server reclaiming the slot on lease expiry.
            # (Swallowing also keeps `with admit(...)` from masking the block's own exception on exit.)
            pass

    def close(self) -> None:
        """Stop the heartbeat thread (if any) and close the underlying channel."""
        if self._pump is not None:
            self._pump.close()
        self._channel.close()

    def __enter__(self) -> ServiceBackend:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()
