"""The gRPC ``ServiceBackend`` — a thin client for the ThrottleKit service door.

It speaks ``throttlekit.v1.RateLimiter`` and decodes the reply into :class:`Decision` / :class:`Forecast`.
No rate-limiting math lives here: the Node core (running in the service) computes every decision, and
the golden vectors prove this client transports them faithfully. A *denial* is a normal ``Decision``
(``allowed == False``), never an error; gRPC errors map to the operational exceptions in
:mod:`throttlekit.errors`.
"""

from __future__ import annotations

from collections.abc import Sequence

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


class ServiceBackend:
    """A client for a running ``throttlekit-server``.

    :param target: ``host:port`` of the service (default ``localhost:50051``).
    :param credentials: gRPC channel credentials for TLS/mTLS; ``None`` uses an **insecure** channel
        (loopback/dev only — front anything exposed with mTLS).
    """

    def __init__(
        self,
        target: str = "localhost:50051",
        *,
        credentials: grpc.ChannelCredentials | None = None,
    ) -> None:
        self._channel = (
            grpc.secure_channel(target, credentials)
            if credentials is not None
            else grpc.insecure_channel(target)
        )
        self._stub = pb_grpc.RateLimiterStub(self._channel)

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

    def close(self) -> None:
        """Close the underlying channel."""
        self._channel.close()

    def __enter__(self) -> ServiceBackend:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()
