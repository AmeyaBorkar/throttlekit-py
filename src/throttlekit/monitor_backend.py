"""The gRPC ``MonitorBackend`` — a read-only client for the ThrottleKit Monitor observability door.

It speaks ``throttlekit.v1.Monitor`` and projects the operational snapshot the ``--tui`` dashboard renders,
so a Python service can read a server's live state remotely. Strictly read-only — it never computes,
returns, or affects a rate-limit decision. The snapshot carries traffic keys (PII), so the server serves it
**loopback-only** unless a monitor secret is configured; pass that secret here (sent as ``x-monitor-secret``
metadata) plus TLS ``credentials`` for a remote door.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from types import TracebackType
from typing import Any

import grpc

from ._grpc import map_rpc_error, pb, pb_grpc


@dataclass(frozen=True)
class MonitorPolicy:
    """One tracked policy's at-a-glance counters (the stable projection)."""

    name: str
    kind: str
    strategy: str
    allowed: int
    denied: int
    limit: int


@dataclass(frozen=True)
class MonitorSnapshot:
    """A point-in-time operational snapshot: the typed envelope + the full snapshot as JSON for depth."""

    generated_at: int
    window_ms: int
    mode: str
    lens_version: str
    node_id: str
    policies: tuple[MonitorPolicy, ...]
    raw_json: str
    """The FULL ``LensSnapshot`` as JSON (cost rooms, per-axis analytics, replay, …). See :meth:`parsed`."""

    def parsed(self) -> dict[str, Any]:
        """The full snapshot decoded from :attr:`raw_json` — for depth beyond the typed fields."""
        data: dict[str, Any] = json.loads(self.raw_json)
        return data


def _snapshot(msg: pb.Snapshot) -> MonitorSnapshot:
    return MonitorSnapshot(
        generated_at=msg.meta.generated_at,
        window_ms=msg.meta.window_ms,
        mode=msg.meta.mode,
        lens_version=msg.meta.lens_version,
        node_id=msg.meta.node_id,
        policies=tuple(
            MonitorPolicy(
                name=p.name,
                kind=p.kind,
                strategy=p.strategy,
                allowed=p.allowed,
                denied=p.denied,
                limit=p.limit,
            )
            for p in msg.policies
        ),
        raw_json=msg.raw_json,
    )


class MonitorBackend:
    """A thin synchronous client for the read-only ``Monitor`` door.

    with MonitorBackend("localhost:50051") as mon:
        snap = mon.get_snapshot()
        for p in snap.policies:
            print(p.name, p.allowed, p.denied)
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
        self._stub = pb_grpc.MonitorStub(self._channel)
        self._metadata = (("x-monitor-secret", secret),) if secret else None

    def get_snapshot(self) -> MonitorSnapshot:
        """A point-in-time operational snapshot (stateless, cacheable)."""
        try:
            resp = self._stub.GetSnapshot(pb.GetSnapshotRequest(), metadata=self._metadata)
        except grpc.RpcError as err:
            raise map_rpc_error(err) from err
        return _snapshot(resp.snapshot)

    def close(self) -> None:
        """Close the underlying channel."""
        self._channel.close()

    def __enter__(self) -> MonitorBackend:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()


__all__ = ["MonitorBackend", "MonitorPolicy", "MonitorSnapshot"]
