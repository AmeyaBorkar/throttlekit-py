"""The asyncio twin of :mod:`throttlekit.monitor_backend` — ``AsyncMonitorBackend`` over ``grpc.aio``."""

from __future__ import annotations

from types import TracebackType

import grpc

from ._grpc import map_rpc_error, pb, pb_grpc
from .monitor_backend import MonitorSnapshot, _snapshot


class AsyncMonitorBackend:
    """An asyncio client for the read-only ``Monitor`` door (see :class:`~throttlekit.MonitorBackend`)."""

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
        self._stub = pb_grpc.MonitorStub(self._channel)
        self._metadata = (("x-monitor-secret", secret),) if secret else None

    async def get_snapshot(self) -> MonitorSnapshot:
        """A point-in-time operational snapshot (stateless, cacheable)."""
        try:
            resp = await self._stub.GetSnapshot(pb.GetSnapshotRequest(), metadata=self._metadata)
        except grpc.RpcError as err:
            raise map_rpc_error(err) from err
        return _snapshot(resp.snapshot)

    async def close(self) -> None:
        """Close the underlying channel."""
        await self._channel.close()

    async def __aenter__(self) -> AsyncMonitorBackend:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close()


__all__ = ["AsyncMonitorBackend"]
