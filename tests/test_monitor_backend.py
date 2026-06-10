"""``MonitorBackend`` — snapshot decoding (typed envelope + parsed raw JSON), auth metadata, error mapping."""

from __future__ import annotations

import asyncio

import grpc
import pytest

from throttlekit import ServiceUnavailableError
from throttlekit._grpc import pb
from throttlekit.monitor_backend import MonitorBackend
from throttlekit.monitor_backend_async import AsyncMonitorBackend


class _RpcError(grpc.RpcError):
    def __init__(self, code: grpc.StatusCode, details: str) -> None:
        self._code = code
        self._details = details

    def code(self) -> grpc.StatusCode:
        return self._code

    def details(self) -> str:
        return self._details


def _snapshot_msg() -> object:
    return pb.Snapshot(
        meta=pb.MonitorMeta(
            generated_at=123, window_ms=60_000, mode="process", lens_version="lens-1", node_id="n1"
        ),
        policies=[
            pb.PolicySummary(
                name="api", kind="limiter", strategy="gcra", allowed=7, denied=2, limit=10
            )
        ],
        raw_json='{"meta":{"generatedAt":123},"policies":[{"name":"api"}]}',
    )


class _FakeMonitorStub:
    def __init__(self, *, snapshot: object | None = None, error: Exception | None = None) -> None:
        self._snapshot = snapshot
        self._error = error
        self.calls: list[object] = []

    def GetSnapshot(self, req: object, metadata: object = None) -> object:  # noqa: N802 (gRPC stub name)
        self.calls.append(metadata)
        if self._error is not None:
            raise self._error
        return pb.GetSnapshotResponse(snapshot=self._snapshot)


class _FakeAsyncMonitorStub(_FakeMonitorStub):
    async def GetSnapshot(self, req: object, metadata: object = None) -> object:  # noqa: N802
        return super().GetSnapshot(req, metadata)


def _monitor(stub: object, *, secret: str | None = None) -> MonitorBackend:
    backend = MonitorBackend("localhost:1", secret=secret)
    backend._stub = stub  # type: ignore[assignment]
    return backend


def test_get_snapshot_decodes_meta_policies_and_raw_json() -> None:
    backend = _monitor(_FakeMonitorStub(snapshot=_snapshot_msg()))
    try:
        snap = backend.get_snapshot()
        assert (snap.generated_at, snap.window_ms, snap.mode, snap.node_id) == (
            123,
            60_000,
            "process",
            "n1",
        )
        assert len(snap.policies) == 1
        p = snap.policies[0]
        assert (p.name, p.kind, p.strategy, p.allowed, p.denied, p.limit) == (
            "api",
            "limiter",
            "gcra",
            7,
            2,
            10,
        )
        assert snap.parsed()["meta"]["generatedAt"] == 123  # full depth via raw_json
    finally:
        backend.close()


def test_get_snapshot_sends_monitor_secret_metadata() -> None:
    stub = _FakeMonitorStub(snapshot=_snapshot_msg())
    backend = _monitor(stub, secret="m0n")
    try:
        backend.get_snapshot()
        assert stub.calls[0] == (("x-monitor-secret", "m0n"),)
    finally:
        backend.close()


def test_get_snapshot_maps_unavailable() -> None:
    backend = _monitor(_FakeMonitorStub(error=_RpcError(grpc.StatusCode.UNAVAILABLE, "down")))
    try:
        with pytest.raises(ServiceUnavailableError):
            backend.get_snapshot()
    finally:
        backend.close()


def test_async_get_snapshot_decodes_snapshot() -> None:
    async def run() -> object:
        backend = AsyncMonitorBackend("localhost:1")
        backend._stub = _FakeAsyncMonitorStub(snapshot=_snapshot_msg())  # type: ignore[assignment]
        try:
            return await backend.get_snapshot()
        finally:
            await backend.close()

    snap = asyncio.run(run())
    assert snap.generated_at == 123
    assert snap.policies[0].name == "api"
