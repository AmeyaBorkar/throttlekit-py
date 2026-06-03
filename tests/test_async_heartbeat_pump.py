"""Unit tests for the async heartbeat pump (no server) — pins the reclaim signal and error resilience.

These cover invariant 1 (the server→client *reclaim* transport) and the pump's best-effort resilience,
which the live-server async tests can't pin deterministically.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

pytest.importorskip("grpc")

from throttlekit import Decision  # noqa: E402
from throttlekit.service_backend_async import AsyncAdmission, _AsyncHeartbeatPump  # noqa: E402

_HELD = Decision(allowed=True, limit=1, remaining=0, reset_at=0, retry_after_ms=0)


def _admission(lease_id: str) -> AsyncAdmission:
    # backend is unused on the reclaim path (no release call), so a dummy is fine.
    return AsyncAdmission(None, _HELD, lease_id, 0, "", False, heartbeat=True)  # type: ignore[arg-type]


class _ReclaimStub:
    """A fake RateLimiterStub whose Heartbeat reports the given lease ids as reclaimed."""

    def __init__(self, reclaimed: list[str]) -> None:
        self._reclaimed = reclaimed
        self.calls = 0

    async def Heartbeat(self, request: Any) -> Any:
        self.calls += 1

        class _Resp:
            reclaimed_ids = self._reclaimed

        return _Resp()


class _BoomStub:
    """A fake stub whose Heartbeat always raises a NON-RpcError — the pump must survive it."""

    def __init__(self) -> None:
        self.calls = 0

    async def Heartbeat(self, request: Any) -> Any:
        self.calls += 1
        raise RuntimeError("boom")


def test_pump_marks_admission_reclaimed_on_missed_beat() -> None:
    async def go() -> None:
        stub = _ReclaimStub(reclaimed=["L1"])
        pump = _AsyncHeartbeatPump(stub, interval_s=0.02)  # type: ignore[arg-type]
        adm = _admission("L1")
        pump.register(adm)
        for _ in range(50):
            await asyncio.sleep(0.02)
            if adm.reclaimed:
                break
        assert adm.reclaimed, "the pump never propagated the server's reclaim to the admission"
        await pump.close()

    asyncio.run(go())


def test_pump_leaves_unreclaimed_admission_alone() -> None:
    async def go() -> None:
        stub = _ReclaimStub(reclaimed=[])  # server renews, reclaims nothing
        pump = _AsyncHeartbeatPump(stub, interval_s=0.02)  # type: ignore[arg-type]
        adm = _admission("L2")
        pump.register(adm)
        await asyncio.sleep(0.1)
        assert stub.calls >= 1  # it did beat
        assert not adm.reclaimed
        await pump.close()

    asyncio.run(go())


def test_pump_survives_heartbeat_errors() -> None:
    async def go() -> None:
        stub = _BoomStub()
        pump = _AsyncHeartbeatPump(stub, interval_s=0.02)  # type: ignore[arg-type]
        adm = _admission("L3")
        pump.register(adm)
        await asyncio.sleep(0.12)  # several beats, every one raising
        assert stub.calls >= 2, "the pump died on the first error instead of retrying"
        assert not adm.reclaimed
        await pump.close()

    asyncio.run(go())
