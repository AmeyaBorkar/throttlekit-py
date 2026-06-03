"""Cross-language conformance for the async door: the ``AsyncServiceBackend`` ↔ the Node server.

The async twin of ``tests/test_service_backend.py``: same skip-if-absent guards (grpcio + stubs + node +
a built server), same shared-server module fixture, same clock-independent assertions. Each test drives a
coroutine with ``asyncio.run`` (no pytest-asyncio dependency), creating + closing the aio backend inside
its own loop.
"""

from __future__ import annotations

import asyncio
import os
import pathlib
import shutil
import socket
import subprocess
import time
from collections.abc import Iterator

import pytest

pytestmark = pytest.mark.integration

grpc = pytest.importorskip("grpc")

ROOT = pathlib.Path(__file__).resolve().parent.parent
CORE_REPO = pathlib.Path(os.environ.get("THROTTLEKIT_REPO", str(ROOT.parent / "GreenfeildProject")))
SERVER_BIN = CORE_REPO / "server" / "dist" / "bin.js"
POLICIES = ROOT / "tests" / "_policies.yaml"

try:
    from throttlekit import AsyncServiceBackend
    from throttlekit.errors import OperationNotSupportedError, PolicyNotFoundError

    _STUBS_OK = True
except ImportError:
    _STUBS_OK = False


def _free_port() -> int:
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def _wait_until_listening(port: int, timeout: float = 12.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        with socket.socket() as sock:
            sock.settimeout(0.5)
            if sock.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(0.1)
    return False


@pytest.fixture(scope="module")
def target() -> Iterator[str]:
    if not _STUBS_OK:
        pytest.skip("generated gRPC stubs unavailable — run scripts/gen_proto.py")
    if shutil.which("node") is None:
        pytest.skip("node not on PATH")
    if not SERVER_BIN.exists():
        pytest.skip(
            f"server not built: {SERVER_BIN} (run `npm run build` in the core repo's server/)"
        )

    port = _free_port()
    proc = subprocess.Popen(
        [
            "node",
            str(SERVER_BIN),
            "--config",
            str(POLICIES),
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
        ]
    )
    try:
        if not _wait_until_listening(port):
            proc.terminate()
            pytest.skip("throttlekit-server did not start listening in time")
        yield f"127.0.0.1:{port}"
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


def test_cold_burst_admits_then_denies(target: str) -> None:
    async def go() -> list[tuple[bool, int]]:
        async with AsyncServiceBackend(target) as rl:
            out = []
            for _ in range(6):
                d = await rl.check("api", "auser-1")
                out.append((d.allowed, d.remaining))
            return out

    results = asyncio.run(go())
    assert [allowed for allowed, _ in results] == [True, True, True, True, True, False]
    assert [remaining for _, remaining in results] == [4, 3, 2, 1, 0, 0]


def test_unknown_policy_maps_to_not_found(target: str) -> None:
    async def go() -> None:
        async with AsyncServiceBackend(target) as rl:
            await rl.check("no-such-policy", "k")

    with pytest.raises(PolicyNotFoundError):
        asyncio.run(go())


def test_admit_on_a_rate_limiter_is_unsupported(target: str) -> None:
    async def go() -> None:
        async with AsyncServiceBackend(target) as rl:
            await rl.admit("api", "k")  # `api` is a rate limiter, reached by check, not admit

    with pytest.raises(OperationNotSupportedError):
        asyncio.run(go())


def test_admission_context_manager_releases_on_exit(target: str) -> None:
    async def go() -> None:
        async with AsyncServiceBackend(target) as rl:
            a1 = await rl.admit("cc", "actx")
            a2 = await rl.admit("cc", "actx")
            a3 = await rl.admit("cc", "actx")
            async with a1, a2, a3:
                assert a1.allowed and a2.allowed and a3.allowed
                denied = await rl.admit("cc", "actx")
                assert not denied.allowed
                assert denied.binding_axis == "concurrency"
            # the `async with` released all three on exit → a fresh admit succeeds again
            after = await rl.admit("cc", "actx")
            assert after.allowed
            await after.release()

    asyncio.run(go())


def test_heartbeat_keeps_a_long_hold_alive(target: str) -> None:
    async def go() -> None:
        async with AsyncServiceBackend(target) as rl:
            held = await rl.admit("single", "ahb", heartbeat=True)
            async with held:
                assert held.allowed
                await asyncio.sleep(3.0)  # > leaseTtlMs (2000); the async pump renews it across it
                assert not held.reclaimed
                blocked = await rl.admit("single", "ahb")
                assert not blocked.allowed  # still held by the heart-beaten lease
            freed = await rl.admit("single", "ahb")
            assert freed.allowed  # released on context exit
            await freed.release()

    asyncio.run(go())


def test_admission_releases_and_reraises_on_body_exception(target: str) -> None:
    # Invariant 5: a body exception still releases the slot (dropped=True) AND propagates (__aexit__
    # returns None, never suppresses). `single` has a pinned ceiling of 1, so a freed slot is observable.
    class _Boom(Exception):
        pass

    async def go() -> None:
        async with AsyncServiceBackend(target) as rl:
            adm = await rl.admit("single", "aexc")
            assert adm.allowed
            with pytest.raises(_Boom):
                async with adm:
                    raise _Boom()
            # the slot was released despite the exception → a fresh admit succeeds
            after = await rl.admit("single", "aexc")
            assert after.allowed
            await after.release()

    asyncio.run(go())


def test_admission_double_release_is_idempotent(target: str) -> None:
    async def go() -> None:
        async with AsyncServiceBackend(target) as rl:
            adm = await rl.admit("single", "aidem")
            assert adm.allowed
            await adm.release()
            await adm.release()  # idempotent no-op, no error
            after = await rl.admit("single", "aidem")
            assert after.allowed
            await after.release()

    asyncio.run(go())


def test_server_reclaims_abandoned_leases(target: str) -> None:
    # Crash recovery over the async door: hold all 3 slots and never release; the server's TTL sweep
    # reclaims them so a later admit eventually succeeds. (Defined last — it leaves leases to expire.)
    async def go() -> None:
        async with AsyncServiceBackend(target) as rl:
            abandoned = [await rl.admit("cc", "acrash") for _ in range(3)]
            assert all(a.allowed for a in abandoned)
            assert not (
                await rl.admit("cc", "acrash")
            ).allowed  # full while abandoned slots are held
            deadline = time.time() + 8.0
            reclaimed = False
            while time.time() < deadline:
                a = await rl.admit("cc", "acrash")
                if a.allowed:
                    await a.release()
                    reclaimed = True
                    break
                await asyncio.sleep(0.25)
            assert reclaimed, "the server did not reclaim the abandoned leases within 8s"

    asyncio.run(go())
