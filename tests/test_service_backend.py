"""Cross-language behavioral conformance: the Python ``ServiceBackend`` ↔ the Node ``throttlekit-server``.

Marked ``integration`` and **skipped unless** grpcio + the generated stubs + node + a built server are all
available. The rigorous, time-parametrized golden-vector replay is the Node-side oracle test (it can
drive a ManualClock in-process); over a real cross-process connection the server uses wall-clock time, so
this asserts the *clock-independent* behavior — a cold burst admits exactly ``burst`` then denies, and an
unknown policy maps to ``NOT_FOUND`` — proving the client faithfully transports the core's decisions.
"""

from __future__ import annotations

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
    from throttlekit import ServiceBackend
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
def backend() -> Iterator[ServiceBackend]:
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
        client = ServiceBackend(f"127.0.0.1:{port}")
        yield client
        client.close()
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


def test_cold_burst_admits_then_denies(backend: ServiceBackend) -> None:
    results = [backend.check("api", "user-1") for _ in range(6)]
    assert [r.allowed for r in results] == [True, True, True, True, True, False]
    assert [r.remaining for r in results] == [4, 3, 2, 1, 0, 0]
    assert results[0].limit == 5
    assert results[5].retry_after_ms > 0  # the denied request advises a wait


def test_independent_keys_have_independent_budgets(backend: ServiceBackend) -> None:
    assert backend.check("api", "alice").allowed
    assert backend.check("api", "bob").allowed  # bob's budget is untouched by alice


def test_leased_two_tier_policy_is_reachable(backend: ServiceBackend) -> None:
    # Door A: `leased` is a two-tier *leased* limiter (L1-local credits over a shared L2). The advanced
    # axis is reachable with a plain `check` — no new client API — and the core still computes the
    # decision, so the cold burst admits exactly the budget then denies, just like a plain limiter.
    results = [backend.check("leased", "user-leased") for _ in range(6)]
    assert [r.allowed for r in results] == [True, True, True, True, True, False]
    assert results[0].limit == 5
    assert results[5].retry_after_ms > 0  # the denied request advises a wait


def test_token_budget_debit_is_reachable(backend: ServiceBackend) -> None:
    # Door B: `budget` is a token-budget meter (the cost axis), reached via the new `debit` op. Debiting
    # one token at a time spends the budget of 5 then refuses — the core's tokenBudget primitive, over
    # the wire, from Python.
    results = [backend.debit("budget", "tenant-1", 1) for _ in range(6)]
    assert [r.allowed for r in results] == [True, True, True, True, True, False]
    assert results[0].limit == 5
    assert results[5].retry_after_ms > 0  # the window has not rolled, so a wait is advised


def test_unknown_policy_maps_to_not_found(backend: ServiceBackend) -> None:
    with pytest.raises(PolicyNotFoundError):
        backend.check("no-such-policy", "k")


def test_concurrency_admit_holds_then_denies_then_release_frees(backend: ServiceBackend) -> None:
    # Door C: `cc` is a concurrency-only admitter (pinned ceiling 3). Three admits hold a slot each; the
    # fourth is denied on the concurrency axis (and holds nothing). Releasing one frees a slot.
    held = [backend.admit("cc", "k") for _ in range(4)]
    assert [a.allowed for a in held] == [True, True, True, False]
    assert all(a.held for a in held[:3])
    assert not held[3].held
    assert held[3].binding_axis == "concurrency"
    held[0].release()  # free one slot
    extra = backend.admit("cc", "k")
    assert extra.allowed
    for a in (held[1], held[2], extra):  # leave the guard empty for the next test
        a.release()


def test_admission_context_manager_releases_on_exit(backend: ServiceBackend) -> None:
    with (
        backend.admit("cc", "ctx") as a1,
        backend.admit("cc", "ctx") as a2,
        backend.admit("cc", "ctx") as a3,
    ):
        assert a1.allowed and a2.allowed and a3.allowed
        assert not backend.admit("cc", "ctx").allowed  # all three slots are held
    # the `with` released all three on exit → a fresh admit succeeds again
    after = backend.admit("cc", "ctx")
    assert after.allowed
    after.release()


def test_unified_admit_binds_on_the_concurrency_axis(backend: ServiceBackend) -> None:
    # `unified` = rate(gcra burst 5) × concurrency(2). The concurrency ceiling (2) binds before the rate.
    a = [backend.admit("unified", "u") for _ in range(3)]
    assert [x.allowed for x in a] == [True, True, False]
    assert a[2].binding_axis == "concurrency"
    for x in a[:2]:
        x.release()


def test_admit_on_a_rate_limiter_is_unsupported(backend: ServiceBackend) -> None:
    with pytest.raises(OperationNotSupportedError):
        backend.admit("api", "k")  # `api` is a rate limiter, reached by `check`, not `admit`


def test_heartbeat_keeps_a_long_hold_alive(backend: ServiceBackend) -> None:
    # `single` is a pinned ceiling of 1. With heartbeat=True a daemon thread renews the lease, so holding
    # past the server's 2s lease TTL must NOT be reclaimed — the long-hold story, over the wire.
    with backend.admit("single", "hb", heartbeat=True) as a:
        assert a.allowed
        time.sleep(3.0)  # > leaseTtlMs (2000); the heartbeat pump renews it across the boundary
        assert not a.reclaimed
        assert not backend.admit("single", "hb").allowed  # still held by the heart-beaten lease
    assert backend.admit("single", "hb").allowed  # released on context exit


def test_server_reclaims_abandoned_leases(backend: ServiceBackend) -> None:
    # Crash recovery, over the wire: hold all 3 slots and never release. The server's TTL sweep reclaims
    # the abandoned slots, so a later admit eventually succeeds. (Defined last: it leaves leases to expire.)
    abandoned = [backend.admit("cc", "crash") for _ in range(3)]
    assert all(a.allowed for a in abandoned)
    assert not backend.admit("cc", "crash").allowed  # full while the abandoned slots are still held
    deadline = time.time() + 8.0
    reclaimed = False
    while time.time() < deadline:
        a = backend.admit("cc", "crash")
        if a.allowed:
            a.release()
            reclaimed = True
            break
        time.sleep(0.25)
    assert reclaimed, "the server did not reclaim the abandoned leases within 8s"
