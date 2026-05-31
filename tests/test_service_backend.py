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
    from throttlekit.errors import PolicyNotFoundError

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


def test_unknown_policy_maps_to_not_found(backend: ServiceBackend) -> None:
    with pytest.raises(PolicyNotFoundError):
        backend.check("no-such-policy", "k")
