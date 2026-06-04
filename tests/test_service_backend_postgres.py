"""Cross-language conformance over a **Postgres-backed** server: Python ``ServiceBackend`` ↔ a
``throttlekit-server`` launched with ``--store postgres``.

This is the PY-A1 deliverable — proving a Python client reaches a *non-Redis* store through the service
door. The Python client is store-agnostic: it sends the same gRPC requests regardless of the server's
backend, and the **core still computes every decision server-side** (the one-oracle invariant). So the
*same* clock-independent asserts the in-memory/Redis path satisfies — a cold burst admits exactly
``burst`` then denies — must hold with Postgres holding the limiter state.

Skipped unless: grpcio + stubs + node + a built server are present **and** a Postgres is reachable
(``THROTTLEKIT_POSTGRES_URL``, default the project's ``tk-postgres`` on :5433). Locally:

    docker start tk-postgres
    npm --prefix ../GreenfeildProject/server run build
    THROTTLEKIT_POSTGRES_URL=postgresql://throttlekit:throttlekit@127.0.0.1:5433/throttlekit \
      python -m pytest tests/test_service_backend_postgres.py -v

Postgres (unlike the per-process memory store) *persists across server restarts*, so every run uses a
fresh ``--postgres-prefix`` to get a clean key namespace — otherwise a previous run's consumed burst
would bleed into this one.
"""

from __future__ import annotations

import os
import pathlib
import shutil
import socket
import subprocess
import time
import uuid
from collections.abc import Iterator
from urllib.parse import urlparse

import pytest

pytestmark = pytest.mark.integration

grpc = pytest.importorskip("grpc")

ROOT = pathlib.Path(__file__).resolve().parent.parent
CORE_REPO = pathlib.Path(os.environ.get("THROTTLEKIT_REPO", str(ROOT.parent / "GreenfeildProject")))
SERVER_BIN = CORE_REPO / "server" / "dist" / "bin.js"
POLICIES = ROOT / "tests" / "_policies.yaml"

# The project convention: tk-postgres on :5433 (5432 is another project's). Override via env.
POSTGRES_URL = os.environ.get(
    "THROTTLEKIT_POSTGRES_URL", "postgresql://throttlekit:throttlekit@127.0.0.1:5433/throttlekit"
)
# A dedicated table keeps the e2e state out of any other Postgres-backed run; a fresh per-run prefix
# keeps each invocation's keys distinct despite Postgres persisting across server restarts.
PG_TABLE = "tk_py_e2e"
RUN_PREFIX = f"py-e2e-{uuid.uuid4().hex[:8]}"

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


def _postgres_reachable(url: str, timeout: float = 1.0) -> bool:
    """A light TCP pre-flight so we skip cleanly (rather than fail-open silently admitting) when no
    Postgres is up. Does not authenticate — just confirms the host:port accepts a connection."""
    parsed = urlparse(url)
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or 5432
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
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
    if not _postgres_reachable(POSTGRES_URL):
        pytest.skip(f"no Postgres reachable at {POSTGRES_URL} (start tk-postgres on :5433)")

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
            "--store",
            "postgres",
            "--postgres-url",
            POSTGRES_URL,
            "--postgres-table",
            PG_TABLE,
            "--postgres-prefix",
            RUN_PREFIX,
        ]
    )
    try:
        if not _wait_until_listening(port):
            proc.terminate()
            pytest.skip("throttlekit-server (--store postgres) did not start listening in time")
        client = ServiceBackend(f"127.0.0.1:{port}")
        yield client
        client.close()
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


def test_cold_burst_admits_then_denies_over_postgres(backend: ServiceBackend) -> None:
    # `api` is a plain GCRA (burst 5) whose state now lives in Postgres. The cold burst must admit
    # exactly the burst then deny — bit-identical to the memory/Redis path, because the core computes
    # the decision and Postgres only transports the state. Fresh key ⇒ a clean GCRA bucket.
    key = f"user-{uuid.uuid4().hex}"
    results = [backend.check("api", key) for _ in range(6)]
    assert [r.allowed for r in results] == [True, True, True, True, True, False]
    assert [r.remaining for r in results] == [4, 3, 2, 1, 0, 0]
    assert results[0].limit == 5
    assert results[5].retry_after_ms > 0  # the denied request advises a wait


def test_independent_keys_have_independent_budgets_over_postgres(backend: ServiceBackend) -> None:
    a = f"alice-{uuid.uuid4().hex}"
    b = f"bob-{uuid.uuid4().hex}"
    assert backend.check("api", a).allowed
    assert backend.check("api", b).allowed  # bob's budget is untouched by alice (per-key rows in PG)


def test_leased_two_tier_over_postgres(backend: ServiceBackend) -> None:
    # Door A over Postgres: `leased` is a two-tier *leased* limiter whose shared L2 is now the Postgres
    # store. The cold burst still admits exactly the budget then denies — the core's two-tier decision,
    # leased from a Postgres L2.
    key = f"leased-{uuid.uuid4().hex}"
    results = [backend.check("leased", key) for _ in range(6)]
    assert [r.allowed for r in results] == [True, True, True, True, True, False]
    assert results[0].limit == 5
    assert results[5].retry_after_ms > 0


def test_unknown_policy_maps_to_not_found(backend: ServiceBackend) -> None:
    with pytest.raises(PolicyNotFoundError):
        backend.check("no-such-policy", "k")
