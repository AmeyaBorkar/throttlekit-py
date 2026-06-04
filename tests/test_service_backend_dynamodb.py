"""Cross-language conformance over a **DynamoDB-backed** server: Python ``ServiceBackend`` ↔ a
``throttlekit-server`` launched with ``--store dynamodb``.

The PY-A2 deliverable — a Python client reaching a *DynamoDB* store through the service door. The client
is store-agnostic (the same gRPC regardless of backend) and **the core computes every decision
server-side** (one oracle), so the same clock-independent asserts the memory/Redis/Postgres path
satisfies — a cold burst admits exactly ``burst`` then denies — must hold with DynamoDB holding the
state via its version CAS.

Skipped unless: grpcio + stubs + node + a built server are present **and** a DynamoDB endpoint is
reachable (``THROTTLEKIT_TEST_DYNAMODB``, e.g. dynamodb-local on :8000). Locally:

    docker run -d --name tk-dynamodb -p 8000:8000 amazon/dynamodb-local
    npm --prefix ../GreenfeildProject/server run build
    THROTTLEKIT_TEST_DYNAMODB=http://127.0.0.1:8000 \
      python -m pytest tests/test_service_backend_dynamodb.py -v

The server provisions the single-``pk`` table itself (``--dynamodb-create-table``), so this needs no
boto3. dynamodb-local ignores credentials, but the AWS SDK requires them present — dummy creds are
passed in the spawned server's env. A fresh per-run ``--dynamodb-prefix`` keeps each invocation's keys
distinct (DynamoDB persists across server restarts, unlike the memory store).
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

DDB_ENDPOINT = os.environ.get("THROTTLEKIT_TEST_DYNAMODB")
DDB_REGION = "us-east-1"
DDB_TABLE = "tk_py_e2e_ddb"
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


def _wait_until_listening(port: int, timeout: float = 15.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        with socket.socket() as sock:
            sock.settimeout(0.5)
            if sock.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(0.1)
    return False


def _endpoint_reachable(url: str, timeout: float = 1.0) -> bool:
    parsed = urlparse(url)
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or 8000
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
    if DDB_ENDPOINT is None or not _endpoint_reachable(DDB_ENDPOINT):
        pytest.skip("no DynamoDB reachable at THROTTLEKIT_TEST_DYNAMODB (start dynamodb-local on :8000)")

    port = _free_port()
    # dynamodb-local ignores credentials, but the AWS SDK requires them to be present.
    env = {
        **os.environ,
        "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID", "dummy"),
        "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY", "dummy"),
        "AWS_REGION": os.environ.get("AWS_REGION", DDB_REGION),
    }
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
            "dynamodb",
            "--dynamodb-table",
            DDB_TABLE,
            "--dynamodb-endpoint",
            DDB_ENDPOINT,
            "--dynamodb-region",
            DDB_REGION,
            "--dynamodb-prefix",
            RUN_PREFIX,
            "--dynamodb-create-table",  # provision the single-pk table on boot ⇒ no boto3 here
        ],
        env=env,
    )
    try:
        if not _wait_until_listening(port):
            proc.terminate()
            pytest.skip("throttlekit-server (--store dynamodb) did not start listening in time")
        client = ServiceBackend(f"127.0.0.1:{port}")
        yield client
        client.close()
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


def test_cold_burst_admits_then_denies_over_dynamodb(backend: ServiceBackend) -> None:
    # `api` is a plain GCRA (burst 5) whose state now lives in DynamoDB. The cold burst must admit
    # exactly the burst then deny — bit-identical to the memory/Redis/Postgres path, because the core
    # computes the decision and DynamoDB only transports state. Fresh key ⇒ a clean GCRA bucket.
    key = f"user-{uuid.uuid4().hex}"
    results = [backend.check("api", key) for _ in range(6)]
    assert [r.allowed for r in results] == [True, True, True, True, True, False]
    assert [r.remaining for r in results] == [4, 3, 2, 1, 0, 0]
    assert results[0].limit == 5
    assert results[5].retry_after_ms > 0  # the denied request advises a wait


def test_independent_keys_have_independent_budgets_over_dynamodb(backend: ServiceBackend) -> None:
    a = f"alice-{uuid.uuid4().hex}"
    b = f"bob-{uuid.uuid4().hex}"
    assert backend.check("api", a).allowed
    assert backend.check("api", b).allowed  # bob's budget is untouched by alice (per-key items in DDB)


def test_unknown_policy_maps_to_not_found(backend: ServiceBackend) -> None:
    with pytest.raises(PolicyNotFoundError):
        backend.check("no-such-policy", "k")
