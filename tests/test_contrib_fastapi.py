"""Adapter test: throttlekit.contrib.fastapi.RateLimit dependency via the FastAPI TestClient."""

from __future__ import annotations

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("httpx")

from fastapi import Depends, FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from throttlekit import Decision, PolicyNotFoundError, ServiceUnavailableError  # noqa: E402
from throttlekit.contrib.fastapi import RateLimit  # noqa: E402

ALLOW = Decision(
    allowed=True, limit=100, remaining=50, reset_at=9_999_999_999_000, retry_after_ms=0
)
DENY = Decision(
    allowed=False, limit=100, remaining=0, reset_at=9_999_999_999_000, retry_after_ms=3_000
)


class _SeqChecker:
    def __init__(self, decisions: list[Decision]) -> None:
        self.decisions = decisions
        self.i = 0

    def __call__(self, key: str, *, cost: int = 1) -> Decision:
        d = self.decisions[min(self.i, len(self.decisions) - 1)]
        self.i += 1
        return d


def _app(checker: object, **kw: object) -> FastAPI:
    app = FastAPI()
    dep = RateLimit(checker, **kw)

    @app.get("/", dependencies=[Depends(dep)])
    def root() -> dict[str, bool]:
        return {"ok": True}

    return app


def test_allows_then_denies_with_headers() -> None:
    client = TestClient(_app(_SeqChecker([ALLOW, DENY])))
    r1 = client.get("/")
    assert r1.status_code == 200
    assert r1.headers["RateLimit-Limit"] == "100"
    r2 = client.get("/")
    assert r2.status_code == 429
    assert r2.headers["Retry-After"] == "3"
    assert r2.headers["RateLimit-Remaining"] == "0"


def test_fail_open_on_unavailable() -> None:
    class Boom:
        def __call__(self, key: str, *, cost: int = 1) -> Decision:
            raise ServiceUnavailableError("down")

    assert TestClient(_app(Boom())).get("/").status_code == 200


def test_fail_closed_on_unavailable() -> None:
    class Boom:
        def __call__(self, key: str, *, cost: int = 1) -> Decision:
            raise ServiceUnavailableError("down")

    assert TestClient(_app(Boom(), on_unavailable="deny")).get("/").status_code == 503


def test_policy_not_found_propagates_as_500() -> None:
    # PolicyNotFoundError is a config bug, not an outage — it must NOT be masked by fail-open.
    class NotFound:
        def __call__(self, key: str, *, cost: int = 1) -> Decision:
            raise PolicyNotFoundError("no such policy")

    client = TestClient(_app(NotFound()), raise_server_exceptions=False)
    assert client.get("/").status_code == 500
