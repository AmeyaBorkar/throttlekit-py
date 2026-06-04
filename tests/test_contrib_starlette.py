"""Adapter test: throttlekit.contrib.starlette.ThrottleKitMiddleware via the Starlette TestClient."""

from __future__ import annotations

import asyncio

import pytest

pytest.importorskip("starlette")
pytest.importorskip("httpx")

from starlette.applications import Starlette  # noqa: E402
from starlette.responses import PlainTextResponse  # noqa: E402
from starlette.routing import Route  # noqa: E402
from starlette.testclient import TestClient  # noqa: E402

from throttlekit import Decision, PolicyNotFoundError, ServiceUnavailableError  # noqa: E402
from throttlekit.contrib.starlette import ThrottleKitMiddleware  # noqa: E402

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


class _Boom:
    def __call__(self, key: str, *, cost: int = 1) -> Decision:
        raise ServiceUnavailableError("backend down")


def _app(checker: object, **kw: object) -> Starlette:
    async def home(request: object) -> PlainTextResponse:
        return PlainTextResponse("ok")

    app = Starlette(routes=[Route("/", home)])
    app.add_middleware(ThrottleKitMiddleware, checker=checker, **kw)
    return app


def test_allows_then_denies_with_headers() -> None:
    client = TestClient(_app(_SeqChecker([ALLOW, DENY])))
    r1 = client.get("/")
    assert r1.status_code == 200
    assert r1.headers["RateLimit-Limit"] == "100"
    assert r1.headers["RateLimit-Remaining"] == "50"
    r2 = client.get("/")
    assert r2.status_code == 429
    assert r2.headers["Retry-After"] == "3"
    assert r2.headers["RateLimit-Remaining"] == "0"


def test_fail_open_on_unavailable() -> None:
    assert TestClient(_app(_Boom())).get("/").status_code == 200


def test_fail_closed_on_unavailable() -> None:
    assert TestClient(_app(_Boom(), on_unavailable="deny")).get("/").status_code == 503


def test_legacy_style_headers() -> None:
    r = TestClient(_app(_SeqChecker([DENY]), style="legacy")).get("/")
    assert r.status_code == 429
    assert "X-RateLimit-Limit" in r.headers
    assert "RateLimit-Limit" not in r.headers


class _AsyncSeqChecker:
    def __init__(self, decisions: list[Decision]) -> None:
        self.decisions = decisions
        self.i = 0

    async def __call__(self, key: str, *, cost: int = 1) -> Decision:
        d = self.decisions[min(self.i, len(self.decisions) - 1)]
        self.i += 1
        return d


def test_async_checker_is_awaited() -> None:
    # An async checker must be awaited on the loop (invariant 4), not thread-offloaded — it still works.
    client = TestClient(_app(_AsyncSeqChecker([ALLOW, DENY])))
    assert client.get("/").status_code == 200
    assert client.get("/").status_code == 429


def test_non_outage_error_is_not_masked() -> None:
    # A config error (not an outage) must propagate, never be silently admitted (invariant 6).
    class _NotFound:
        def __call__(self, key: str, *, cost: int = 1) -> Decision:
            raise PolicyNotFoundError("no such policy")

    with pytest.raises(PolicyNotFoundError):
        TestClient(_app(_NotFound())).get("/")


def test_cost_is_passed_to_checker() -> None:
    seen: dict[str, int] = {}

    class _Recording:
        def __call__(self, key: str, *, cost: int = 1) -> Decision:
            seen["cost"] = cost
            return ALLOW

    TestClient(_app(_Recording(), cost=5)).get("/")
    assert seen["cost"] == 5


def test_non_http_scope_passes_through_without_consulting_checker() -> None:
    # A websocket/lifespan scope must reach the inner app and never trigger a rate-limit check.
    called = {"inner": False}

    async def inner(scope: object, receive: object, send: object) -> None:
        called["inner"] = True

    checker = _SeqChecker([DENY])  # would deny if (wrongly) consulted
    mw = ThrottleKitMiddleware(inner, checker=checker)

    async def _noop() -> dict[str, str]:
        return {"type": "noop"}

    async def go() -> None:
        await mw({"type": "websocket"}, _noop, _noop)

    asyncio.run(go())
    assert called["inner"] is True
    assert checker.i == 0  # the checker was never consulted
