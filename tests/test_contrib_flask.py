"""Adapter test: throttlekit.contrib.flask.ThrottleKit extension via the Flask test client."""

from __future__ import annotations

import pytest

pytest.importorskip("flask")

from flask import Flask  # noqa: E402

from throttlekit import Decision, PolicyNotFoundError, ServiceUnavailableError  # noqa: E402
from throttlekit.contrib.flask import ThrottleKit  # noqa: E402

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
        raise ServiceUnavailableError("down")


def _app(checker: object, **kw: object) -> Flask:
    app = Flask(__name__)
    tk = ThrottleKit(app, checker=checker, **kw)

    @app.get("/")
    @tk.limit()
    def index() -> str:
        return "ok"

    return app


def test_extension_registers_on_app() -> None:
    app = _app(_SeqChecker([ALLOW]))
    assert "throttlekit" in app.extensions


def test_allows_then_denies_with_headers() -> None:
    client = _app(_SeqChecker([ALLOW, DENY])).test_client()
    r1 = client.get("/")
    assert r1.status_code == 200
    assert r1.headers["RateLimit-Limit"] == "100"
    r2 = client.get("/")
    assert r2.status_code == 429
    assert r2.headers["Retry-After"] == "3"
    assert r2.headers["RateLimit-Remaining"] == "0"


def test_fail_open_on_unavailable() -> None:
    assert _app(_Boom()).test_client().get("/").status_code == 200


def test_fail_closed_on_unavailable() -> None:
    assert _app(_Boom(), on_unavailable="deny").test_client().get("/").status_code == 503


def test_legacy_style_headers() -> None:
    r = _app(_SeqChecker([DENY]), style="legacy").test_client().get("/")
    assert r.status_code == 429
    assert "X-RateLimit-Limit" in r.headers
    assert "RateLimit-Limit" not in r.headers


def test_non_outage_error_is_not_masked_as_200() -> None:
    # A config error (not an outage) must surface as a 500, never be silently admitted (invariant 6).
    class _NotFound:
        def __call__(self, key: str, *, cost: int = 1) -> Decision:
            raise PolicyNotFoundError("no such policy")

    assert _app(_NotFound()).test_client().get("/").status_code == 500


def test_per_route_cost_override() -> None:
    seen: dict[str, int] = {}

    class _Recording:
        def __call__(self, key: str, *, cost: int = 1) -> Decision:
            seen["cost"] = cost
            return ALLOW

    app = Flask(__name__)
    tk = ThrottleKit(app, checker=_Recording(), default_cost=1)

    @app.get("/")
    @tk.limit(cost=7)
    def index() -> str:
        return "ok"

    app.test_client().get("/")
    assert seen["cost"] == 7
