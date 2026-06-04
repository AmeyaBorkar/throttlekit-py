"""Adapter test: throttlekit.contrib.django rate_limit decorator + ThrottleKitMiddleware.

Uses ``RequestFactory`` to build requests and calls the decorated views directly — no URLconf, DB, or
server needed.
"""

from __future__ import annotations

import pytest

pytest.importorskip("django")

from django.conf import settings  # noqa: E402

if not settings.configured:
    settings.configure(
        DEBUG=True,
        SECRET_KEY="test-only",
        ALLOWED_HOSTS=["*"],
        INSTALLED_APPS=[],
        DATABASES={},
        MIDDLEWARE=[],
    )
    import django  # noqa: E402

    django.setup()

from django.http import HttpResponse  # noqa: E402
from django.test import RequestFactory  # noqa: E402

from throttlekit import Decision, PolicyNotFoundError, ServiceUnavailableError  # noqa: E402
from throttlekit.contrib.django import Ratelimited, ThrottleKitMiddleware, rate_limit  # noqa: E402

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


def _req() -> object:
    return RequestFactory().get("/", REMOTE_ADDR="1.2.3.4")


def test_block_true_raises_ratelimited() -> None:
    @rate_limit(_SeqChecker([DENY]), key=lambda r: "k")
    def view(request: object) -> HttpResponse:
        return HttpResponse("ok")

    with pytest.raises(Ratelimited) as ei:
        view(_req())
    assert ei.value.decision is DENY


def test_allowed_runs_and_stamps_headers() -> None:
    @rate_limit(_SeqChecker([ALLOW]), key=lambda r: "k")
    def view(request: object) -> HttpResponse:
        return HttpResponse("ok")

    resp = view(_req())
    assert resp.status_code == 200
    assert resp.headers["RateLimit-Limit"] == "100"


def test_block_false_sets_request_attr_and_runs() -> None:
    captured: dict[str, bool] = {}

    @rate_limit(_SeqChecker([DENY]), key=lambda r: "k", block=False)
    def view(request: object) -> HttpResponse:
        captured["allowed"] = request.throttlekit_decision.allowed  # type: ignore[attr-defined]
        return HttpResponse("ran")

    resp = view(_req())
    assert resp.status_code == 200
    assert captured["allowed"] is False


def test_middleware_maps_ratelimited_to_429() -> None:
    mw = ThrottleKitMiddleware(get_response=lambda req: HttpResponse("ok"))
    resp = mw.process_exception(_req(), Ratelimited(DENY))
    assert resp is not None
    assert resp.status_code == 429
    assert resp.headers["Retry-After"] == "3"


def test_fail_open_on_unavailable() -> None:
    @rate_limit(_Boom(), key=lambda r: "k")
    def view(request: object) -> HttpResponse:
        return HttpResponse("ok")

    assert view(_req()).status_code == 200


def test_fail_closed_on_unavailable_returns_503() -> None:
    @rate_limit(_Boom(), key=lambda r: "k", on_unavailable="deny")
    def view(request: object) -> HttpResponse:
        return HttpResponse("ok")

    assert view(_req()).status_code == 503


def test_non_outage_error_propagates() -> None:
    # A config error (not an outage) must propagate, never be silently admitted (invariant 6).
    class _NotFound:
        def __call__(self, key: str, *, cost: int = 1) -> Decision:
            raise PolicyNotFoundError("no such policy")

    @rate_limit(_NotFound(), key=lambda r: "k")
    def view(request: object) -> HttpResponse:
        return HttpResponse("ok")

    with pytest.raises(PolicyNotFoundError):
        view(_req())


def test_fail_open_sets_decision_to_none_for_block_false_views() -> None:
    # Regression: under outage + block=False, request.throttlekit_decision must exist (as None),
    # not raise AttributeError in the exact case fail-open exists to survive.
    captured: dict[str, object] = {}

    @rate_limit(_Boom(), key=lambda r: "k", block=False)
    def view(request: object) -> HttpResponse:
        captured["decision"] = request.throttlekit_decision  # type: ignore[attr-defined]
        return HttpResponse("ran")

    resp = view(_req())
    assert resp.status_code == 200
    assert captured["decision"] is None
