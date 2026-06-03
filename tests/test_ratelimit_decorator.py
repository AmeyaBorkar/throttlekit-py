"""Unit tests for the ``rate_limit`` decorator + ``bind_policy`` — sync and async, no infra needed."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from throttlekit import (
    Decision,
    RateLimited,
    ThrottleKitError,
    bind_policy,
    rate_limit,
)

ALLOW = Decision(allowed=True, limit=10, remaining=9, reset_at=0, retry_after_ms=0)
DENY = Decision(allowed=False, limit=10, remaining=0, reset_at=1_000, retry_after_ms=500)


class _SyncChecker:
    def __init__(self, decision: Decision) -> None:
        self.decision = decision
        self.calls: list[tuple[str, int]] = []

    def __call__(self, key: str, *, cost: int = 1) -> Decision:
        self.calls.append((key, cost))
        return self.decision


class _AsyncChecker:
    def __init__(self, decision: Decision) -> None:
        self.decision = decision
        self.calls: list[tuple[str, int]] = []

    async def __call__(self, key: str, *, cost: int = 1) -> Decision:
        self.calls.append((key, cost))
        return self.decision


def test_sync_allows_passthrough() -> None:
    chk = _SyncChecker(ALLOW)

    @rate_limit(chk, key=lambda uid: uid)
    def handler(uid: str) -> str:
        return f"ok:{uid}"

    assert handler("u1") == "ok:u1"
    assert chk.calls == [("u1", 1)]


def test_sync_denies_raises_ratelimited_with_decision() -> None:
    chk = _SyncChecker(DENY)

    @rate_limit(chk, key=lambda uid: uid)
    def handler(uid: str) -> str:
        return "should not run"

    with pytest.raises(RateLimited) as ei:
        handler("u2")
    assert ei.value.decision is DENY
    assert ei.value.retry_after_ms == 500
    assert ei.value.reset_at == 1_000


def test_async_allows_passthrough() -> None:
    chk = _AsyncChecker(ALLOW)

    @rate_limit(chk, key=lambda uid: uid)
    async def handler(uid: str) -> str:
        return f"ok:{uid}"

    assert asyncio.run(handler("u3")) == "ok:u3"
    assert chk.calls == [("u3", 1)]


def test_async_denies_raises() -> None:
    chk = _AsyncChecker(DENY)

    @rate_limit(chk, key=lambda uid: uid)
    async def handler(uid: str) -> str:
        return "nope"

    with pytest.raises(RateLimited):
        asyncio.run(handler("u4"))


def test_async_function_with_sync_checker_offloads() -> None:
    # A sync checker paired with an async function is resolved off-loop, not rejected.
    chk = _SyncChecker(ALLOW)

    @rate_limit(chk, key=lambda uid: uid)
    async def handler(uid: str) -> str:
        return "ok"

    assert asyncio.run(handler("u5")) == "ok"
    assert chk.calls == [("u5", 1)]


def test_sync_function_with_async_checker_raises_typeerror() -> None:
    chk = _AsyncChecker(ALLOW)

    @rate_limit(chk, key=lambda uid: uid)
    def handler(uid: str) -> str:
        return "ok"

    with pytest.raises(TypeError):
        handler("u6")


def test_cost_is_passed_through() -> None:
    chk = _SyncChecker(ALLOW)

    @rate_limit(chk, key=lambda uid: uid, cost=5)
    def handler(uid: str) -> str:
        return "ok"

    handler("u7")
    assert chk.calls == [("u7", 5)]


def test_ratelimited_is_not_a_throttlekit_error() -> None:
    # A denial is the backend succeeding ("no") — it must not be swallowed by `except ThrottleKitError`.
    assert not issubclass(RateLimited, ThrottleKitError)


def test_bind_policy_sync_backend() -> None:
    class FakeBackend:
        def check(self, policy: str, key: str, cost: int = 1) -> Decision:
            assert policy == "api"
            return ALLOW

    checker = bind_policy(FakeBackend(), "api")
    assert not asyncio.iscoroutinefunction(checker)
    assert checker("k", cost=2).allowed


def test_bind_policy_async_backend_preserves_asyncness() -> None:
    class FakeAsyncBackend:
        async def check(self, policy: str, key: str, cost: int = 1) -> Decision:
            return DENY

    checker = bind_policy(FakeAsyncBackend(), "api")
    assert asyncio.iscoroutinefunction(checker)
    result: Any = asyncio.run(checker("k"))
    assert not result.allowed
