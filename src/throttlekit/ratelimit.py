"""Framework-agnostic rate-limit ergonomics: the ``rate_limit`` decorator, the :class:`RateLimited`
signal, and the small ``Checker`` plumbing every ``contrib`` adapter shares.

A **checker** is the one thing these helpers need: a callable ``key -> Decision`` (or an awaitable of one).
It is what turns the two backends — whose ``check`` signatures differ — into a single uniform shape:

* a :class:`~throttlekit.ServiceBackend` / :class:`~throttlekit.AsyncServiceBackend` is bound to a policy
  with :func:`bind_policy` — ``bind_policy(backend, "api")`` ⇒ ``key -> backend.check("api", key, cost)``;
* a :class:`~throttlekit.RedisBackend` / :class:`~throttlekit.AsyncRedisBackend` is already
  ``key -> Decision``, so its bound method ``backend.check`` *is* a checker — pass it directly.

Why a checker and not a ``(backend, policy=None)`` pair: a sentinel would leak which backend you hold into
every call site and would be a lie for the Redis door (it has no policy axis). A pre-bound callable keeps
the call sites uniform and honest.
"""

from __future__ import annotations

import asyncio
import functools
import inspect
from collections.abc import Awaitable, Callable
from typing import Any, Literal, Protocol, TypeVar, cast

from .decision import Decision

__all__ = ["RateLimited", "Checker", "OnUnavailable", "bind_policy", "rate_limit"]

F = TypeVar("F", bound=Callable[..., Any])

#: What an adapter does when the *backend itself* is unreachable (a ``ServiceUnavailableError``): fail
#: open (admit — a rate limiter is an availability guard, not an auth gate) or fail closed (deny).
OnUnavailable = Literal["allow", "deny"]


class Checker(Protocol):
    """A pre-bound limit: map a ``key`` to a :class:`Decision` (sync) or an awaitable of one (async)."""

    def __call__(self, key: str, *, cost: int = 1) -> Decision | Awaitable[Decision]: ...


class _PolicyBackend(Protocol):
    """A backend whose ``check`` takes a policy first — the (Async)ServiceBackend shape."""

    def check(self, policy: str, key: str, cost: int = 1) -> Decision | Awaitable[Decision]: ...


class RateLimited(Exception):
    """Raised by :func:`rate_limit` (and the ``block=True`` adapters) when a decision denies the request.

    This is **not** a :class:`~throttlekit.ThrottleKitError`: a denial is the backend succeeding (it said
    "no"), the opposite of an operational fault — so ``except ThrottleKitError`` must not swallow it. It
    carries the full :class:`Decision`, so a handler can render ``Retry-After`` / ``RateLimit-*`` headers
    (see :func:`throttlekit.decision_headers`).
    """

    def __init__(self, decision: Decision) -> None:
        self.decision = decision
        super().__init__(f"rate limit exceeded (retry after {decision.retry_after_ms} ms)")

    @property
    def retry_after_ms(self) -> int:
        return self.decision.retry_after_ms

    @property
    def reset_at(self) -> int:
        return self.decision.reset_at


def _is_async_callable(obj: object) -> bool:
    """True for a coroutine function OR a callable instance whose ``__call__`` is one.

    ``asyncio.iscoroutinefunction`` alone is False for a callable *object* with an ``async def __call__``
    (it only recognises plain functions / bound methods), so an async checker written as a class would be
    misrouted to the blocking path. This closes that gap.
    """
    if asyncio.iscoroutinefunction(obj):
        return True
    # Fetch __call__ to inspect ITS coroutine-ness (not a callability test — so B004 doesn't apply).
    return asyncio.iscoroutinefunction(getattr(obj, "__call__", None))  # noqa: B004


def bind_policy(backend: _PolicyBackend, policy: str) -> Checker:
    """Bind a (sync or async) ``ServiceBackend`` to one ``policy``, yielding a :class:`Checker`.

    The returned checker preserves the backend's sync/async nature (so the loop-safety logic in the async
    adapters can tell them apart) — an async backend yields a coroutine function, a sync one a plain one.
    """
    check = backend.check
    if _is_async_callable(check):

        async def _async_checker(key: str, *, cost: int = 1) -> Decision:
            return await cast("Awaitable[Decision]", check(policy, key, cost))

        return _async_checker

    def _sync_checker(key: str, *, cost: int = 1) -> Decision:
        return cast("Decision", check(policy, key, cost))

    return _sync_checker


def _resolve_sync(checker: Checker, key: str, cost: int) -> Decision:
    """Call a checker from synchronous code; reject an async checker with a clear error."""
    outcome = checker(key, cost=cost)
    if inspect.isawaitable(outcome):
        close = getattr(outcome, "close", None)  # avoid a "coroutine was never awaited" warning
        if close is not None:
            close()
        raise TypeError(
            "a coroutine was returned to a synchronous call site — pass a *sync* checker "
            "(e.g. bind_policy(sync_backend, policy) or a RedisBackend.check) to a sync "
            "function or adapter, or use the async path."
        )
    # `outcome` is narrowed to Decision here (the awaitable case raised above).
    return outcome


async def _resolve_async(checker: Checker, key: str, cost: int) -> Decision:
    """Call a checker from async code without ever blocking the event loop.

    Branches on the *callable* (not the result): an async checker is awaited directly; a sync checker is
    run in a worker thread, so a blocking network ``check`` never stalls the loop.
    """
    if _is_async_callable(checker):
        return await cast("Awaitable[Decision]", checker(key, cost=cost))
    return await asyncio.to_thread(_resolve_sync, checker, key, cost)


def rate_limit(checker: Checker, *, key: Callable[..., str], cost: int = 1) -> Callable[[F], F]:
    """Decorate a function so each call is admitted by ``checker`` before the body runs.

    Wraps **either** a sync or an ``async def`` function (detected automatically). ``key`` derives the
    limit key from the call's own arguments (e.g. ``key=lambda req: req.client_ip``). On a denial it
    raises :class:`RateLimited` carrying the :class:`Decision`; on admission the wrapped function runs
    normally. ``cost`` is the per-call weight.

        @rate_limit(bind_policy(backend, "api"), key=lambda user_id: user_id)
        def handle(user_id): ...
    """

    def decorate(fn: F) -> F:
        if _is_async_callable(fn):

            @functools.wraps(fn)
            async def awrapper(*args: Any, **kwargs: Any) -> Any:
                decision = await _resolve_async(checker, key(*args, **kwargs), cost)
                if not decision.allowed:
                    raise RateLimited(decision)
                return await fn(*args, **kwargs)

            return cast("F", awrapper)

        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            decision = _resolve_sync(checker, key(*args, **kwargs), cost)
            if not decision.allowed:
                raise RateLimited(decision)
            return fn(*args, **kwargs)

        return cast("F", wrapper)

    return decorate
