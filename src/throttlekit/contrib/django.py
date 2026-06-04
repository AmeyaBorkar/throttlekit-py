"""Django integration: a ``@rate_limit`` view decorator + a middleware that renders denials as 429.

Mirrors the shape Django users already know from ``django-ratelimit``: decorate a view, and on a denial
either **raise** :class:`Ratelimited` (``block=True``, the default) or annotate the request and let the
view decide (``block=False`` sets ``request.throttlekit_decision``). :class:`Ratelimited` subclasses
``PermissionDenied`` (so without any middleware it yields Django's 403); install
:class:`ThrottleKitMiddleware` to map it to a **429** with ``Retry-After`` / ``RateLimit-*`` headers.

    from throttlekit import ServiceBackend, bind_policy
    from throttlekit.contrib.django import rate_limit

    backend = ServiceBackend("localhost:50051")

    @rate_limit(bind_policy(backend, "api"))
    def my_view(request): ...
"""

from __future__ import annotations

import functools
from collections.abc import Callable
from typing import Any, TypeVar, cast

from django.conf import settings
from django.core.exceptions import PermissionDenied
from django.http import HttpResponse

from ..decision import Decision
from ..errors import ServiceUnavailableError
from ..headers import Style, decision_headers
from ..ratelimit import Checker, OnUnavailable, _resolve_sync

__all__ = ["Ratelimited", "rate_limit", "ThrottleKitMiddleware"]

F = TypeVar("F", bound=Callable[..., Any])


class Ratelimited(PermissionDenied):  # type: ignore[misc]  # base is Any under ignore_missing_imports
    """Raised by ``rate_limit(block=True)`` on a denial. Carries the :class:`~throttlekit.Decision`.

    Subclasses ``PermissionDenied`` (→ 403 by default), so :class:`ThrottleKitMiddleware` can upgrade it
    to a 429 without affecting the rest of your exception handling.
    """

    def __init__(self, decision: Decision) -> None:
        self.decision = decision
        super().__init__("rate limit exceeded")


def _key_remote_addr(request: Any) -> str:
    """Default key: the raw ``REMOTE_ADDR`` (never a forgeable ``X-Forwarded-For``)."""
    return str(request.META.get("REMOTE_ADDR", "anonymous"))


def rate_limit(
    checker: Checker,
    *,
    key: Callable[[Any], str] = _key_remote_addr,
    cost: int = 1,
    block: bool = True,
    style: Style = "ietf",
    on_unavailable: OnUnavailable = "allow",
    stamp_headers: bool = True,
) -> Callable[[F], F]:
    """Decorate a (sync) Django view so each request is admitted by ``checker`` first.

    :param block: ``True`` (default) raises :class:`Ratelimited` on a denial; ``False`` sets
        ``request.throttlekit_decision`` and runs the view (which decides what to do). Under a backend
        outage with ``on_unavailable="allow"``, ``request.throttlekit_decision`` is set to ``None``.
    :param on_unavailable: behaviour when the backend is unreachable — ``"allow"`` (fail open, default)
        or ``"deny"`` (returns a 503).
    :param stamp_headers: stamp ``RateLimit-*`` onto the view's response (default True).
    """

    def decorate(view: F) -> F:
        @functools.wraps(view)
        def wrapped(request: Any, *args: Any, **kwargs: Any) -> Any:
            try:
                decision = _resolve_sync(checker, key(request), cost)
            except ServiceUnavailableError:
                if on_unavailable == "deny":
                    return HttpResponse("rate limiter unavailable", status=503)
                # Fail open: no decision is available under a backend outage. Still set the attribute
                # (to None) so a block=False view that reads request.throttlekit_decision doesn't raise
                # AttributeError in exactly the outage case fail-open exists to survive.
                request.throttlekit_decision = None
                return view(request, *args, **kwargs)

            request.throttlekit_decision = decision
            if not decision.allowed and block:
                raise Ratelimited(decision)
            response = view(request, *args, **kwargs)
            if stamp_headers:
                for header, value in decision_headers(decision, style).items():
                    response.headers[header] = value
            return response

        return cast("F", wrapped)

    return decorate


class ThrottleKitMiddleware:
    """Maps a :class:`Ratelimited` raised by a view into a 429 response with the rate-limit headers.

    Add ``"throttlekit.contrib.django.ThrottleKitMiddleware"`` to ``MIDDLEWARE``. The header style is read
    from ``settings.THROTTLEKIT_HEADER_STYLE`` (default ``"ietf"``).
    """

    def __init__(self, get_response: Callable[[Any], Any]) -> None:
        self.get_response = get_response

    def __call__(self, request: Any) -> Any:
        return self.get_response(request)

    def process_exception(self, request: Any, exception: Exception) -> Any:
        if isinstance(exception, Ratelimited):
            style: Style = getattr(settings, "THROTTLEKIT_HEADER_STYLE", "ietf")
            return HttpResponse(
                "rate limit exceeded",
                status=429,
                headers=decision_headers(exception.decision, style),
            )
        return None
