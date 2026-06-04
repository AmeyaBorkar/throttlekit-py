"""Starlette (and Bare-ASGI) integration: a middleware that rate-limits every HTTP request.

``ThrottleKitMiddleware`` is a **pure ASGI** middleware (no ``BaseHTTPMiddleware`` — that wraps the body
in a way that interferes with streaming): it derives a key from the connection, asks a
:class:`~throttlekit.ratelimit.Checker`, and on a denial **returns** a 429 (it never raises), stamping
``Retry-After`` / ``RateLimit-*`` headers. On an admission it stamps the same ``RateLimit-*`` headers onto
the downstream response. Because FastAPI *is* Starlette, this middleware also works on a FastAPI app
(``app.add_middleware(ThrottleKitMiddleware, checker=...)``); FastAPI additionally offers a per-route
dependency in :mod:`throttlekit.contrib.fastapi`.

    from throttlekit import ServiceBackend, bind_policy
    from throttlekit.contrib.starlette import ThrottleKitMiddleware

    backend = ServiceBackend("localhost:50051")
    app.add_middleware(ThrottleKitMiddleware, checker=bind_policy(backend, "api"))
"""

from __future__ import annotations

from collections.abc import Callable

from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from ..errors import ServiceUnavailableError
from ..headers import Style, decision_headers
from ..ratelimit import Checker, OnUnavailable, _resolve_async


def _key_scope_peer(scope: Scope) -> str:
    """Default key: the raw connecting peer IP (never a forgeable ``X-Forwarded-For``)."""
    client = scope.get("client")
    return str(client[0]) if client else "anonymous"


def _with_headers(message: Message, extra: dict[str, str]) -> Message:
    raw = list(message.get("headers", []))
    raw.extend((k.encode("latin-1"), v.encode("latin-1")) for k, v in extra.items())
    return {**message, "headers": raw}


class ThrottleKitMiddleware:
    """Pure-ASGI rate-limit middleware. See the module docstring.

    :param checker: a :class:`~throttlekit.ratelimit.Checker` (``bind_policy(backend, policy)`` for the
        service door, or a ``RedisBackend.check`` bound method for the direct door).
    :param key: maps the ASGI ``scope`` to a limit key (default: the connecting peer IP).
    :param cost: units to consume per request (default 1).
    :param style: header style — ``"ietf"`` (default), ``"legacy"``, or ``"both"``.
    :param on_unavailable: behaviour when the backend itself is unreachable — ``"allow"`` (fail open,
        default) or ``"deny"`` (fail closed, returns 503).
    """

    def __init__(
        self,
        app: ASGIApp,
        *,
        checker: Checker,
        key: Callable[[Scope], str] = _key_scope_peer,
        cost: int = 1,
        style: Style = "ietf",
        on_unavailable: OnUnavailable = "allow",
    ) -> None:
        self.app = app
        self.checker = checker
        self.key = key
        self.cost = cost
        self.style = style
        self.on_unavailable = on_unavailable

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        try:
            decision = await _resolve_async(self.checker, self.key(scope), self.cost)
        except ServiceUnavailableError:
            if self.on_unavailable == "deny":
                await JSONResponse({"detail": "rate limiter unavailable"}, status_code=503)(
                    scope, receive, send
                )
                return
            await self.app(scope, receive, send)  # fail open
            return

        if not decision.allowed:
            await JSONResponse(
                {"detail": "rate limit exceeded"},
                status_code=429,
                headers=decision_headers(decision, self.style),
            )(scope, receive, send)
            return

        # Admitted: stamp RateLimit-* onto the downstream response's start message.
        extra = decision_headers(decision, self.style)

        async def send_wrapper(message: Message) -> None:
            if message["type"] == "http.response.start":
                message = _with_headers(message, extra)
            await send(message)

        await self.app(scope, receive, send_wrapper)
