"""FastAPI integration: a per-route ``Depends()`` rate limiter (plus the Starlette middleware, re-exported).

The idiomatic FastAPI tool is a **dependency**: attach ``Depends(RateLimit(...))`` to a route (or a router,
or the whole app via ``dependencies=[...]``). On a denial it raises ``HTTPException(429)`` with
``Retry-After`` / ``RateLimit-*`` headers — exactly how FastAPI expects an endpoint to reject — and on an
admission it stamps the ``RateLimit-*`` headers onto the response. For global, pre-router limiting use the
pure-ASGI :class:`~throttlekit.contrib.starlette.ThrottleKitMiddleware` (re-exported here), since FastAPI
*is* Starlette.

    from fastapi import FastAPI, Depends
    from throttlekit import ServiceBackend, bind_policy
    from throttlekit.contrib.fastapi import RateLimit

    app = FastAPI()
    backend = ServiceBackend("localhost:50051")
    limit_api = RateLimit(bind_policy(backend, "api"))

    @app.get("/items", dependencies=[Depends(limit_api)])
    async def items(): ...
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from fastapi import HTTPException, Request, Response

from ..errors import ServiceUnavailableError
from ..headers import Style, decision_headers
from ..ratelimit import Checker, OnUnavailable, _resolve_async

# Re-export so `from throttlekit.contrib.fastapi import ThrottleKitMiddleware` works (FastAPI is Starlette).
from .starlette import ThrottleKitMiddleware

__all__ = ["RateLimit", "ThrottleKitMiddleware"]


def _key_request_peer(request: Request) -> str:
    """Default key: the raw connecting peer IP (never a forgeable ``X-Forwarded-For``)."""
    client = request.client
    return str(client.host) if client else "anonymous"


def RateLimit(
    checker: Checker,
    *,
    key: Callable[[Request], str] = _key_request_peer,
    cost: int = 1,
    style: Style = "ietf",
    on_unavailable: OnUnavailable = "allow",
    stamp_headers: bool = True,
) -> Callable[[Request, Response], Awaitable[None]]:
    """Build a FastAPI dependency that admits (or rejects) the request before the route runs.

    :param checker: a :class:`~throttlekit.ratelimit.Checker` (``bind_policy(backend, policy)`` for the
        service door, or a ``RedisBackend.check`` bound method for the direct door).
    :param key: maps the request to a limit key (default: the connecting peer IP).
    :param cost: units to consume per request (default 1).
    :param style: header style — ``"ietf"`` (default), ``"legacy"``, or ``"both"``.
    :param on_unavailable: behaviour when the backend is unreachable — ``"allow"`` (fail open, default)
        or ``"deny"`` (fail closed, raises ``HTTPException(503)``).
    :param stamp_headers: stamp ``RateLimit-*`` onto the (admitted) response (default True). Note: this
        works for a response FastAPI **builds from your return value** (a dict / model / etc.); if your
        endpoint returns its **own** ``Response`` object, FastAPI uses it verbatim and the dependency's
        header mutations are lost — use :class:`ThrottleKitMiddleware` for unconditional stamping.
    :returns: an ``async`` dependency callable; pass it to ``Depends(...)``.
    """

    async def dependency(request: Request, response: Response) -> None:
        try:
            decision = await _resolve_async(checker, key(request), cost)
        except ServiceUnavailableError:
            if on_unavailable == "deny":
                raise HTTPException(status_code=503, detail="rate limiter unavailable") from None
            return  # fail open
        if not decision.allowed:
            raise HTTPException(
                status_code=429,
                detail="rate limit exceeded",
                headers=decision_headers(decision, style),
            )
        if stamp_headers:
            response.headers.update(decision_headers(decision, style))

    return dependency
