"""Flask integration: a ``ThrottleKit`` extension whose ``@tk.limit()`` decorates routes.

Shaped like ``flask-limiter``: construct the extension (optionally bound to an app via ``init_app``) with a
default :class:`~throttlekit.ratelimit.Checker`, then decorate views with ``@tk.limit()``. On a denial the
view is short-circuited with a **429** carrying ``Retry-After`` / ``RateLimit-*`` headers; on an admission
those headers are stamped onto the view's response.

    from flask import Flask
    from throttlekit import ServiceBackend, bind_policy
    from throttlekit.contrib.flask import ThrottleKit

    app = Flask(__name__)
    backend = ServiceBackend("localhost:50051")
    tk = ThrottleKit(app, checker=bind_policy(backend, "api"))

    @app.get("/items")
    @tk.limit()
    def items(): ...
"""

from __future__ import annotations

import functools
from collections.abc import Callable
from typing import Any, TypeVar, cast

from flask import abort, make_response, request

from ..errors import ServiceUnavailableError
from ..headers import Style, decision_headers
from ..ratelimit import Checker, OnUnavailable, _resolve_sync

__all__ = ["ThrottleKit"]

F = TypeVar("F", bound=Callable[..., Any])


def _key_remote_addr() -> str:
    """Default key: the raw ``request.remote_addr`` (never a forgeable ``X-Forwarded-For``)."""
    return str(request.remote_addr or "anonymous")


class ThrottleKit:
    """Flask extension. Configure a default ``checker`` (and key/cost/style), then ``@tk.limit()`` views.

    :param app: bind immediately (or call :meth:`init_app` later).
    :param checker: the default :class:`~throttlekit.ratelimit.Checker` for ``@limit()`` (overridable per
        route).
    :param key: default key function (no args; reads the Flask request context). Default: peer IP.
    :param default_cost: units consumed per request (default 1).
    :param style: header style — ``"ietf"`` (default), ``"legacy"``, or ``"both"``.
    :param headers_enabled: stamp ``RateLimit-*`` onto responses (default True).
    :param on_unavailable: behaviour when the backend is unreachable — ``"allow"`` (fail open, default)
        or ``"deny"`` (aborts 503).
    """

    def __init__(
        self,
        app: Any = None,
        *,
        checker: Checker | None = None,
        key: Callable[[], str] = _key_remote_addr,
        default_cost: int = 1,
        style: Style = "ietf",
        headers_enabled: bool = True,
        on_unavailable: OnUnavailable = "allow",
    ) -> None:
        self.checker = checker
        self.key = key
        self.default_cost = default_cost
        self.style = style
        self.headers_enabled = headers_enabled
        self.on_unavailable = on_unavailable
        if app is not None:
            self.init_app(app)

    def init_app(self, app: Any) -> None:
        """Register the extension on ``app`` (Flask's ``app.extensions['throttlekit']``)."""
        if not hasattr(app, "extensions"):
            app.extensions = {}
        app.extensions["throttlekit"] = self

    def limit(
        self,
        *,
        checker: Checker | None = None,
        key: Callable[[], str] | None = None,
        cost: int | None = None,
    ) -> Callable[[F], F]:
        """Decorate a view; ``checker`` / ``key`` / ``cost`` override the extension defaults for this route."""

        def decorate(view: F) -> F:
            @functools.wraps(view)
            def wrapped(*args: Any, **kwargs: Any) -> Any:
                resolved_checker = checker if checker is not None else self.checker
                if resolved_checker is None:
                    raise RuntimeError(
                        "throttlekit: no checker configured — pass checker= to ThrottleKit(...) "
                        "or to @tk.limit(...)"
                    )
                resolved_key = key if key is not None else self.key
                resolved_cost = self.default_cost if cost is None else cost
                try:
                    decision = _resolve_sync(resolved_checker, resolved_key(), resolved_cost)
                except ServiceUnavailableError:
                    if self.on_unavailable == "deny":
                        abort(503)
                    return view(*args, **kwargs)  # fail open

                if not decision.allowed:
                    response = make_response(("rate limit exceeded", 429))
                else:
                    response = make_response(view(*args, **kwargs))
                if self.headers_enabled:
                    response.headers.update(decision_headers(decision, self.style))
                return response

            return cast("F", wrapped)

        return decorate
