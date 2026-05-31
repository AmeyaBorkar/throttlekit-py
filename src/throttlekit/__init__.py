"""ThrottleKit — Python client for distributed rate limiting via the gRPC service door.

``import throttlekit`` is dependency-light (no gRPC import). :class:`ServiceBackend` is imported lazily
on first access, so the contract drift-gate and the domain types are usable without the generated stubs.

    from throttlekit import ServiceBackend
    with ServiceBackend("localhost:50051") as rl:
        d = rl.check("api", api_key)
        if not d.allowed:
            ...  # 429; retry after d.retry_after_ms
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from ._version import __version__
from .decision import Decision, Forecast
from .errors import (
    OperationNotSupportedError,
    PolicyNotFoundError,
    ServiceUnavailableError,
    ThrottleKitError,
)

if TYPE_CHECKING:
    from .service_backend import ServiceBackend

__all__ = [
    "ServiceBackend",
    "Decision",
    "Forecast",
    "ThrottleKitError",
    "PolicyNotFoundError",
    "OperationNotSupportedError",
    "ServiceUnavailableError",
    "__version__",
]


def __getattr__(name: str) -> object:
    """Lazily import :class:`ServiceBackend` so ``import throttlekit`` doesn't require grpc/stubs."""
    if name == "ServiceBackend":
        from .service_backend import ServiceBackend

        return ServiceBackend
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
