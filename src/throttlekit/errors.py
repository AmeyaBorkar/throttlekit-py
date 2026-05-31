"""Exceptions raised by the ThrottleKit client. grpc-free so they can be imported without the stubs."""

from __future__ import annotations


class ThrottleKitError(Exception):
    """Base class for all ThrottleKit client errors."""


class PolicyNotFoundError(ThrottleKitError):
    """The service was not configured with the requested policy (gRPC ``NOT_FOUND``)."""


class OperationNotSupportedError(ThrottleKitError):
    """The policy's strategy does not support a non-consuming op (gRPC ``UNIMPLEMENTED``)."""


class ServiceUnavailableError(ThrottleKitError):
    """The service could not be reached (gRPC ``UNAVAILABLE``). The caller settles fail-open/closed."""
