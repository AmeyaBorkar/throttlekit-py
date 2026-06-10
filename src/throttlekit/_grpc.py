"""Shared gRPC plumbing for the service-door backends: the generated-stub import guard + error mapping.

Kept in one place so the Fleet / Monitor clients (sync + async) map a gRPC status to a ThrottleKit
exception identically — a denial is never an error (it is a normal reply); these are operational faults.
"""

from __future__ import annotations

import grpc

from .errors import (
    OperationNotSupportedError,
    PolicyNotFoundError,
    ServiceUnavailableError,
    ThrottleKitError,
)

try:
    from ._generated import throttlekit_pb2 as pb
    from ._generated import throttlekit_pb2_grpc as pb_grpc
except ImportError as exc:  # pragma: no cover - exercised only when stubs are absent
    raise ImportError(
        "ThrottleKit gRPC stubs are not generated. Run `python scripts/gen_proto.py` "
        "(after `pip install -e .[dev]`) to generate them from the vendored contract."
    ) from exc


def map_rpc_error(err: grpc.RpcError) -> ThrottleKitError:
    """Map a gRPC status to the operational exception in :mod:`throttlekit.errors`."""
    code = err.code()
    details = err.details() or ""
    if code == grpc.StatusCode.NOT_FOUND:
        return PolicyNotFoundError(details)
    if code == grpc.StatusCode.UNIMPLEMENTED:
        return OperationNotSupportedError(details)
    if code == grpc.StatusCode.UNAVAILABLE:
        return ServiceUnavailableError(details)
    return ThrottleKitError(f"{code.name}: {details}")


__all__ = ["map_rpc_error", "pb", "pb_grpc"]
