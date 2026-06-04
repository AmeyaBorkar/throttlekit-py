"""Reach ThrottleKit from Python over the gRPC service door — across any server store backend.

The Python client is **store-agnostic**: this exact script works whether the `throttlekit-server` it
talks to is backed by in-process memory, Redis, Postgres, or DynamoDB. The server chooses the store; the
core computes every decision server-side, so decisions are bit-identical across backends.

Start a server (pick one backend), then run this script — `python examples/service_backend.py`:

    # in-process memory (single instance)
    npx throttlekit-server --config examples/policies.yaml --port 50051

    # Redis (shared fleet)
    npx throttlekit-server --config examples/policies.yaml --port 50051 --redis redis://localhost:6379

    # Postgres (no Redis needed)
    npx throttlekit-server --config examples/policies.yaml --port 50051 \
        --postgres-url postgres://user:pass@localhost:5432/app

    # DynamoDB (provisions the single-pk table on first run)
    npx throttlekit-server --config examples/policies.yaml --port 50051 \
        --store dynamodb --dynamodb-table throttlekit --dynamodb-create-table

`throttlekit-server` is the Node package (`npm i -g throttlekit-server`, or `npx throttlekit-server`).
"""

from __future__ import annotations

import os

from throttlekit import ServiceBackend

ADDR = os.environ.get("THROTTLEKIT_ADDR", "localhost:50051")


def main() -> None:
    with ServiceBackend(ADDR) as rl:
        # Rate — the base axis. A denial is a normal Decision (allowed=False), never an exception.
        print("== rate (check) ==")
        for i in range(7):
            d = rl.check("api", "user-1")
            print(
                f"  check #{i + 1}: allowed={d.allowed} remaining={d.remaining} "
                f"retry_after_ms={d.retry_after_ms}"
            )

        # Cost — debit the tokens a unit of work actually spends (the LLM-gateway problem).
        print("== cost (debit) ==")
        spent = rl.debit("completions", "tenant-1", tokens=3)
        print(f"  debit 3: allowed={spent.allowed} remaining={spent.remaining}")

        # Concurrency — hold an in-flight slot for the duration of the work; released on context exit.
        print("== concurrency (admit) ==")
        with rl.admit("checkout", "user-1") as adm:
            print(f"  admit: allowed={adm.allowed} binding_axis={adm.binding_axis}")
            if adm.allowed:
                ...  # do the protected work here


if __name__ == "__main__":
    main()
