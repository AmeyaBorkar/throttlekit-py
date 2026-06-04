"""Reach ThrottleKit over the async gRPC service door — `AsyncServiceBackend` (the `grpc.aio` twin).

Same one-oracle guarantee as the sync client: the Node core (inside the service) computes every decision;
this client `await`s the transport and never re-derives one. The win is concurrency — fire many checks at
once without blocking the event loop.

Start a server (any store backend works — see service_backend.py), then run this script:

    npx throttlekit-server --config examples/policies.yaml --port 50051
    python examples/async_service_backend.py
"""

from __future__ import annotations

import asyncio
import os

from throttlekit import AsyncServiceBackend

ADDR = os.environ.get("THROTTLEKIT_ADDR", "localhost:50051")


async def main() -> None:
    async with AsyncServiceBackend(ADDR) as rl:
        # Rate — the base axis, awaited. A denial is a normal Decision (allowed=False), never an exception.
        print("== rate (await check) ==")
        d = await rl.check("api", "user-1")
        print(f"  check: allowed={d.allowed} remaining={d.remaining}")

        # The async win: drive many keys at one instant without blocking the loop.
        print("== many keys concurrently (asyncio.gather) ==")
        keys = [f"user-{i}" for i in range(5)]
        decisions = await asyncio.gather(*(rl.check("api", k) for k in keys))
        for k, decision in zip(keys, decisions, strict=True):
            print(f"  {k}: allowed={decision.allowed} remaining={decision.remaining}")

        # Concurrency — the admission axis as an *async* context manager (the slot is released on exit).
        print("== concurrency (async with admit) ==")
        adm = await rl.admit("checkout", "user-1")
        async with adm:
            print(f"  admit: allowed={adm.allowed} binding_axis={adm.binding_axis!r}")
            if adm.allowed:
                await asyncio.sleep(0)  # the protected work happens here


if __name__ == "__main__":
    asyncio.run(main())
