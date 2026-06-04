"""The direct Redis door — `RedisBackend` runs the core's vendored Lua against the SAME Redis a Node fleet
uses, so its decisions are bit-identical to an embedded Node library. No server, one hop.

`check` is the whole surface (it is the contract-vectored, Lua-computed decision); `peek` / `forecast`
deliberately stay on the service door, where the core — not a re-derived client port — computes them.

    pip install "throttlekit-py[redis]"
    docker run -d -p 6379:6379 redis:7-alpine    # or set THROTTLEKIT_REDIS_URL
    python examples/redis_backend.py
"""

from __future__ import annotations

import os

import redis

from throttlekit import Gcra, RedisBackend

URL = os.environ.get("THROTTLEKIT_REDIS_URL", "redis://localhost:6379")


def main() -> None:
    client = redis.Redis.from_url(URL)

    # GCRA: 5 cells of burst headroom, draining one cell per (period_ms / limit). The `prefix` is the SAME
    # key scheme the core uses, so a Python and a Node client on this limit address the same Redis key.
    api = RedisBackend(client, Gcra(limit=5, period_ms=3_600_000, burst=5), prefix="demo")

    print(f"== direct RedisBackend (gcra: 5 burst / 1h) against {URL} ==")
    for i in range(7):
        # now=None (the default) sends the server-time sentinel, so the script uses the Redis server clock
        # — skew-free across a fleet. Pass an explicit `now` only for deterministic replay.
        d = api.check("user-1")
        print(
            f"  check #{i + 1}: allowed={d.allowed} remaining={d.remaining} "
            f"retry_after_ms={d.retry_after_ms}"
        )

    client.close()


if __name__ == "__main__":
    main()
