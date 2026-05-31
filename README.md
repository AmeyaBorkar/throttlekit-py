# throttlekit (Python)

Python client for [**ThrottleKit**](https://www.npmjs.com/package/throttlekit) — distributed rate
limiting against the **one** Node core, reached through either of two pluggable backends and proven
against the **same** golden vectors:

| Backend | Path | Decision computed in | Use it when |
|---|---|---|---|
| `ServiceBackend` | gRPC → [`throttlekit-server`](https://github.com/AmeyaBorkar/throttlekit/tree/main/server) | the service (= the core) | you want the full surface (`check`/`check_many`/`peek`/`forecast`) and to never touch the raw wire |
| `RedisBackend` | vendored Lua → the **same Redis** a Node fleet uses | Lua-in-Redis (the core's own script) | you already run Redis and want one hop, no extra service — `check` only |

> **Status: experimental (alpha).** The contract (`throttlekit.proto`, the golden vectors, and the
> extracted Lua) is vendored and checksum-pinned from the frozen `throttlekit` 1.0 core; this client
> tracks it. The raw Lua wire is **not** a frozen contract yet (it ships `frozen: false`), so the
> `RedisBackend` is explicitly experimental and may change with the core's scripts.

## The one invariant

The whole ThrottleKit design rests on it: **exactly one thing computes a `Decision`** — the Node core,
directly or as Lua-in-Redis. Neither backend re-implements an algorithm, so there is no second rate
limiter to keep in sync and no float-determinism risk. The `RedisBackend` marshals ARGV, runs the
core's vendored script, and decodes the reply; the decision is produced **server-side, in Lua**.

## Install

```bash
pip install throttlekit            # (alpha; not yet published) — the gRPC ServiceBackend
pip install throttlekit[redis]     # + a redis client for the direct RedisBackend
```

## Use — the service door

```python
from throttlekit import ServiceBackend

with ServiceBackend("localhost:50051") as rl:
    d = rl.check("api", api_key)
    if not d.allowed:
        ...  # 429 — retry after d.retry_after_ms
```

`check` / `check_many` / `peek` / `forecast` return frozen `Decision` / `Forecast` dataclasses. A
*denial* is a normal `Decision` (`allowed is False`), never an exception; gRPC faults map to
`PolicyNotFoundError` / `OperationNotSupportedError` / `ServiceUnavailableError`.

## Use — the direct Redis door

Configure a strategy and point it at the Redis your fleet shares. `check` is the whole surface (it is
the contract-vectored, Lua-computed decision); `peek` / `forecast` deliberately stay on the service
door, where the core — not a re-derived client port — computes them.

```python
import redis
from throttlekit import RedisBackend, Gcra

client = redis.Redis.from_url("redis://localhost:6379")
api = RedisBackend(client, Gcra(limit=100, period_ms=60_000, burst=20), prefix="prod")

d = api.check(api_key)             # now defaults to the Redis server clock (skew-free across a fleet)
if not d.allowed:
    ...                            # 429 — retry after d.retry_after_ms
```

Strategies: `Gcra`, `TokenBucket`, `FixedWindow`, `SlidingWindow`, `SlidingWindowLog`. The `prefix`
joins as `f"{prefix}:{key}"` — the **same** key scheme the core uses, so a Python and a Node client on
one limit address the same Redis key. The backend is client-agnostic: pass any object with `evalsha` /
`eval` (`redis-py` satisfies it structurally), exactly as the Node `RedisStore` does.

## How this stays in lock-step with the core

`scripts/sync_contract.py` vendors, with checksums, from the core repo:

* `contract/` — the dev/test artifacts: `throttlekit.proto` (→ gRPC stubs) and `golden-vectors.json`.
* `src/throttlekit/_scripts/` — the **runtime** Lua the `RedisBackend` executes (shipped in the wheel),
  with the core's `manifest.json` (which carries each script's sha256).

`tests/test_contract.py` is the **drift-gate** (the vendored bytes must match their checksums and the
pinned `contractVersion`). But the real proof is behavioral:

* **`tests/test_redis_backend.py`** replays **every** rate-limit golden vector — the full,
  time-parametrized timeline — through the Python client → vendored Lua → **real Redis**, and asserts
  every reply field equals the Node oracle bit-for-bit. (Because the direct path puts an explicit `now`
  in ARGV, it can do the rigorous time-parametrized replay the cross-process service door can't.)
* `tests/test_service_backend.py` starts a real `throttlekit-server` and asserts the clock-independent
  behavior over gRPC.

## Develop

```bash
pip install -e .[dev]
python scripts/sync_contract.py          # vendor proto + vectors + Lua from ../GreenfeildProject (the core)
python scripts/gen_proto.py              # generate the gRPC stubs from the vendored proto
pytest                                   # unit + contract; the Redis/service tests skip if their backend is absent
ruff check . && mypy                     # lint + types
```

The `RedisBackend` conformance needs a reachable Redis: it uses `THROTTLEKIT_REDIS_URL` or the project
default `redis://localhost:6380`, and skips cleanly when neither is up.
