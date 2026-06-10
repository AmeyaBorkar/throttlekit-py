# throttlekit (Python)

**Beyond rate limiting — from Python.** Govern **rate, concurrency, and cost**, *provably*. This is
[**ThrottleKit**](https://www.npmjs.com/package/throttlekit)'s Python client, and it re-implements nothing:
every decision comes from the **one** Node core and its two engines — **GALE** (provable distributed leasing,
a fleet-size-independent overshoot bound machine-checked in TLA⁺) and **TALE** (token-budget escrow — meter
what your LLM *spends* as it streams) — **bit-identical** to the Node oracle, through either of two pluggable
backends:

| Backend | Path | Decision computed in | Use it when |
|---|---|---|---|
| `ServiceBackend` | gRPC → [`throttlekit-server`](https://github.com/AmeyaBorkar/throttlekit/tree/main/server) | the service (= the core) | you want the full surface (`check`/`check_many`/`peek`/`forecast`) and to never touch the raw wire |
| `RedisBackend` | vendored Lua → the **same Redis** a Node fleet uses | Lua-in-Redis (the core's own script) | you already run Redis and want one hop, no extra service — `check` only |

Each door has an `asyncio` twin — **`AsyncServiceBackend`** and **`AsyncRedisBackend`** — and there are
batteries-included **FastAPI / Starlette / Django / Flask** integrations under `throttlekit.contrib`
(see below).

**New in 0.5.0 — the fleet reaches Python.** Point the same `ServiceBackend` at a server whose policy is
configured **distributed** (`federated:` / `fleetBudget:` / `distributedConcurrency:` / `federatedFairEscrow:`)
and every decision is **coordinated across the whole fleet** — with *no client change*. For the
highest-throughput path, the new **`FleetBackend`** leases a chunk of a global budget and spends it locally
(one round trip per *batch*, not per request); and the read-only **`MonitorBackend`** reads the server's live
operational state — [Fleet & Monitor clients](https://github.com/AmeyaBorkar/throttlekit-py/wiki/Fleet-and-Monitor).

The `ServiceBackend` is **store-agnostic**: the `throttlekit-server` it points at can be backed by an
in-process **memory** store, a shared **Redis**, **Postgres** (`--store postgres`), or **DynamoDB**
(`--store dynamodb`) — no Redis required for the latter two — and the client sends the same requests with
the decision still the core's regardless. (Deno KV and Cloudflare D1 / Durable Objects / Workers KV are
*edge-runtime* stores: they run only inside those runtimes, not behind the Node service door.)

> **Status: experimental (alpha).** The contract (`throttlekit.proto`, the golden vectors, and the
> extracted Lua) is vendored and checksum-pinned from the frozen `throttlekit` 1.0 core; this client
> tracks it. The raw Lua wire is **not** a frozen contract yet (it ships `frozen: false`), so the
> `RedisBackend` is explicitly experimental and may change with the core's scripts.

🌐 **[throttlekit.in](https://throttlekit.in)** · 📖 **Full guide:** the [**wiki**](https://github.com/AmeyaBorkar/throttlekit-py/wiki) — [Getting Started](https://github.com/AmeyaBorkar/throttlekit-py/wiki/Getting-Started) · [The axes](https://github.com/AmeyaBorkar/throttlekit-py/wiki/The-Axes) · [Conformance & development](https://github.com/AmeyaBorkar/throttlekit-py/wiki/Conformance-and-Development).

## The one invariant

The whole ThrottleKit design rests on it: **exactly one thing computes a `Decision`** — the Node core,
directly or as Lua-in-Redis. Neither backend re-implements an algorithm, so there is no second rate
limiter to keep in sync and no float-determinism risk. The `RedisBackend` marshals ARGV, runs the
core's vendored script, and decodes the reply; the decision is produced **server-side, in Lua**.

## Why reach for it (it's not a thin client)

You're not reaching a re-implemented toy — you're reaching the **one** core whose distributed behavior is
*proven*. Every decision this client returns carries the guarantees the Node core is built on:

- A **machine-checked (TLA⁺), fleet-size-independent overshoot bound** — window-coupled two-tier leasing
  admits ≤ the limit *no matter how many instances*. Most rate limiters can't state a bound at all.
- **GALE** (provable distributed leasing) and **TALE** (token-budget escrow for LLM gateways) ship as real
  features — and they're reachable from Python: leased two-tier `check`, the LLM cost axis via `debit`,
  weighted-fair escrow, and unified rate × concurrency × cost via `admit`.
- **Bit-identical** decisions: the `RedisBackend` replays the core's full golden vectors through real Redis
  and matches the Node oracle field-for-field — so a Python and a Node client on one limit never drift.

A Python service gets the *same proven core* a Node fleet does, not a second rate limiter to keep in sync.
The guarantees — [**how they work**](https://github.com/AmeyaBorkar/throttlekit/wiki/Research) — are what
make this worth reaching for from any language.

## Install

Installed as **`throttlekit-py`**, imported as **`throttlekit`** (PyPI's `throttlekit` is an unrelated
project):

```bash
pip install throttlekit-py            # the gRPC ServiceBackend
pip install "throttlekit-py[redis]"   # + a redis client for the direct RedisBackend
```

## Use — the service door

```python
from throttlekit import ServiceBackend

with ServiceBackend("localhost:50051") as rl:
    d = rl.check("api", api_key)
    if not d.allowed:
        ...  # 429 — retry after d.retry_after_ms
```

`check` / `check_many` / `peek` / `forecast` return frozen `Decision` / `Forecast` dataclasses;
`debit(policy, key, tokens)` meters a windowed **token budget** (the cost axis — debit the actual tokens a
stream produces, e.g. for an LLM gateway). A *denial* is a normal `Decision` (`allowed is False`), never an
exception; gRPC faults map to `PolicyNotFoundError` / `OperationNotSupportedError` / `ServiceUnavailableError`.

`admit(policy, key)` reaches the **concurrency axis** (and unified rate × concurrency): it holds an
in-flight slot for the duration of the work and returns an `Admission` context manager that releases it on
exit — `dropped=True` if the block raised, so the adaptive limit contracts on an overload. The server
reclaims an abandoned slot if a client crashes; for a hold longer than the lease TTL, pass `heartbeat=True`
to renew it from a background thread.

```python
with rl.admit("checkout", user_id) as adm:    # holds a concurrency slot
    if not adm.allowed:
        return 429                             # adm.binding_axis says which axis bound it
    do_work()                                  # released on exit (dropped=True if this raises)
```

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

## Use — async (`asyncio`)

Both doors have `asyncio` twins with the identical surface and the same one-oracle guarantee (they
`await` the transport; they never re-derive a decision). Neither is imported by `import throttlekit`
— they load lazily, so the synchronous client stays free of `grpc.aio` / `redis.asyncio`.

```python
from throttlekit import AsyncServiceBackend

async with AsyncServiceBackend("localhost:50051") as rl:
    d = await rl.check("api", api_key)
    if not d.allowed:
        ...  # 429 — retry after d.retry_after_ms

    adm = await rl.admit("checkout", user_id)   # the concurrency axis
    async with adm:
        if not adm.allowed:
            return 429
        await do_work()                          # released on exit (dropped=True if this raises)
```

```python
import redis.asyncio as redis
from throttlekit import AsyncRedisBackend, Gcra

client = redis.Redis.from_url("redis://localhost:6379")
api = AsyncRedisBackend(client, Gcra(limit=100, period_ms=60_000, burst=20), prefix="prod")
d = await api.check(api_key)
```

## Fleet leasing & the Monitor door (0.5.0)

Two new clients reach the server's additive **Fleet** and **Monitor** doors.

**`FleetBackend` — lease a chunk, spend it locally.** When the server runs a `federated:` policy, a very
high-throughput client can lease a slice of the global per-window budget through `Fleet.Reserve` and serve it
**locally**, round-tripping only to *refresh* — not once per request. The server stays the **one oracle** (it
sizes the grant); `LeasedLimiter` spends it with a `LeaseSpender` that is byte-for-byte the core's leased
path (pinned by the golden `lease` vectors):

```python
from throttlekit import FleetBackend

with FleetBackend("localhost:50051") as fleet:               # loopback needs no secret
    api = fleet.leased("global-api", domain="acme", batch=200)   # lease ~200 at a time, for this tenant
    for _ in workload:
        d = api.check()                                      # spends a local credit; refreshes when low
        if not d.allowed:
            backoff(d.retry_after_ms)                        # the global window is spent (the server's verdict)
```

`batch` (≥ 1) is the throughput lever — how much budget the client holds per refresh; the grant is
**window-coupled** and discarded at the server's window boundary, so the fleet never over-admits. One
`LeasedLimiter` tracks one `(policy, domain)` budget — `domain` selects which tenant's budget to lease (empty
leases the policy as a whole). `AsyncFleetBackend` is the `await` twin.

**`MonitorBackend` — read the server's live state.** The read-only **Monitor door** exposes the same
operational state **ThrottleKit Lens** renders in the terminal — from Python, remotely:

```python
from throttlekit import MonitorBackend

with MonitorBackend("localhost:50051") as mon:               # loopback needs no secret
    snap = mon.get_snapshot()                                # a point-in-time operational snapshot
    for p in snap.policies:
        print(p.name, p.allowed, p.denied)                   # per-policy allow / deny, top keys, latency, …
```

`AsyncMonitorBackend` is the `await` twin. Both doors are **loopback-only by default**; pass the server's
secret as `secret="…"` (with TLS `credentials=…`) to reach them from another host — paired with the server's
`--fleet-secret` / `--monitor-secret`.

## Framework integrations (`throttlekit.contrib`)

Batteries-included adapters for the common Python web stacks. Install the matching extra and import the
one you use (nothing here is pulled in by `import throttlekit`):

```bash
pip install "throttlekit-py[fastapi]"   # or [starlette] / [django] / [flask] / [all]
```

`bind_policy(backend, "api")` turns a service backend into a uniform `key → Decision` checker (a
`RedisBackend.check` bound method already is one). A denial returns **429** with `Retry-After` /
`RateLimit-*` headers (choose IETF or legacy `X-RateLimit-*` with `style=`); the admitted path stamps the
same `RateLimit-*` headers on the response (for the FastAPI **dependency**, when your endpoint returns its
own `Response` object FastAPI keeps it verbatim — reach for `ThrottleKitMiddleware` if you need
unconditional stamping there). Adapters **fail open** by default if the backend is unreachable
(`on_unavailable="allow"`) and key on the raw connecting peer (never a forgeable `X-Forwarded-For`).

```python
# FastAPI — a per-route dependency (or app.add_middleware(ThrottleKitMiddleware, ...) for global limiting)
from fastapi import FastAPI, Depends
from throttlekit import ServiceBackend, bind_policy
from throttlekit.contrib.fastapi import RateLimit

app, backend = FastAPI(), ServiceBackend("localhost:50051")

@app.get("/items", dependencies=[Depends(RateLimit(bind_policy(backend, "api")))])
async def items(): ...
```

```python
# Flask — an extension + @tk.limit() decorator
from throttlekit.contrib.flask import ThrottleKit
tk = ThrottleKit(app, checker=bind_policy(backend, "api"))

@app.get("/")
@tk.limit()
def index(): ...
```

```python
# Django — a view decorator (+ ThrottleKitMiddleware to render denials as 429)
from throttlekit.contrib.django import rate_limit

@rate_limit(bind_policy(backend, "api"))
def my_view(request): ...
```

Prefer to wire it yourself? Use the framework-agnostic `@rate_limit` decorator on any sync or async
function, and `decision_headers(decision)` to render the headers.

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

Beyond unit conformance, a cross-repo **battle test** in the core repo drives every axis through **both**
backends against a real Redis-backed 3-server fleet — distributed cap exactness, the windowCoupled
overshoot bound, crash-reclaim, heartbeat, and one-oracle/two-door state sharing:
[`research/polyglot/battle-test`](https://github.com/AmeyaBorkar/throttlekit/tree/main/research/polyglot/battle-test).

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
