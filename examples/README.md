# Examples

Runnable examples for **throttlekit-py**. Each script is self-contained and carries its own run command
in the module docstring.

| File | Shows |
|---|---|
| [`service_backend.py`](service_backend.py) | The sync gRPC **service door** across all three axes (rate / cost / concurrency) — **store-agnostic**: the same client works whether the server is backed by memory, Redis, Postgres, or DynamoDB. |
| [`async_service_backend.py`](async_service_backend.py) | The **async** door (`AsyncServiceBackend`, the `grpc.aio` twin): `await check`, many keys via `asyncio.gather`, and `admit` as an async context manager. |
| [`redis_backend.py`](redis_backend.py) | The **direct Redis door** — the core's vendored Lua straight against your Redis, decisions bit-identical to an embedded Node library. No server. |
| [`llm_token_budget.py`](llm_token_budget.py) | The **cost axis** (`debit`) — meter what an LLM *spends* as it streams against a windowed token budget (TALE). |
| [`concurrency_admit.py`](concurrency_admit.py) | The **concurrency axis** (`admit`) lifecycle — held slot, release on exit, `dropped=True` on failure, `heartbeat` for long holds, and `binding_axis` on a unified denial (GALE). |
| [`fastapi_app.py`](fastapi_app.py) | A **web-adapter** sample — FastAPI rate-limited by `throttlekit.contrib.fastapi` (a `Depends()` dependency that returns 429 + `RateLimit-*` headers). |
| [`policies.yaml`](policies.yaml) | The policy file the server loads — one policy per axis (rate, two-tier leased, cost, concurrency, unified). |

## Run

Most examples talk to a `throttlekit-server` (the Node package). Start one against any backend, then run a
client. The server's store is its own choice; the Python client sends the same requests regardless and the
core computes every decision server-side — bit-identical across backends.

```sh
# 1. a server, on any store backend (see service_backend.py for all four):
npx throttlekit-server --config examples/policies.yaml --port 50051        # in-process memory
#   …or  --redis redis://localhost:6379
#   …or  --postgres-url postgres://user:pass@localhost:5432/app
#   …or  --store dynamodb --dynamodb-table throttlekit --dynamodb-create-table

# 2. a Python client (connects to localhost:50051; override with THROTTLEKIT_ADDR):
pip install throttlekit-py
python examples/service_backend.py
python examples/llm_token_budget.py
python examples/concurrency_admit.py
```

The **direct** `redis_backend.py` needs no server — it runs the core's Lua against a Redis you point at
(`THROTTLEKIT_REDIS_URL`, default `redis://localhost:6379`). The **web** sample needs the `[fastapi]` extra
and `uvicorn` (`pip install "throttlekit-py[fastapi]" uvicorn`).

> **See your decisions live.** Start the server with `--tui` for **ThrottleKit Lens**, the built-in
> **terminal dashboard** (`throttlekit-server --config policies.yaml --tui`) — the traffic these examples
> drive shows up there, with live **binding-axis attribution** (which of rate / concurrency / cost bound each
> denial) for `unified` policies. It watches the *server's* decisions, so your Python client's traffic appears
> too.

(Deno KV and Cloudflare D1 / Durable Objects / Workers KV are *edge-runtime* stores, reachable only inside
those runtimes — not through the service door.) For the design behind the two doors, see the
[README](../README.md) and the [wiki](https://github.com/AmeyaBorkar/throttlekit-py/wiki).
