# Examples

Runnable examples for **throttlekit-py**.

| File | Shows |
|---|---|
| [`service_backend.py`](service_backend.py) | Reach the gRPC **service door** (rate / cost / concurrency) — **store-agnostic**: the same client code works whether the server is backed by memory, Redis, Postgres, or DynamoDB. |
| [`policies.yaml`](policies.yaml) | The policy file the server loads for the example. |

## Run

Start a `throttlekit-server` (the Node package) against any backend, then run the client:

```sh
# 1. a server, on any store backend (see service_backend.py for all four):
npx throttlekit-server --config examples/policies.yaml --port 50051        # in-process memory
#   …or  --redis redis://localhost:6379
#   …or  --postgres-url postgres://user:pass@localhost:5432/app
#   …or  --store dynamodb --dynamodb-table throttlekit --dynamodb-create-table

# 2. the Python client (connects to localhost:50051; override with THROTTLEKIT_ADDR):
pip install throttlekit-py
python examples/service_backend.py
```

The server's store backend is its own choice (`--store memory|redis|postgres|dynamodb`); the Python client
sends the **same** requests regardless, and the core computes every decision server-side — bit-identical
across backends. (Deno KV and Cloudflare are edge-runtime stores, reachable only inside those runtimes, not
through the service door.)

For the direct `RedisBackend` (no server — the client runs the core's vendored Lua against your Redis), see
the [README](../README.md#use--the-direct-redis-door).
