# throttlekit (Python)

Python client for [**ThrottleKit**](https://www.npmjs.com/package/throttlekit) — distributed rate
limiting via the **gRPC service door**. Talk to a running [`throttlekit-server`](https://github.com/AmeyaBorkar/throttlekit/tree/main/server)
and get decisions **identical** to an embedded Node library, without re-implementing any algorithm.

> **Status: experimental (alpha).** The contract (`throttlekit.proto` + golden vectors) is vendored and
> checksum-pinned from the frozen `throttlekit` 1.0 core; this client tracks it.

## Why a client, not a port

The whole ThrottleKit design rests on one invariant: **exactly one thing computes a `Decision`** — the
Node core. This client is a thin gRPC stub over the service that runs it, so there is no second rate
limiter to keep in sync and no float-determinism risk. (The in-process ~169 ns Node number doesn't
transfer to CPython anyway, so the network-bound service is the right shape for Python.)

## Install & use

```bash
pip install throttlekit            # (alpha; not yet published)
```

```python
from throttlekit import ServiceBackend

with ServiceBackend("localhost:50051") as rl:
    d = rl.check("api", api_key)
    if not d.allowed:
        # 429 — retry after d.retry_after_ms
        ...
```

`check` / `check_many` / `peek` / `forecast` return frozen `Decision` / `Forecast` dataclasses. A
*denial* is a normal `Decision` (`allowed is False`), never an exception; gRPC faults map to
`PolicyNotFoundError` / `OperationNotSupportedError` / `ServiceUnavailableError`.

## The contract (how this stays in lock-step with the core)

`contract/` holds the vendored, sha256-pinned `throttlekit.proto` + `golden-vectors.json`, copied from
the core repo by `scripts/sync_contract.py`. `tests/test_contract.py` is the **drift-gate**: it fails if
the vendored files don't match their checksums or if the golden vectors' `contractVersion` isn't the one
this client pins to. The client cannot silently diverge from the core's wire.

## Develop

```bash
pip install -e .[dev]
python scripts/sync_contract.py          # vendor proto + vectors from ../GreenfeildProject (the core repo)
python scripts/gen_proto.py              # generate the gRPC stubs from the vendored proto
pytest                                   # unit + contract; integration tests need a built server + node
ruff check . && mypy                     # lint + types
```

The cross-language integration test (`tests/test_service_backend.py`) starts a real `throttlekit-server`
and asserts the clock-independent behavior (a cold burst admits exactly `burst`, then denies; unknown
policy → `NOT_FOUND`). It is skipped automatically unless node + a built server are available. The
rigorous, time-parametrized golden-vector replay is the [server's own end-to-end test](https://github.com/AmeyaBorkar/throttlekit/tree/main/server).
