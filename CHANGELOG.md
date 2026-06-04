# Changelog

All notable changes to **throttlekit-py** are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); this client tracks the frozen `throttlekit`
1.0 core's contract and versions independently of it.

## [Unreleased]

### Added

- **Async backends (`asyncio`).** `AsyncServiceBackend` (the `grpc.aio` twin of `ServiceBackend`) and
  `AsyncRedisBackend` (the `redis.asyncio` twin of `RedisBackend`) — faithful `await`-mirrors of the sync
  doors with the same one-oracle guarantee (they transport a decision, never re-derive it). Both are
  lazily imported, so `import throttlekit` still needs neither `grpc.aio` nor a redis client.
  `AsyncServiceBackend` carries the full surface (`check`/`check_many`/`peek`/`forecast`/`debit`/`admit`)
  and an `AsyncAdmission` async context manager with a background `asyncio` heartbeat task.
- **Framework-agnostic ergonomics.** `decision_headers(decision, style=...)` renders standard
  `RateLimit-*` (IETF) or `X-RateLimit-*` (legacy) + `Retry-After` response headers; the `@rate_limit`
  decorator (wraps sync **or** async functions) raises `RateLimited` on a denial; `bind_policy(backend,
  policy)` turns a service backend into a uniform `key → Decision` checker. All eagerly importable and
  dependency-light.
- **Framework integrations (`throttlekit.contrib.*`).** Batteries-included adapters, each behind its own
  optional extra:
  - `throttlekit.contrib.fastapi` — a `RateLimit(...)` `Depends()` dependency (+ the Starlette middleware,
    re-exported).
  - `throttlekit.contrib.starlette` — `ThrottleKitMiddleware`, a pure-ASGI middleware that returns a 429
    with headers and stamps `RateLimit-*` on admitted responses.
  - `throttlekit.contrib.django` — a `@rate_limit` view decorator (`block` / `Ratelimited`) and a
    middleware that maps `Ratelimited` to a 429.
  - `throttlekit.contrib.flask` — a `ThrottleKit` extension with an `@tk.limit()` decorator.
  - New extras: `throttlekit-py[fastapi]` / `[starlette]` / `[django]` / `[flask]` / `[all]`.
  - Adapters default to **fail-open** on a backend outage (`on_unavailable="allow"`) and key on the raw
    connecting peer (never a forgeable `X-Forwarded-For`).

## [0.3.0] — 2026-05-31

### Added

- `admit()` reaches the concurrency / unified (rate × concurrency) admission axis over the service door
  (Door C), returning an `Admission` lifecycle handle with crash-safe leasing + opt-in heartbeats.

## [0.2.1] — 2026-05-31

### Fixed

- Packaging: ship the generated gRPC stubs inside the wheel so a fresh `pip install` can import
  `ServiceBackend`; declare the `protobuf` runtime dependency.

## [0.2.0] — 2026-05-31

### Added

- `debit()` for the cost axis — reach windowed token-budget policies over the service door (Door B).

## [0.1.0] — 2026-05-31

### Added

- Initial release: the `ServiceBackend` (gRPC) and direct `RedisBackend` (vendored Lua) doors, the five
  rate-limit strategies, the frozen `Decision` / `Forecast` types, and the cross-language conformance
  suite proving decisions bit-identical to the Node oracle.

[Unreleased]: https://github.com/AmeyaBorkar/throttlekit-py/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/AmeyaBorkar/throttlekit-py/releases/tag/v0.3.0
[0.2.1]: https://github.com/AmeyaBorkar/throttlekit-py/releases/tag/v0.2.1
[0.2.0]: https://github.com/AmeyaBorkar/throttlekit-py/releases/tag/v0.2.0
[0.1.0]: https://github.com/AmeyaBorkar/throttlekit-py/releases/tag/v0.1.0
