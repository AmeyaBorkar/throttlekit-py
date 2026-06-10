# Security Policy

## Reporting a vulnerability

**Please do not open a public issue for security problems.** Report privately via GitHub's coordinated
disclosure: on the [throttlekit-py repository](https://github.com/AmeyaBorkar/throttlekit-py), open the
**Security** tab → **“Report a vulnerability”** (GitHub Private Vulnerability Reporting). For an issue in the
*core decision logic* rather than this client, report it on the
[throttlekit](https://github.com/AmeyaBorkar/throttlekit) repo instead.

Please include the affected version, a minimal reproduction, the impact you observed, and any suggested fix.
Expect acknowledgement within 72 hours.

## Supported versions

`throttlekit-py` is **experimental (alpha)**; fixes land on the latest published version. The vendored
contract (proto, golden vectors, Lua) is checksum-pinned from the frozen `throttlekit` 1.0 core.

## Scope notes

`throttlekit-py` is a **client** — it transports decisions computed by the one Node core and implements no
rate-limiting math itself. The threats most relevant to it:

- **Service door (`ServiceBackend`).** The default gRPC credentials are **insecure** (loopback/dev only).
  Front any exposed `throttlekit-server` with **TLS/mTLS** so nothing can poison a shared budget — pass channel
  credentials to `ServiceBackend(target, credentials=...)`.
- **Fleet & Monitor doors (`FleetBackend`, `MonitorBackend`).** Same posture: the server binds these **loopback-only
  by default** and exposing either off-host requires an explicit secret (and you should add **TLS**). Treat them as
  sensitive — the Fleet `Reserve` door *hands out* a chunk of a policy's global budget, and the Monitor door
  *exposes* traffic snapshots and the denial feed (i.e. your limiter keys). Don't put them on an open port.
- **Direct door (`RedisBackend`).** It runs the core's vendored Lua against your shared Redis. Any party that
  can write to that Redis can consume or distort the budget — restrict access to trusted instances and treat
  the Redis as a trust boundary.
- **Contract integrity.** The vendored proto, golden vectors, and Lua are checksum-pinned (`tests/test_contract.py`);
  a tampered contract fails the drift-gate.
- **Unfrozen wire.** The raw Lua wire ships `frozen: false` and may change with the core's scripts; pin a
  compatible `throttlekit-py` ↔ core pair.
