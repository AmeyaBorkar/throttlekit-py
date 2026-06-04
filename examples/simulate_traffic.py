"""Simulate sustained, mixed traffic against a throttlekit-server — and watch the Lens light up.

This drives realistic load across all three axes at once — **rate** (`check`), **cost** (`debit`), and
**concurrency** (`admit`) — from a skewed key population (a few hot users + a long tail), so the
server's built-in **Lens** dashboard shows live deny rates, top-denied keys, and the thing nobody else
can render: which axis is *binding* each denial right now. It prints the same picture to the terminal,
so it's a demo with or without the browser.

Two terminals:

    # 1. the server — the Lens auto-starts at http://127.0.0.1:9090
    npx throttlekit-server --config examples/policies.yaml --port 50051

    # 2. open http://127.0.0.1:9090, then drive traffic:
    pip install throttlekit-py
    python examples/simulate_traffic.py                    # 30s at ~150 req/s
    python examples/simulate_traffic.py --rps 400 --duration 120

Flags: --addr (default $THROTTLEKIT_ADDR or localhost:50051), --rps, --duration, --seed.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import random
import sys
import time
from collections import Counter

from throttlekit import AsyncServiceBackend
from throttlekit.errors import ThrottleKitError

# A skewed population: ~70% of traffic hits 3 hot users, the rest a long tail — so deny rates and a
# "top keys" leaderboard actually emerge (a flat key space never trips a per-key limit).
HOT_USERS = [f"user-{i}" for i in range(3)]
TAIL_USERS = [f"user-{i}" for i in range(3, 40)]
TENANTS = ["tenant-a", "tenant-b", "tenant-c"]
AXES = ("rate", "concurrency", "cost", "policy")


def pick_user() -> str:
    return random.choice(HOT_USERS) if random.random() < 0.7 else random.choice(TAIL_USERS)


class Stats:
    """Running tallies, mirrored from what the Lens shows server-side."""

    def __init__(self) -> None:
        self.total = 0
        self.allowed = 0
        self.denied = 0
        self.errors = 0
        self.by_axis: Counter[str] = Counter()
        self.denied_keys: Counter[str] = Counter()

    def record(self, *, allowed: bool, axis: str, key: str) -> None:
        self.total += 1
        if allowed:
            self.allowed += 1
        else:
            self.denied += 1
            self.by_axis[axis] += 1
            self.denied_keys[key] += 1


async def drive_rate(rl: AsyncServiceBackend, stats: Stats) -> None:
    key = pick_user()
    try:
        d = await rl.check("api", key)
    except ThrottleKitError:
        stats.errors += 1
        return
    stats.record(allowed=d.allowed, axis="rate", key=key)


async def drive_cost(rl: AsyncServiceBackend, stats: Stats) -> None:
    tenant = random.choice(TENANTS)
    try:
        d = await rl.debit("completions", tenant, tokens=random.randint(500, 6000))
    except ThrottleKitError:
        stats.errors += 1
        return
    stats.record(allowed=d.allowed, axis="cost", key=tenant)


async def drive_admit(
    rl: AsyncServiceBackend, stats: Stats, held: list[asyncio.Task[None]]
) -> None:
    key = pick_user()
    try:
        adm = await rl.admit("unified-api", key)
    except ThrottleKitError:
        stats.errors += 1
        return
    # On a denial the core reports which axis bound it ("rate" / "concurrency"); credit the right lane.
    stats.record(allowed=adm.allowed, axis=adm.binding_axis or "concurrency", key=key)
    if not adm.allowed:
        return

    async def hold_then_release() -> None:
        # Hold the slot a short random while so in-flight builds up against the cap, then return it
        # (occasionally as a "drop" — a failed request — so the adaptive limit contracts).
        try:
            await asyncio.sleep(random.uniform(0.05, 0.4))
        finally:
            await adm.release(dropped=random.random() < 0.1)

    held.append(asyncio.create_task(hold_then_release()))


_prev_lines = 0


def show(text: str) -> None:
    """Redraw the summary in place on a TTY; otherwise just append blocks."""
    global _prev_lines
    if sys.stdout.isatty():
        out = sys.stdout
        if _prev_lines:
            out.write(f"\033[{_prev_lines}A")  # move the cursor back up over the previous block
        out.write("\n".join(f"\033[2K{line}" for line in text.split("\n")) + "\n")
        out.flush()
        _prev_lines = text.count("\n") + 1
    else:
        print(text)
        print("-" * 48)


def render(stats: Stats, *, remaining: float, final: bool, started: float) -> str:
    elapsed = max(1e-9, time.monotonic() - started)
    rps = stats.total / elapsed
    deny_pct = (stats.denied / stats.total * 100) if stats.total else 0.0
    peak = max(stats.by_axis.values(), default=1) or 1
    lines = [
        f"  throttlekit traffic sim   {'DONE' if final else f'{remaining:4.0f}s left'}",
        f"  requests {stats.total:>8}   {rps:6.0f} req/s",
        f"  allowed  {stats.allowed:>8}   denied {stats.denied:>7}   ({deny_pct:4.1f}% deny)",
        "  denials by binding axis:",
    ]
    for axis in AXES:
        n = stats.by_axis.get(axis, 0)
        bar = "#" * min(28, n * 28 // peak)
        lines.append(f"    {axis:<12} {n:>6}  {bar}")
    top = ", ".join(f"{k}({n})" for k, n in stats.denied_keys.most_common(3)) or "none"
    lines.append(f"  top denied keys: {top}")
    if stats.errors:
        lines.append(f"  rpc errors: {stats.errors}")
    return "\n".join(lines)


async def run(addr: str, rps: int, duration: int) -> int:
    stats = Stats()
    async with AsyncServiceBackend(addr) as rl:
        try:
            await rl.check("api", "warmup")  # fail fast with a friendly hint if no server is up
        except ThrottleKitError:
            print(
                f"Can't reach a throttlekit-server at {addr}. Start one in another terminal:\n"
                "  npx throttlekit-server --config examples/policies.yaml --port 50051",
                file=sys.stderr,
            )
            return 1

        print(f"Driving ~{rps} req/s for {duration}s at {addr} — open http://127.0.0.1:9090\n")
        held: list[asyncio.Task[None]] = []
        started = time.monotonic()
        deadline = started + duration
        last_print = 0.0
        tick = 0.1
        per_tick = max(1, round(rps * tick))

        while time.monotonic() < deadline:
            t0 = time.monotonic()
            batch = []
            for _ in range(per_tick):
                roll = random.random()
                if roll < 0.5:
                    batch.append(drive_rate(rl, stats))
                elif roll < 0.8:
                    batch.append(drive_admit(rl, stats, held))
                else:
                    batch.append(drive_cost(rl, stats))
            await asyncio.gather(*batch, return_exceptions=True)
            held = [task for task in held if not task.done()]
            now = time.monotonic()
            if now - last_print >= 1.0:
                show(render(stats, remaining=deadline - now, final=False, started=started))
                last_print = now
            slept = time.monotonic() - t0
            if slept < tick:
                await asyncio.sleep(tick - slept)

        for task in held:
            task.cancel()
        show(render(stats, remaining=0, final=True, started=started))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Simulate mixed traffic against a throttlekit-server."
    )
    parser.add_argument("--addr", default=os.environ.get("THROTTLEKIT_ADDR", "localhost:50051"))
    parser.add_argument("--rps", type=int, default=150, help="target requests/second (default 150)")
    parser.add_argument("--duration", type=int, default=30, help="seconds to run (default 30)")
    parser.add_argument("--seed", type=int, default=None, help="seed the RNG for a repeatable run")
    args = parser.parse_args()
    if args.seed is not None:
        random.seed(args.seed)
    if os.name == "nt":
        os.system("")  # enable ANSI escape handling on the Windows console
    try:
        return asyncio.run(run(args.addr, args.rps, args.duration))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
