"""The concurrency axis — `admit` holds an in-flight slot for the duration of the work (GALE).

`admit` returns an `Admission` context manager: the slot is held while the block runs and returned on
exit — `dropped=True` if the block raised, so an adaptive limit contracts on an overload. If the client
crashes, the server reclaims the slot once the lease TTL lapses; for a hold longer than the TTL, pass
`heartbeat=True` and a background thread renews it.

The `unified-api` policy carries BOTH a rate strategy and a concurrency cap, so `admit` composes them and
`adm.binding_axis` reports which axis ("rate" / "concurrency") bound a denial.

    npx throttlekit-server --config examples/policies.yaml --port 50051
    python examples/concurrency_admit.py
"""

from __future__ import annotations

import os

from throttlekit import ServiceBackend

ADDR = os.environ.get("THROTTLEKIT_ADDR", "localhost:50051")


def main() -> None:
    with ServiceBackend(ADDR) as rl:
        # 1) The lifecycle: hold a slot, do the work, release on block exit.
        print("== concurrency lifecycle (checkout, cap=8) ==")
        with rl.admit("checkout", "user-1") as adm:
            print(f"  allowed={adm.allowed} held={adm.held}")
            if adm.allowed:
                ...  # the protected work; the slot is returned when this block exits
        print("  slot released on block exit")

        # 2) A failed request releases dropped=True — signalling an overload so the limit contracts.
        print("== a raising block releases dropped=True ==")
        try:
            with rl.admit("checkout", "user-1") as adm:
                if adm.allowed:
                    raise RuntimeError("simulated downstream failure")
        except RuntimeError as exc:
            print(f"  released with dropped=True after: {exc}")

        # 3) Unified rate x concurrency — hold slots to press the cap; binding_axis names what denied.
        print("== unified (unified-api: rate 5/burst 5, concurrency cap 2) ==")
        held = []
        try:
            for i in range(4):
                adm = rl.admit("unified-api", "user-1")
                state = "allowed" if adm.allowed else f"DENIED by {adm.binding_axis!r}"
                print(f"  admit #{i + 1}: {state}")
                if adm.allowed:
                    held.append(adm)  # keep the slot held to drive inflight up against the cap
        finally:
            for slot in held:
                slot.release()
        print("  held slots released")

        # 4) Long holds outlive the lease TTL with heartbeat=True (a background thread renews the lease).
        print("== a long hold renews via heartbeat ==")
        with rl.admit("checkout", "batch-job", heartbeat=True) as adm:
            if adm.allowed:
                ...  # run_long_job(); the lease is renewed across the TTL boundary
                if adm.reclaimed:
                    print("  WARNING: the server reclaimed our slot mid-flight — treat as dropped")
        print("  done")


if __name__ == "__main__":
    main()
