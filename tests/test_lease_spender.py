"""``LeaseSpender`` — unit behaviour + the golden ``lease`` vector conformance.

The vector replay is the cross-language proof that the Python lease spend is **byte-identical** to the
Node core's ``twoTier(leased, windowCoupled)`` L1 path: the same inputs (a scripted interleave of grants
and spends) must reproduce every expected reply (an allow, or ``None`` = "refresh needed") exactly.
"""

from __future__ import annotations

import json
import pathlib

import pytest

from throttlekit import Decision
from throttlekit.lease_spender import LeaseGrant, LeaseSpender

VECTORS = pathlib.Path(__file__).resolve().parents[1] / "contract" / "golden-vectors.json"


def _lease_suites() -> list[dict]:
    doc = json.loads(VECTORS.read_text(encoding="utf-8"))
    return [s for s in doc["suites"] if s["primitive"] == "lease"]


def test_contract_ships_lease_suites() -> None:
    assert len(_lease_suites()) >= 6  # the core contract pins (a)-(f) + the windowCoupled contrast


@pytest.mark.parametrize("suite", _lease_suites(), ids=lambda s: s["name"])
def test_lease_vector_replay(suite: dict) -> None:
    spender = LeaseSpender(
        limit=suite["limit"], ttl_ms=suite["ttlMs"], window_coupled=suite["windowCoupled"]
    )
    for ev in suite["events"]:
        if ev["op"] == "grant":
            spender.apply_lease(LeaseGrant(capacity=ev["capacity"], expires_at=ev["expiresAt"]))
            continue
        got = spender.spend(ev["now"], ev["cost"])
        expect = ev["expect"]
        if expect["needsRefresh"]:
            assert got is None, f"{suite['name']}: expected a refresh signal"
        else:
            d = expect["decision"]
            assert got == Decision(
                allowed=d["allowed"],
                limit=d["limit"],
                remaining=d["remaining"],
                reset_at=d["resetAt"],
                retry_after_ms=d["retryAfterMs"],
            ), f"{suite['name']} @ now={ev['now']}"


def test_spend_decrements_then_signals_refresh() -> None:
    s = LeaseSpender(limit=5, ttl_ms=1000)
    s.apply_lease(LeaseGrant(capacity=3, expires_at=1000))
    assert s.spend(0, 1) == Decision(
        allowed=True, limit=5, remaining=2, reset_at=1000, retry_after_ms=0
    )
    assert s.spend(0, 2) == Decision(
        allowed=True, limit=5, remaining=0, reset_at=1000, retry_after_ms=0
    )
    assert s.spend(0, 1) is None


def test_window_coupled_discard() -> None:
    s = LeaseSpender(limit=10, ttl_ms=1000)
    s.apply_lease(LeaseGrant(capacity=5, expires_at=1000))
    assert s.spend(0, 2) is not None
    assert s.credits == 3
    assert s.spend(1000, 1) is None  # now >= expiry → the 3 credits are discarded
    assert s.credits == 0


def test_carry_over_when_not_window_coupled() -> None:
    s = LeaseSpender(limit=10, ttl_ms=1000, window_coupled=False)
    s.apply_lease(LeaseGrant(capacity=5, expires_at=1000))
    s.spend(0, 2)  # credits 3
    assert s.spend(2000, 1) == Decision(
        allowed=True, limit=10, remaining=2, reset_at=1000, retry_after_ms=0
    )


def test_cost_is_validated() -> None:
    s = LeaseSpender(limit=5, ttl_ms=1000)
    s.apply_lease(LeaseGrant(capacity=5, expires_at=1000))
    with pytest.raises(ValueError):
        s.spend(0, 0)
    with pytest.raises(ValueError):
        s.spend(0, -1)


def test_reset_forgets_credits_and_window() -> None:
    s = LeaseSpender(limit=5, ttl_ms=1000)
    s.apply_lease(LeaseGrant(capacity=5, expires_at=1000))
    s.reset()
    assert s.credits == 0
    assert s.expires_at is None
    assert s.spend(0, 1) is None
