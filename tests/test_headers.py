"""Unit tests for ``decision_headers`` — exact header names/values, no infra needed."""

from __future__ import annotations

import pytest

from throttlekit import Decision, decision_headers


def _d(allowed: bool, limit: int, remaining: int, reset_at: int, retry_after_ms: int) -> Decision:
    return Decision(
        allowed=allowed,
        limit=limit,
        remaining=remaining,
        reset_at=reset_at,
        retry_after_ms=retry_after_ms,
    )


def test_default_style_is_ietf() -> None:
    h = decision_headers(_d(True, 5, 5, reset_at=3_000, retry_after_ms=0), now_ms=0)
    assert "RateLimit-Limit" in h
    assert "X-RateLimit-Limit" not in h


def test_ietf_allowed_no_retry_after() -> None:
    # reset 10s out at now=2s ⇒ delta 8s; allowed ⇒ no Retry-After.
    h = decision_headers(_d(True, 100, 42, reset_at=10_000, retry_after_ms=0), "ietf", now_ms=2_000)
    assert h == {"RateLimit-Limit": "100", "RateLimit-Remaining": "42", "RateLimit-Reset": "8"}


def test_ietf_reset_ceils_and_clamps_and_retry_after() -> None:
    d = _d(False, 100, 0, reset_at=10_500, retry_after_ms=1_500)
    h = decision_headers(d, "ietf", now_ms=10_000)
    assert h["RateLimit-Reset"] == "1"  # ceil((10500-10000)/1000) = ceil(0.5) = 1
    assert h["Retry-After"] == "2"  # ceil(1500/1000) = 2
    past = decision_headers(d, "ietf", now_ms=20_000)
    assert past["RateLimit-Reset"] == "0"  # now beyond reset ⇒ clamp to 0


def test_legacy_reset_is_absolute_epoch_seconds_and_now_independent() -> None:
    d = _d(False, 50, 0, reset_at=1_700_000_000_000, retry_after_ms=250)
    a = decision_headers(d, "legacy", now_ms=0)
    b = decision_headers(d, "legacy", now_ms=1_699_999_999_000)
    assert a == b  # legacy reset does not depend on `now`
    assert a == {
        "X-RateLimit-Limit": "50",
        "X-RateLimit-Remaining": "0",
        "X-RateLimit-Reset": str(1_700_000_000_000 // 1000),
        "Retry-After": "1",  # ceil(250/1000) = 1
    }


def test_remaining_clamps_negative_in_both_styles() -> None:
    h = decision_headers(_d(False, 10, -5, reset_at=5_000, retry_after_ms=100), "both", now_ms=0)
    assert h["RateLimit-Remaining"] == "0"
    assert h["X-RateLimit-Remaining"] == "0"


def test_both_merges_disjoint_keys_with_single_retry_after() -> None:
    h = decision_headers(_d(False, 10, 3, reset_at=4_000, retry_after_ms=2_000), "both", now_ms=0)
    assert set(h) == {
        "RateLimit-Limit",
        "RateLimit-Remaining",
        "RateLimit-Reset",
        "X-RateLimit-Limit",
        "X-RateLimit-Remaining",
        "X-RateLimit-Reset",
        "Retry-After",
    }
    assert h["Retry-After"] == "2"


def test_no_retry_after_when_allowed() -> None:
    h = decision_headers(_d(True, 10, 9, reset_at=4_000, retry_after_ms=0), "both", now_ms=0)
    assert "Retry-After" not in h


def test_legacy_reset_floors_sub_second() -> None:
    # X-RateLimit-Reset is absolute epoch SECONDS, floored — a sub-second resetAt must NOT round up.
    h = decision_headers(_d(False, 1, 0, reset_at=30_500, retry_after_ms=10), "legacy", now_ms=0)
    assert h["X-RateLimit-Reset"] == "30"  # 30_500 // 1000 == 30 (floor, not ceil→31)


def test_retry_after_ceils_and_has_no_min_one_floor() -> None:
    # ceil, emitted only when retry_after_ms>0; a 1ms wait rounds up to 1, never floored from 0 to 1.
    assert decision_headers(_d(False, 1, 0, 5_000, 1), now_ms=0)["Retry-After"] == "1"
    assert decision_headers(_d(False, 1, 0, 5_000, 1_000), now_ms=0)["Retry-After"] == "1"
    assert decision_headers(_d(False, 1, 0, 5_000, 1_001), now_ms=0)["Retry-After"] == "2"
    assert "Retry-After" not in decision_headers(_d(True, 1, 1, 5_000, 0), now_ms=0)


def test_default_now_uses_wall_clock_non_flaky() -> None:
    # With reset far in the future and no now_ms, RateLimit-Reset is a positive integer bounded by the
    # absolute epoch-seconds — asserting only relative invariants keeps this clock-independent.
    h = decision_headers(_d(True, 100, 100, reset_at=9_999_999_999_000, retry_after_ms=0))
    reset = int(h["RateLimit-Reset"])
    assert reset > 0
    assert reset <= 9_999_999_999_000 // 1000


def test_unknown_style_raises() -> None:
    with pytest.raises(ValueError):
        decision_headers(_d(True, 1, 1, 1, 0), "weird")
