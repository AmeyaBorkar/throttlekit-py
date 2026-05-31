"""Pure-Python gate on the strategy ↔ wire-ARGV binding (no Redis required).

The :class:`~throttlekit.RedisBackend` marshals a script's ARGV by resolving each manifest ARGV *name*
to ``now`` / ``cost`` / a strategy parameter. These tests prove, for every strategy, that the
parameters it supplies are exactly the names the vendored manifest expects (in order) — so a typo or a
core ARGV reordering is caught here, without standing up Redis. The live golden-vector replay
(``test_redis_backend.py``) is the behavioral backstop on top of this structural one.
"""

from __future__ import annotations

import pytest

from throttlekit import _contract, from_spec
from throttlekit.strategies import (
    FixedWindow,
    Gcra,
    SlidingWindow,
    SlidingWindowLog,
    TokenBucket,
)

# Representative options per strategy (camelCase, exactly as the golden vectors express them).
SPECS = {
    "gcra": {"limit": 10, "periodMs": 1000, "burst": 5},
    "tokenBucket": {"capacity": 10, "refillPerSec": 5},
    "fixedWindow": {"limit": 5, "windowMs": 1000},
    "slidingWindow": {"limit": 10, "windowMs": 1000, "buckets": 10},
    "slidingWindowLog": {"limit": 5, "windowMs": 1000},
}


@pytest.mark.parametrize("kind", sorted(SPECS))
def test_params_exactly_cover_check_argv(kind: str) -> None:
    strategy = from_spec(kind, SPECS[kind])
    script = _contract.script(kind, "check")
    # {now, cost} ∪ the strategy's params must equal the manifest's ARGV names — no missing, no extra.
    supplied = {"now", "cost"} | set(strategy.params())
    assert set(script.argv) == supplied, kind
    # And the whole ARGV vector resolves positionally from those values.
    values = {"now": 0, "cost": 1, **strategy.params()}
    argv = [values[name] for name in script.argv]
    assert len(argv) == len(script.argv)
    assert argv[0] == 0  # now is always ARGV[1]


def test_from_spec_maps_camelcase_options() -> None:
    assert from_spec("gcra", SPECS["gcra"]) == Gcra(limit=10, period_ms=1000, burst=5)
    assert from_spec("tokenBucket", SPECS["tokenBucket"]) == TokenBucket(capacity=10, refill_per_sec=5)
    assert from_spec("fixedWindow", SPECS["fixedWindow"]) == FixedWindow(limit=5, window_ms=1000)
    assert from_spec("slidingWindow", SPECS["slidingWindow"]) == SlidingWindow(
        limit=10, window_ms=1000, buckets=10
    )
    assert from_spec("slidingWindowLog", SPECS["slidingWindowLog"]) == SlidingWindowLog(
        limit=5, window_ms=1000
    )


def test_from_spec_rejects_unknown_kind() -> None:
    with pytest.raises(ValueError, match="unknown strategy kind"):
        from_spec("leakyBucket", {"limit": 1})


def test_strategies_are_frozen() -> None:
    g = Gcra(limit=10, period_ms=1000, burst=5)
    with pytest.raises((AttributeError, TypeError)):
        g.limit = 11  # type: ignore[misc]
