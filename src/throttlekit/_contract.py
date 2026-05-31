"""Loader for the vendored wire contract — the Redis Lua scripts and their manifest.

Reads ``_scripts/manifest.json`` and the extracted ``*.lua`` beside it (the exact bytes the Node core
publishes, vendored by ``scripts/sync_contract.py``) and exposes, per (strategy, role), the script
source, its ordered ARGV names, and the **SHA-1** Redis caches the script by. The ARGV order comes
from the manifest, so the wire — not this client — is the single source of truth for marshalling.

The directory lives inside the package, so it resolves identically in a source checkout and an
installed wheel. Lookups are cached; the files are read once.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from functools import cache, lru_cache
from pathlib import Path
from typing import Any

_SCRIPTS_DIR = Path(__file__).resolve().parent / "_scripts"
_MANIFEST_PATH = _SCRIPTS_DIR / "manifest.json"


@dataclass(frozen=True)
class Script:
    """One vendored Lua script: its source, ordered ARGV names, and the SHA-1 Redis caches it by."""

    name: str
    """Manifest name, e.g. ``gcra.check``."""
    strategy: str
    role: str
    """``check`` (returns the reply tuple, may write) or ``read`` (returns raw state, never writes)."""
    argv: tuple[str, ...]
    """ARGV names in order — ``now`` first, then strategy params, ``cost`` where present."""
    source: str
    """The exact Lua text (UTF-8)."""
    sha1: str
    """``sha1(source)`` hex — the key Redis uses for ``EVALSHA`` (computed client-side)."""


@lru_cache(maxsize=1)
def _manifest() -> dict[str, Any]:
    data: dict[str, Any] = json.loads(_MANIFEST_PATH.read_text(encoding="utf-8"))
    return data


def contract_version() -> str:
    """The vendored wire ``contractVersion`` (a behavioral break in the core bumps this)."""
    return str(_manifest()["contractVersion"])


@cache
def script(strategy: str, role: str) -> Script:
    """The vendored :class:`Script` for ``strategy`` (e.g. ``gcra``) and ``role`` (``check``/``read``)."""
    for entry in _manifest()["scripts"]:
        if entry["strategy"] == strategy and entry["role"] == role:
            source = (_SCRIPTS_DIR / entry["file"]).read_text(encoding="utf-8")
            return Script(
                name=entry["name"],
                strategy=strategy,
                role=role,
                argv=tuple(entry["argv"]),
                source=source,
                sha1=hashlib.sha1(source.encode("utf-8")).hexdigest(),
            )
    raise KeyError(f"no vendored script for strategy={strategy!r} role={role!r}")
