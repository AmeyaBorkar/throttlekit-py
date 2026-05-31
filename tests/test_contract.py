"""Contract drift-gate (pure Python; no gRPC required).

Verifies the vendored contract is internally consistent with its recorded checksums and that the golden
vectors carry the ``contractVersion`` this client pins to. Re-running ``scripts/sync_contract.py`` after
the core changes would change these files (and the manifest), surfacing drift as a reviewable diff.
"""

from __future__ import annotations

import hashlib
import json
import pathlib

REPO = pathlib.Path(__file__).resolve().parent.parent
CONTRACT = REPO / "contract"
# Runtime Lua ships inside the package (so it resolves in an installed wheel), with its own manifest.
SCRIPTS = REPO / "src" / "throttlekit" / "_scripts"

# The wire/contract version this client supports. A behavioral break in the core bumps this; the bump
# must be matched here deliberately, so an incompatible contract fails fast rather than mis-decoding.
PINNED_CONTRACT_VERSION = "1"

# The five strategies that ship an extracted Lua check/read pair (the rateLimit primitive).
VECTORED_STRATEGIES = {"gcra", "tokenBucket", "fixedWindow", "slidingWindow", "slidingWindowLog"}


def _manifest() -> dict[str, str]:
    digests: dict[str, str] = {}
    for line in (CONTRACT / "manifest.sha256").read_text(encoding="utf-8").splitlines():
        if line.strip():
            digest, name = line.split()
            digests[name] = digest
    return digests


def test_vendored_artifacts_match_manifest() -> None:
    manifest = _manifest()
    assert set(manifest) == {"throttlekit.proto", "golden-vectors.json"}
    for name, digest in manifest.items():
        actual = hashlib.sha256((CONTRACT / name).read_bytes()).hexdigest()
        assert actual == digest, f"{name} drifted from manifest — re-run scripts/sync_contract.py"


def test_golden_vectors_pin_and_shape() -> None:
    doc = json.loads((CONTRACT / "golden-vectors.json").read_text(encoding="utf-8"))
    assert doc["contractVersion"] == PINNED_CONTRACT_VERSION
    # The reply field order the client decodes (camelCase in the JSON; snake_case on Decision).
    assert doc["decisionFields"] == ["allowed", "limit", "remaining", "resetAt", "retryAfterMs"]
    assert any(s["primitive"] == "rateLimit" for s in doc["suites"])


def test_proto_declares_the_v1_service() -> None:
    text = (CONTRACT / "throttlekit.proto").read_text(encoding="utf-8")
    assert "package throttlekit.v1;" in text
    assert "service RateLimiter" in text


# ── The vendored runtime Lua (src/throttlekit/_scripts/) — the RedisBackend's wire ──────────────────


def _scripts_manifest() -> dict[str, object]:
    return json.loads((SCRIPTS / "manifest.json").read_text(encoding="utf-8"))


def test_vendored_scripts_match_their_manifest() -> None:
    """Every shipped .lua must match the sha256 recorded inside _scripts/manifest.json.

    This is the byte-lock the Node ``conformance-scripts.test.ts`` enforces on the core side, now
    checked on the vendored copy: a script and its manifest entry cannot drift apart unnoticed.
    """
    doc = _scripts_manifest()
    scripts = doc["scripts"]
    assert isinstance(scripts, list)
    seen: set[tuple[str, str]] = set()
    for entry in scripts:
        path = SCRIPTS / entry["file"]
        actual = hashlib.sha256(path.read_bytes()).hexdigest()
        assert actual == entry["sha256"], f"{entry['file']} drifted — re-run scripts/sync_contract.py"
        seen.add((entry["strategy"], entry["role"]))
    # Exactly the five vectored strategies, each with a check + a read script.
    assert seen == {(s, r) for s in VECTORED_STRATEGIES for r in ("check", "read")}


def test_scripts_manifest_pins_contract_version_and_reply_shape() -> None:
    doc = _scripts_manifest()
    assert doc["contractVersion"] == PINNED_CONTRACT_VERSION
    assert isinstance(doc["frozen"], bool)
    # The reply tuple the RedisBackend decodes must agree with the vectors' decisionFields.
    assert doc["replyTuple"] == ["allowed", "limit", "remaining", "resetAt", "retryAfterMs"]
    vectors = json.loads((CONTRACT / "golden-vectors.json").read_text(encoding="utf-8"))
    assert doc["replyTuple"] == vectors["decisionFields"]
    # Every check script carries `now` first and `cost`; read scripts take no ARGV.
    for entry in doc["scripts"]:
        if entry["role"] == "check":
            assert entry["argv"][0] == "now"
            assert "cost" in entry["argv"]
        else:
            assert entry["argv"] == []
