"""Contract drift-gate (pure Python; no gRPC required).

Verifies the vendored contract is internally consistent with its recorded checksums and that the golden
vectors carry the ``contractVersion`` this client pins to. Re-running ``scripts/sync_contract.py`` after
the core changes would change these files (and the manifest), surfacing drift as a reviewable diff.
"""

from __future__ import annotations

import hashlib
import json
import pathlib

CONTRACT = pathlib.Path(__file__).resolve().parent.parent / "contract"

# The wire/contract version this client supports. A behavioral break in the core bumps this; the bump
# must be matched here deliberately, so an incompatible contract fails fast rather than mis-decoding.
PINNED_CONTRACT_VERSION = "1"


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
