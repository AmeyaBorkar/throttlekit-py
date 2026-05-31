"""Vendor the language-neutral contract from the ThrottleKit core repo.

The Python client is a *consumer* of the contract, exactly like every other surface. It pins the
**same** artifacts the Node core publishes, with sha256 checksums, so the two repos cannot silently
drift; re-running this after the core changes produces a reviewable diff (and, on a behavioral break,
a bumped ``contractVersion``). ``tests/test_contract.py`` is the drift-gate.

Two destinations, by audience:

* ``contract/`` — **dev/test artifacts** (not shipped in the wheel): ``throttlekit.proto`` (→ gRPC
  stubs) and ``golden-vectors.json`` (→ conformance). Their integrity is recorded in
  ``contract/manifest.sha256``.
* ``src/throttlekit/_scripts/`` — **runtime data** (shipped in the wheel): the extracted Redis Lua
  (``*.lua``) the :class:`~throttlekit.RedisBackend` executes, plus their ``manifest.json`` (which
  itself carries each script's sha256 — that is the scripts' integrity record).

    python scripts/sync_contract.py [--source <path-to-core-repo>]

Defaults to the sibling ``../GreenfeildProject`` checkout, overridable via ``--source`` or the
``THROTTLEKIT_REPO`` env var. (For a release this would pin a published core version instead.)
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import pathlib
import shutil

ROOT = pathlib.Path(__file__).resolve().parent.parent
CONTRACT = ROOT / "contract"
SCRIPTS_OUT = ROOT / "src" / "throttlekit" / "_scripts"

# Dev/test artifacts (path within the core repo, vendored filename) → contract/, sha256-pinned.
ARTIFACTS = [
    ("wire/throttlekit.proto", "throttlekit.proto"),
    ("wire/vectors/golden-vectors.json", "golden-vectors.json"),
]

# Runtime Lua → src/throttlekit/_scripts/. The script *files* are enumerated from the core's own
# scripts manifest, so we copy exactly the manifest-declared set (no globbing surprises).
SCRIPTS_MANIFEST = "wire/scripts/manifest.json"


def _sha256(path: pathlib.Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source",
        default=os.environ.get("THROTTLEKIT_REPO", str(ROOT.parent / "GreenfeildProject")),
        help="Path to the ThrottleKit core repo (the one containing wire/).",
    )
    args = parser.parse_args()
    source = pathlib.Path(args.source).resolve()

    # 1) Dev/test artifacts → contract/ (sha256-pinned in manifest.sha256).
    CONTRACT.mkdir(exist_ok=True)
    manifest_lines: list[str] = []
    for src_rel, dst_name in ARTIFACTS:
        src = source / src_rel
        if not src.exists():
            raise SystemExit(f"source artifact missing: {src} (is --source the core repo?)")
        dst = CONTRACT / dst_name
        shutil.copyfile(src, dst)
        manifest_lines.append(f"{_sha256(dst)}  {dst_name}")
    (CONTRACT / "manifest.sha256").write_text("\n".join(manifest_lines) + "\n", encoding="utf-8")

    # 2) Runtime Lua → src/throttlekit/_scripts/ (manifest.json + each declared *.lua, byte-for-byte).
    src_manifest = source / SCRIPTS_MANIFEST
    if not src_manifest.exists():
        raise SystemExit(f"source scripts manifest missing: {src_manifest}")
    manifest = json.loads(src_manifest.read_text(encoding="utf-8"))

    if SCRIPTS_OUT.exists():
        shutil.rmtree(SCRIPTS_OUT)  # drop any script the core no longer ships
    SCRIPTS_OUT.mkdir(parents=True)
    shutil.copyfile(src_manifest, SCRIPTS_OUT / "manifest.json")
    script_files: list[str] = []
    for entry in manifest["scripts"]:
        name = entry["file"]
        shutil.copyfile(source / "wire" / "scripts" / name, SCRIPTS_OUT / name)
        script_files.append(name)

    print(f"vendored {len(ARTIFACTS)} dev artifact(s) from {source} into {CONTRACT}")
    for line in manifest_lines:
        print(f"  {line}")
    print(f"vendored {len(script_files)} Lua script(s) + manifest.json into {SCRIPTS_OUT}")
    for name in script_files:
        print(f"  scripts/{name}")


if __name__ == "__main__":
    main()
