"""Vendor the language-neutral contract from the ThrottleKit core repo into ``contract/``.

The Python client is a *consumer* of the contract, exactly like every other surface: it pins the
**same** ``throttlekit.proto`` and ``golden-vectors.json`` the Node core publishes, with sha256
checksums recorded in ``contract/manifest.sha256``. ``tests/test_contract.py`` verifies the vendored
copies match those checksums, so the two repos cannot silently drift; re-running this after the core
changes produces a reviewable diff (and, on a behavioral break, a bumped ``contractVersion``).

    python scripts/sync_contract.py [--source <path-to-core-repo>]

Defaults to the sibling ``../GreenfeildProject`` checkout, overridable via ``--source`` or the
``THROTTLEKIT_REPO`` env var. (For a release this would pin a published core version instead.)
"""

from __future__ import annotations

import argparse
import hashlib
import os
import pathlib
import shutil

ROOT = pathlib.Path(__file__).resolve().parent.parent
CONTRACT = ROOT / "contract"

# (path within the core repo, vendored filename)
ARTIFACTS = [
    ("wire/throttlekit.proto", "throttlekit.proto"),
    ("wire/vectors/golden-vectors.json", "golden-vectors.json"),
]


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
    print(f"vendored {len(ARTIFACTS)} artifact(s) from {source} into {CONTRACT}")
    for line in manifest_lines:
        print(f"  {line}")


if __name__ == "__main__":
    main()
