"""CI guard: assert the built wheel contains the generated gRPC stubs (i.e. the build hook ran).

The stubs in ``src/throttlekit/_generated/`` are git-ignored build artifacts generated at build time by
``hatch_build.py``. A broken hook would ship a stub-less wheel whose ``import ServiceBackend`` fails only
on a fresh install — a regression a file-presence check in the source tree can't catch. Run it after
``python -m build`` (the CI ``build`` job does):

    python -m build
    python scripts/check_wheel_stubs.py
"""

from __future__ import annotations

import glob
import sys
import zipfile

# The two stubs the service door imports; both must be force-included past .gitignore into the wheel.
REQUIRED = (
    "throttlekit/_generated/throttlekit_pb2.py",
    "throttlekit/_generated/throttlekit_pb2_grpc.py",
)


def main() -> int:
    wheels = glob.glob("dist/*.whl")
    if not wheels:
        print("no wheel in dist/ - run `python -m build` first", file=sys.stderr)
        return 2
    whl = wheels[0]
    names = set(zipfile.ZipFile(whl).namelist())
    missing = [name for name in REQUIRED if name not in names]
    if missing:
        have = sorted(name for name in names if "_generated" in name)
        print(f"build hook FAILED: {whl} is missing {missing}", file=sys.stderr)
        print(f"  _generated entries present: {have}", file=sys.stderr)
        return 1
    print(f"build hook OK: gRPC stubs present in {whl}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
