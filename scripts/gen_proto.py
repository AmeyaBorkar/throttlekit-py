"""Generate the gRPC stubs from the vendored contract proto into ``src/throttlekit/_generated/``.

Requires the dev extra (``pip install -e .[dev]`` → ``grpcio-tools``). The generated files are build
artifacts (git-ignored); regenerate them whenever the vendored ``throttlekit.proto`` changes.

    python scripts/gen_proto.py
"""

from __future__ import annotations

import pathlib
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
PROTO = ROOT / "contract" / "throttlekit.proto"
OUT = ROOT / "src" / "throttlekit" / "_generated"


def main() -> None:
    if not PROTO.exists():
        raise SystemExit(
            f"vendored proto missing: {PROTO}\nrun `python scripts/sync_contract.py` first"
        )

    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "__init__.py").write_text(
        '"""Generated gRPC stubs for throttlekit.v1 — do not edit; see scripts/gen_proto.py."""\n',
        encoding="utf-8",
    )

    cmd = [
        sys.executable,
        "-m",
        "grpc_tools.protoc",
        f"-I{PROTO.parent}",
        f"--python_out={OUT}",
        f"--grpc_python_out={OUT}",
        str(PROTO),
    ]
    subprocess.run(cmd, check=True)

    # protoc emits an absolute `import throttlekit_pb2` in the *_grpc.py; make it package-relative so the
    # stubs import correctly from inside the `throttlekit._generated` package.
    grpc_file = OUT / "throttlekit_pb2_grpc.py"
    text = grpc_file.read_text(encoding="utf-8")
    text = text.replace(
        "import throttlekit_pb2 as throttlekit__pb2",
        "from . import throttlekit_pb2 as throttlekit__pb2",
    )
    grpc_file.write_text(text, encoding="utf-8")

    print(f"generated stubs in {OUT}")


if __name__ == "__main__":
    main()
