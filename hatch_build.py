"""Hatchling build hook: generate the gRPC stubs into the wheel.

The stubs (``src/throttlekit/_generated/``) are git-ignored build artifacts, so a plain
``python -m build`` would ship a wheel *without* them and ``ServiceBackend`` would raise ``ImportError``
on a fresh install. This hook regenerates them from the vendored ``contract/throttlekit.proto`` at build
time (``grpcio-tools`` comes from ``[build-system].requires``), and ``[tool.hatch.build.targets.wheel]
artifacts`` force-includes the result past ``.gitignore``. It runs for the wheel build — direct or from
the sdist (the sdist carries the proto + scripts, so a wheel-from-sdist regenerates identically).
"""

from __future__ import annotations

import pathlib
import subprocess
import sys

from hatchling.builders.hooks.plugin.interface import BuildHookInterface


class CustomBuildHook(BuildHookInterface):
    def initialize(self, version: str, build_data: dict) -> None:
        root = pathlib.Path(self.root)
        gen = root / "scripts" / "gen_proto.py"
        if not gen.exists():  # defensive — never silently ship a stub-less wheel without trying
            raise FileNotFoundError(f"cannot generate gRPC stubs: {gen} is missing")
        # `sys.executable` is the isolated build env's interpreter, which has grpcio-tools from
        # [build-system].requires; gen_proto re-invokes it as `-m grpc_tools.protoc`.
        subprocess.run([sys.executable, str(gen)], check=True, cwd=root)
