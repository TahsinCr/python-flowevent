"""Run the project tests and optionally require the compiled C core."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--native", action="store_true", help="Build and require the C extension.")
    args = parser.parse_args()
    project_root = Path(__file__).resolve().parent
    environment = os.environ.copy()
    if args.native:
        environment["EVENTFLOW_PURE_PYTHON"] = "0"
        environment["EVENTFLOW_NO_EXTENSIONS"] = "0"
        environment["EVENTFLOW_REQUIRE_NATIVE"] = "1"
        for command in (
            [sys.executable, "setup.py", "build_ext", "--inplace", "--force"],
            [
                sys.executable,
                "-c",
                "from flowevent._native import CORE; "
                "raise SystemExit(0 if CORE is not None else 'the compiled flowevent core did not load')",
            ],
        ):
            result = subprocess.run(command, cwd=project_root, env=environment, check=False)
            if result.returncode:
                return result.returncode

    return subprocess.run(
        [sys.executable, "-m", "unittest", "discover", "-s", "tests", "-v"],
        cwd=project_root,
        env=environment,
        check=False,
    ).returncode


if __name__ == "__main__":
    raise SystemExit(main())
