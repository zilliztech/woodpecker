#!/usr/bin/env python3
"""Single owner for each root-module unit package and SDK integration package."""
import argparse
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[2]
MODULE = "github.com/zilliztech/woodpecker/"
SCOPES = ("core", "wpcli", "diagnostics")


def below(path, prefix):
    return path == prefix or path.startswith(prefix + "/")


def owner(package):
    if not package.startswith(MODULE):
        raise ValueError("Package outside root module: " + package)
    path = package[len(MODULE):]
    if below(path, "tests"):
        return None
    if below(path, "cmd/wpcli"):
        return "wpcli"
    if below(path, "cmd/wpdiag") or below(path, "diagnostics"):
        return "diagnostics"
    return "core"


def select(packages, scope):
    if scope not in (*SCOPES, "all", "diagnostic-helpers", "sdk-integration"):
        raise ValueError("Unknown CI scope: " + scope)
    if len(packages) != len(set(packages)):
        raise ValueError("Duplicate package enumeration")
    if scope == "diagnostic-helpers":
        result = [p for p in packages if p.startswith(MODULE + "tests/integration/diagnostics/")]
    elif scope == "sdk-integration":
        result = [p for p in packages if below(p, MODULE + "tests/integration") and not any(
            below(p, MODULE + "tests/integration/" + directory) for directory in ("diagnostics", "tracing"))]
    else:
        result = [p for p in packages if owner(p) in (SCOPES if scope == "all" else (scope,))]
    if not result:
        raise ValueError("Empty CI package selection: " + scope)
    return sorted(result)


def packages():
    # Enumerate broken packages too: their owner's go test must fail, without an
    # unrelated tool import error preventing every other suite from starting.
    return subprocess.check_output(["go", "list", "-e", "./..."], cwd=ROOT, text=True).splitlines()


def verify(values):
    groups = [set(select(values, scope)) for scope in SCOPES]
    if set.union(*groups) != {p for p in values if owner(p) is not None} or any(
            a & b for i, a in enumerate(groups) for b in groups[i + 1:]):
        raise ValueError("Root unit ownership is incomplete or overlapping")
    nested = {str(p.parent.relative_to(ROOT)) for p in (ROOT / "tests/integration/diagnostics").rglob("go.mod")}
    if nested != {"tests/integration/diagnostics/queryclient"}:
        raise ValueError("Update the diagnostics nested-module test job: " + repr(nested))
    return {scope: len(select(values, scope)) for scope in SCOPES}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("scope", choices=(*SCOPES, "all", "diagnostic-helpers", "sdk-integration", "verify"))
    args = p.parse_args()
    values = packages()
    if args.scope == "verify":
        print(verify(values))
    else:
        print("\n".join(select(values, args.scope)))


if __name__ == "__main__":
    main()
