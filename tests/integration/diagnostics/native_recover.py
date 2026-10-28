#!/usr/bin/env python3
"""Explicit original-owner cleanup after #489 interruption; offline plan by default.

The only fault lives in an owned ephemeral Pod and automatically releases within
70 seconds. Recovery removes only that Pod and its ConfigMap under the original
namespace/lock/resource identities; it never changes a shared service or dataset.
"""
import argparse
import json
from pathlib import Path
import sys

import cli_acceptance as shared
from native_fixture import Fixture, OWNER, PROFILE, LOCK


def load(folder, context, namespace):
    path = folder / "fixture.json"
    if path.stat().st_size > 1 << 20:
        raise ValueError("Fixture journal exceeds its byte budget")
    receipt = json.loads(path.read_text())
    if receipt.get("schema_version") != "wpdiag.native.fixture/v1" or receipt.get("profile") != PROFILE or not OWNER.fullmatch(receipt.get("run_id", "")):
        raise ValueError("Unsupported native fixture journal")
    if receipt.get("selection") != {"context": context, "namespace": namespace}:
        raise ValueError("Recovery must use the original explicit context and namespace")
    allowed = {"pod/" + receipt["run_id"], "configmap/" + receipt["run_id"], "configmap/" + LOCK}
    if not set(receipt.get("resources", {})) <= allowed:
        raise ValueError("Unregistered resource in recovery journal")
    for key, resource in receipt.get("resources", {}).items():
        kind, name = key.split("/", 1)
        intent = resource["intent"]
        if intent.get("kind", "").lower() != kind or intent["metadata"]["name"] != name:
            raise ValueError("Recovery intent identity mismatch")
        if kind == "pod" and intent.get("metadata", {}).get("namespace") != namespace:
            raise ValueError("Recovery Pod namespace mismatch")
    return receipt


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--run", type=Path, required=True)
    p.add_argument("--context", required=True)
    p.add_argument("--namespace", required=True)
    p.add_argument("--execute", action="store_true")
    a = p.parse_args(argv)
    receipt = load(a.run, a.context, a.namespace)
    if not a.execute:
        print(json.dumps({"mode": "plan", "run_id": receipt["run_id"], "selection": receipt["selection"],
                          "resources": list(receipt["resources"]), "action": "UID-guarded owned ephemeral fixture removal"}, indent=2))
        return 0
    if receipt.get("cleaned"):
        raise RuntimeError("Fixture already cleaned; no further API action required")
    with shared.cleanup_signals():
        fixture = Fixture(a.context, a.namespace, a.run, receipt)
        fixture.cleanup()
        # Preserve the original failed acceptance receipt.
        shared.save(a.run / "manual-recovery.json", {"schema_version": "wpdiag.native.recovery/v1", "run_id": receipt["run_id"],
                                                   "cleaned": receipt.get("cleaned", False), "finished_at": shared.utc()})
    return 0


if __name__ == "__main__":
    sys.exit(main())
