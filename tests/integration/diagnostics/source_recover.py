#!/usr/bin/env python3
"""Inspect, then explicitly delete only original journal-owned source resources."""
import argparse
import json
from pathlib import Path

import cli_acceptance as shared
import source_fixture as fixture


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--context", required=True)
    p.add_argument("--namespace", required=True)
    p.add_argument("--receipt", type=Path, required=True, help="Original fixture.json")
    p.add_argument("--execute", action="store_true")
    a = p.parse_args()
    receipt = json.loads(a.receipt.read_text())
    if receipt.get("profile") != fixture.PROFILE or not fixture.OWNER.fullmatch(receipt.get("run_id", "")) or (
            receipt.get("context") != a.context or receipt.get("namespace") != a.namespace):
        p.error("Original receipt profile/owner/context/namespace mismatch")
    allowed = {kind + "/" + receipt["run_id"] for kind in ("pod", "configmap", "serviceaccount", "role", "rolebinding")}
    allowed.add("configmap/" + fixture.LOCK)
    if not receipt.get("resources") or not set(receipt["resources"]) <= allowed:
        p.error("Journal contains an unregistered resource name/kind")
    if a.execute:
        with shared.cleanup_signals():
            fixture.Fixture(a.context, a.namespace, a.receipt.parent, receipt).cleanup()
    print(json.dumps({"execute": a.execute, "run_id": receipt["run_id"], "cleaned": receipt.get("cleaned", False),
                      "resources": sorted(receipt["resources"])}))


if __name__ == "__main__":
    main()
