#!/usr/bin/env python3
"""Explicit recovery of this profile's journal after process loss (no workload)."""
import argparse
import json
from pathlib import Path

import cli_acceptance as a


def load(folder, context, namespace):
    receipt = json.loads((folder / "acceptance.json").read_text())
    journal = json.loads((folder / "restoration.json").read_text())
    if receipt.get("profile") != a.PROFILE or journal.get("profile") != a.PROFILE \
            or journal.get("schema_version") != "wpdiag.acceptance.restore/v1":
        raise RuntimeError("Unsupported restoration profile")
    if receipt["selection"]["context"] != context or receipt["selection"]["namespace"] != namespace:
        raise RuntimeError("Explicit target does not match the original run")
    before = journal["before"]
    if journal.get("fault") != a.FAULT or set(before) != set(a.FAULT):
        raise RuntimeError("Journal contains unregistered controls")
    for key, row in before.items():
        if row.get("key") != key or row.get("source") not in ("FileSource", "EtcdSource") \
                or not isinstance(row.get("value"), str) or len(row["value"]) > 128:
            raise RuntimeError("Invalid restoration provenance")
    return receipt, journal


def recover(folder, context, namespace):
    receipt, journal = load(folder, context, namespace)
    args = a.arguments(["--context", context, "--namespace", namespace, "--release", receipt["selection"]["release"]])
    check = a.Acceptance(args)
    coordinator, _ = check.inventory()
    original = next(p for p in receipt["builds"] if p["pod"] == coordinator)
    current = next(p for p in check.report["builds"] if p["pod"] == coordinator)
    if original["uid"] != current["uid"]:
        raise RuntimeError("Coordinator identity changed; inspect and restore manually")
    lock = json.loads(a.command(check.kube + ["get", "configmap", "wpdiag-cli-acceptance-lock", "-o", "json"]))
    if lock["metadata"]["uid"] != receipt["lock_uid"] or lock.get("data", {}).get("run-id") != receipt["run_id"]:
        raise RuntimeError("Original namespace acceptance lock is not owned by this run")
    with a.cleanup_signals(), a.Forward(check.kube, "pod/" + coordinator, 9091) as endpoint:
        fault = a.ConfigFault(endpoint, folder)
        fault.before, fault.changed = journal["before"], True
        rows = fault.read()
        for key, row in rows.items():
            if row != fault.before[key] and not (row["source"] == "EtcdSource" and row["value"] == a.FAULT[key]):
                raise RuntimeError("Control changed outside this run; refusing to overwrite it")
        fault.restore()
        a.command(check.kube + ["delete", "configmap", "wpdiag-cli-acceptance-lock", "--wait=false"])
    a.save(folder / "manual-recovery.json", {"configuration_restored": True, "at": a.utc(),
        "collection": receipt["collection"], "collection_cleanup": "Inspect/drop the original owned collection separately"})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", type=Path, required=True)
    parser.add_argument("--context", required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    receipt, journal = load(args.run, args.context, args.namespace)
    if not args.execute:
        print(json.dumps({"execute": False, "selection": receipt["selection"],
                          "restore": journal["before"], "collection": receipt["collection"]}, indent=2))
        return
    recover(args.run, args.context, args.namespace)
    print("Original configuration restored and owned lock released; see manual-recovery.json")


if __name__ == "__main__":
    main()
