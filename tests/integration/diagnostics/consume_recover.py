#!/usr/bin/env python3
"""Explicit recovery of an interrupted consume acceptance run; plan by default."""
import argparse
import json
from pathlib import Path
import re

import cli_acceptance as shared
import consume_acceptance as c


def load(folder, context, namespace):
    receipt = json.loads((folder / "acceptance.json").read_text())
    journal = json.loads((folder / "restoration.json").read_text())
    if receipt.get("schema_version") != "wpdiag.consume-acceptance/v1" or receipt.get("profile") != c.PROFILE \
            or journal.get("schema_version") != "wpdiag.acceptance.restore/v1" or journal.get("profile") != c.PROFILE:
        raise RuntimeError("Unsupported consume recovery profile")
    if receipt["selection"]["context"] != context or receipt["selection"]["namespace"] != namespace:
        raise RuntimeError("Explicit target differs from original run")
    if journal.get("fault") != c.CONTROLS or set(journal.get("before", {})) != set(c.CONTROLS):
        raise RuntimeError("Journal contains unregistered controls")
    row = journal["before"][c.KEY]
    if row.get("key") != c.KEY or row.get("value") != "false" or row.get("source") not in ("FileSource", "EtcdSource"):
        raise RuntimeError("Invalid original scanner-pause provenance")
    if not re.fullmatch(r"[0-9]{8}T[0-9]{6}Z-[0-9a-f]{8}", receipt.get("run_id", "")):
        raise RuntimeError("Invalid original run ID")
    prefix = "consume_accept_" + receipt["run_id"].replace("-", "_") + "_"
    if not set(receipt.get("owned_collections", {})) <= {prefix + "baseline", prefix + "paused"}:
        raise RuntimeError("Receipt contains an unowned collection name")
    return receipt, journal


def recover(folder, context, namespace):
    receipt, journal = load(folder, context, namespace)
    args = c.arguments(["--context", context, "--namespace", namespace, "--release", receipt["selection"]["release"]])
    # Reuse read-only inventory without allocating another run/artifact directory.
    check = c.Acceptance.__new__(c.Acceptance)
    check.args, check.kube = args, ["kubectl", "--context", context, "-n", namespace]
    pods = check.inventory()
    coordinator = receipt["coordinator"]
    if not any(p["pod"] == coordinator["pod"] and p["uid"] == coordinator["uid"] for p in pods):
        raise RuntimeError("Coordinator identity changed; restore after manual investigation")
    lock = json.loads(shared.command(check.kube + ["get", "configmap", c.LOCK, "-o", "json"]))
    if lock["metadata"]["uid"] != receipt.get("lock_uid") or lock.get("data", {}).get("run-id") != receipt["run_id"] \
            or lock.get("data", {}).get("profile") != c.PROFILE:
        raise RuntimeError("Original namespace lock no longer belongs to this run")
    result = {"schema_version": "wpdiag.consume-recovery/v1", "run_id": receipt["run_id"], "started_at": shared.utc(),
              "configuration_restored": False, "owned_collections": {}}
    with shared.cleanup_signals(), shared.Forward(check.kube, "pod/" + coordinator["pod"], 9091) as endpoint:
        fault = c.ConsumeFault(endpoint, folder)
        fault.before, fault.changed = journal["before"], True
        fault.restore()  # Refuses external config changes; preserves original failed receipt.
        result["configuration_restored"] = True
    shared.save(folder / "manual-recovery.json", result)
    with shared.cleanup_signals(), shared.Forward(check.kube, "svc/" + args.release, 19530) as endpoint:
        for name in receipt.get("owned_collections", {}):
            c.drop_owned_collection(endpoint, name)
            result["owned_collections"][name] = {"dropped": True}
            shared.save(folder / "manual-recovery.json", result)
        # Check lock identity again after configuration/data cleanup.
        current = json.loads(shared.command(check.kube + ["get", "configmap", c.LOCK, "-o", "json"]))
        if current["metadata"]["uid"] != receipt["lock_uid"] or current.get("data", {}).get("run-id") != receipt["run_id"]:
            raise RuntimeError("Lock ownership changed during recovery")
        shared.command(check.kube + ["delete", "configmap", c.LOCK, "--wait=false"])
    result.update(lock_released=True, finished_at=shared.utc())
    shared.save(folder / "manual-recovery.json", result)


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--run", required=True, type=Path)
    p.add_argument("--context", required=True)
    p.add_argument("--namespace", required=True)
    p.add_argument("--execute", action="store_true")
    a = p.parse_args(argv)
    receipt, journal = load(a.run, a.context, a.namespace)
    if not a.execute:
        print(json.dumps({"execute": False, "selection": receipt["selection"], "restore": journal["before"],
                          "drop_only_original_owned_collections": list(receipt.get("owned_collections", {}))}, indent=2))
        return 0
    recover(a.run, a.context, a.namespace)
    print("Original scanner config restored, owned collections removed and original lock released; see manual-recovery.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
