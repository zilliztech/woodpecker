#!/usr/bin/env python3
"""Explicit owned-data/lock recovery after query harness loss; offline by default."""
import argparse
import json
from pathlib import Path
import re

import cli_acceptance as shared
import query_acceptance as q


def load(folder, context, namespace):
    receipt = json.loads((folder / "acceptance.json").read_text())
    if receipt.get("schema_version") != "wpdiag.query-acceptance/v1" or receipt.get("profile") != q.PROFILE:
        raise RuntimeError("Unsupported query recovery profile")
    if receipt["selection"]["context"] != context or receipt["selection"]["namespace"] != namespace:
        raise RuntimeError("Explicit recovery target differs from original run")
    if not re.fullmatch(r"[0-9]{8}T[0-9]{6}Z-[0-9a-f]{8}", receipt.get("run_id", "")) \
            or receipt.get("collection") != "query_accept_" + receipt["run_id"].replace("-", "_"):
        raise RuntimeError("Invalid run or unowned collection identity")
    if not receipt.get("lock_uid") or not isinstance(receipt.get("collection_owned"), bool):
        raise RuntimeError("Original lock/data ownership is unresolved")
    return receipt


def recover(folder, context, namespace):
    receipt = load(folder, context, namespace)
    args = q.arguments(["--context", context, "--namespace", namespace, "--release", receipt["selection"]["release"]])
    kube = ["kubectl", "--context", context, "-n", namespace]
    ns = json.loads(shared.command(kube + ["get", "namespace", namespace, "-o", "json"]))
    if ns["metadata"].get("labels", {}).get(q.LABEL) != q.PROFILE:
        raise RuntimeError("Original namespace is no longer opted in")
    def lock():
        value = json.loads(shared.command(kube + ["get", "configmap", q.LOCK, "-o", "json"]))
        if value["metadata"]["uid"] != receipt["lock_uid"] or value.get("data", {}).get("run-id") != receipt["run_id"] or value.get("data", {}).get("profile") != q.PROFILE:
            raise RuntimeError("Original exclusive lock ownership changed")
    lock()
    with shared.cleanup_signals():
        if receipt["collection_owned"]:
            with shared.Forward(kube, "svc/" + args.release, 19530) as endpoint:
                q.drop_owned(endpoint, receipt["collection"])
        lock()
        shared.command(kube + ["delete", "configmap", q.LOCK, "--wait=false"])
        shared.save(folder / "manual-recovery.json", {"schema_version": "wpdiag.query-recovery/v1", "at": shared.utc(),
            "run_id": receipt["run_id"], "owned_collection_absent": bool(receipt["collection_owned"]), "lock_released": True,
            "configuration_changes": [], "original_failed_receipt_preserved": True})


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--run", type=Path, required=True)
    p.add_argument("--context", required=True)
    p.add_argument("--namespace", required=True)
    p.add_argument("--execute", action="store_true")
    a = p.parse_args(argv)
    receipt = load(a.run, a.context, a.namespace)
    if not a.execute:
        print(json.dumps({"execute": False, "selection": receipt["selection"], "configuration_changes": [],
            "drop_only_if_owned": receipt["collection"] if receipt["collection_owned"] else None, "original_lock_uid": receipt["lock_uid"]}, indent=2))
        return 0
    recover(a.run, a.context, a.namespace)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
