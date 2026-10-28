#!/usr/bin/env python3
"""Explicit original-owned query-step data/lock recovery; offline by default."""
import argparse
from contextlib import ExitStack
import copy
import json
from pathlib import Path
import re
import time

import cli_acceptance as shared
import query_steps_acceptance as steps


def load(folder, context, namespace):
    path = folder / "acceptance.json"
    if path.stat().st_size > 16 << 20:
        raise RuntimeError("Oversized query steps recovery receipt")
    r = json.loads(path.read_text())
    if r.get("schema_version") != "wpdiag.query-steps-acceptance/v1" or r.get("profile") != steps.PROFILE:
        raise RuntimeError("Unsupported query steps recovery receipt")
    if r["selection"]["context"] != context or r["selection"]["namespace"] != namespace:
        raise RuntimeError("Explicit recovery target differs from the original")
    if not re.fullmatch(r"[0-9]{8}T[0-9]{6}Z-[0-9a-f]{8}", r.get("run_id", "")) or \
            r.get("collection") != "query_accept_" + r["run_id"].replace("-", "_") or \
            not isinstance(r.get("collection_owned"), bool):
        raise RuntimeError("Unowned collection/run identity")
    if not r.get("namespace_uid") or not (r.get("lock_uid") or r.get("lock_create_intent")):
        raise RuntimeError("No original namespace/lock ownership intent recorded")
    # Validate the original explicit release/selection without touching a cluster.
    steps.arguments(["--context", context, "--namespace", namespace, "--release", r["selection"]["release"]])
    return r


def recover(folder, context, namespace):
    receipt = load(folder, context, namespace)
    if receipt.get("lock_released") and (not receipt["collection_owned"] or receipt.get("collection_dropped")):
        return {"already_cleaned": True, "run_id": receipt["run_id"]}
    a = steps.Acceptance.__new__(steps.Acceptance)
    a.args = steps.arguments(["--context", context, "--namespace", namespace, "--release", receipt["selection"]["release"]])
    a.kube = ["kubectl", "--context", context, "-n", namespace]
    a.out, a.run_id, a.name = folder, receipt["run_id"], receipt["collection"]
    a.report = copy.deepcopy(receipt)
    a.lock_uid = receipt.get("lock_uid")
    a.deadline = time.monotonic() + 90
    with shared.cleanup_signals(), ExitStack() as stack:
        a.namespace_check()
        if not a.lock_uid:
            lock = json.loads(shared.command(a.kube + ["get", "configmap", steps.LOCK, "-o", "json"]))
            steps.require(lock.get("data") == {"run-id": a.run_id, "profile": steps.PROFILE}, "Ambiguous lock create belongs to another run")
            a.lock_uid = lock["metadata"]["uid"]
        a.lock_check()
        if receipt["collection_owned"]:
            a.milvus = stack.enter_context(shared.Forward(a.kube, "svc/" + a.args.release, 19530))
            a.drop_owned()
        a.lock_check()
        options = folder / "manual-delete-lock.json"
        shared.save(options, {"apiVersion": "v1", "kind": "DeleteOptions", "preconditions": {"uid": a.lock_uid}})
        shared.command(a.kube + ["delete", "--raw", "/api/v1/namespaces/" + namespace + "/configmaps/" + steps.LOCK, "-f", str(options)])
        result = {"schema_version": "wpdiag.query-steps-recovery/v1", "run_id": a.run_id, "finished_at": shared.utc(),
                  "owned_collection_absent": bool(receipt["collection_owned"]), "lock_released": True,
                  "configuration_changes": [], "original_failed_receipt_preserved": True}
        shared.save(folder / "manual-recovery.json", result)
        return result


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--run", type=Path, required=True)
    p.add_argument("--context", required=True)
    p.add_argument("--namespace", required=True)
    p.add_argument("--execute", action="store_true")
    args = p.parse_args(argv)
    receipt = load(args.run, args.context, args.namespace)
    if not args.execute:
        print(json.dumps({"execute": False, "selection": receipt["selection"], "namespace_uid": receipt["namespace_uid"],
                          "lock_uid": receipt.get("lock_uid"), "owned_collection": receipt["collection"] if receipt["collection_owned"] else None,
                          "configuration_changes": [], "scope": "original description/collection ID and UID-guarded lock only"}, indent=2))
        return 0
    recover(args.run, args.context, args.namespace)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
