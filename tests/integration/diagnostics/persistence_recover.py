#!/usr/bin/env python3
"""Recover only an original persistence acceptance run; offline plan by default."""
import argparse
import json
from pathlib import Path
import re

import cli_acceptance as shared
import persistence_acceptance as p
from persistence_fault import StorageFault, PROFILE, quota_resources


def load(folder, context, namespace):
    receipt = json.loads((folder / "acceptance.json").read_text())
    journal = json.loads((folder / "storage-restoration.json").read_text())
    if receipt.get("schema_version") != "wpdiag.persistence-acceptance/v1" or receipt.get("profile") != PROFILE \
            or journal.get("schema_version") != "wpdiag.storage-resize/v1" or journal.get("profile") != PROFILE:
        raise RuntimeError("Unsupported recovery profile")
    selection = receipt.get("selection", {})
    if selection.get("context") != context or selection.get("namespace") != namespace:
        raise RuntimeError("Explicit target differs from original run")
    run = receipt.get("run_id", "")
    if not re.fullmatch(r"[0-9]{8}T[0-9]{6}Z-[0-9a-f]{8}", run) or receipt.get("collection") != "persist_accept_" + run.replace("-", "_"):
        raise RuntimeError("Unowned recovery collection")
    before = journal.get("before", {})
    preflight = receipt.get("storage_before") is None and receipt.get("collection_owned") is False and journal.get("mutation_started") is False
    if not preflight and before != receipt.get("storage_before"):
        raise RuntimeError("Storage journal differs from original receipt")
    if before.get("name") != selection.get("storage_statefulset") or before.get("replicas") != 1 \
            or not before.get("uid") or not before.get("claims") or not re.fullmatch(r"[0-9a-f]{64}", before.get("spec_sha256", "")):
        raise RuntimeError("Invalid original controller/claim provenance")
    resources = before.get("resources", {})
    if not isinstance(resources, dict) or not resources.get("limits", {}).get("cpu") or len(before.get("pods", [])) != 1 or not before["pods"][0].get("uid") \
            or journal.get("fault_resources") != quota_resources(resources):
        raise RuntimeError("Invalid or non-reversible original CPU resource journal")
    if type(receipt.get("collection_owned")) is not bool or type(journal.get("mutation_started")) is not bool or not receipt.get("lock_uid"):
        raise RuntimeError("Invalid ownership journal")
    return receipt, journal


def recover(folder, context, namespace):
    receipt, journal = load(folder, context, namespace)
    selection = receipt["selection"]
    args = p.arguments(["--context", context, "--namespace", namespace, "--release", selection["release"],
        "--storage-statefulset", selection["storage_statefulset"], "--storage-service", selection["storage_service"]])
    check = p.Acceptance.__new__(p.Acceptance)
    check.args, check.kube = args, ["kubectl", "--context", context, "-n", namespace]
    check.lock_uid, check.run_id = receipt["lock_uid"], receipt["run_id"]
    # Dependency restoration must not depend on affected Milvus/WP being Ready.
    namespace_row = json.loads(shared.command(check.kube + ["get", "namespace", namespace, "-o", "json"]))
    if namespace_row["metadata"].get("labels", {}).get(p.LABEL) != PROFILE:
        raise RuntimeError("Original namespace is no longer explicitly opted in")
    check.assert_lock()
    fault = StorageFault(check.kube, selection["storage_statefulset"], folder)
    fault.before, fault.changed = journal["before"], journal["mutation_started"]
    result = {"schema_version": "wpdiag.persistence-recovery/v1", "run_id": receipt["run_id"], "started_at": shared.utc(),
              "configuration_restored": False, "collection_dropped": False}
    with shared.cleanup_signals():
        result["storage_after"] = fault.restore()
        result["configuration_restored"] = True
        shared.save(folder / "manual-recovery.json", result)
        check.assert_lock()
        if receipt["collection_owned"]:
            with shared.Forward(check.kube, "svc/" + selection["release"], 19530) as endpoint:
                p.drop_owned(endpoint, receipt["collection"])
            result["collection_dropped"] = True
            shared.save(folder / "manual-recovery.json", result)
        check.assert_lock()
        shared.command(check.kube + ["delete", "configmap", p.LOCK, "--wait=false"])
    result.update(lock_released=True, finished_at=shared.utc())
    shared.save(folder / "manual-recovery.json", result)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", required=True, type=Path)
    parser.add_argument("--context", required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--execute", action="store_true")
    a = parser.parse_args(argv)
    receipt, journal = load(a.run, a.context, a.namespace)
    if not a.execute:
        print(json.dumps({"execute": False, "selection": receipt["selection"], "restore_original_resources": journal["before"]["resources"],
            "preserve_original_claims": journal["before"]["claims"], "drop_owned_collection": receipt["collection"] if receipt["collection_owned"] else None}, indent=2))
        return 0
    recover(a.run, a.context, a.namespace)
    print("Original storage restored, owned data and lock resolved; see manual-recovery.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
