#!/usr/bin/env python3
"""Write an explicit reproducible baseline override for a disposable Helm stack.

Pure local file transformation. It performs no Kubernetes/Helm operations.
Requires the same PyYAML dependency as the existing stack planner.
"""
import argparse
import json
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[3]
BASELINE = {"streaming": {"walRateLimit": {"defaultBurst": "20m", "nodeMemory": {
    "slowdownThreshold": .85, "adaptiveRateLimit": {"slowdown": {"hwm": "4mb", "lwm": "256kb"}}}}}}


def merge(left, right):
    result = dict(left)
    for key, value in right.items():
        result[key] = merge(result.get(key, {}), value) if isinstance(value, dict) else value
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-values", type=Path, default=ROOT / "deployments/milvus-full-stack-helm/values/milvus.yaml")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    values = yaml.safe_load(args.base_values.read_text())
    user = yaml.safe_load(values.get("extraConfigFiles", {}).get("user.yaml", "")) or {}
    result = {"extraConfigFiles": {"user.yaml": yaml.safe_dump(merge(user, BASELINE), sort_keys=False)}}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print("Wrote disposable baseline Helm override: " + str(args.output))


if __name__ == "__main__":
    main()
