#!/usr/bin/env python3
"""Write an explicit Helm tracing fixture override; no Kubernetes operations.

Requires the stack's pinned PyYAML dependency. Compatible images are supplied
separately: a server image does not update the client embedded inside Milvus.
"""
import argparse
import json
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[3]


def image(value):
    repository, separator, tag = value.rpartition(":")
    if not separator or not repository or not tag or "/" in tag or "@" in value:
        raise argparse.ArgumentTypeError("Use an explicit repository:tag; runtime image IDs are captured separately")
    return {"repository": repository, "tag": tag, "pullPolicy": "IfNotPresent"}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--milvus-image", type=image, required=True)
    p.add_argument("--wp-image", type=image, required=True)
    p.add_argument("--profile", choices=("single", "batch", "admission-slow"), default="single")
    p.add_argument("--base-values", type=Path, default=ROOT / "deployments/milvus-full-stack-helm/values/milvus.yaml")
    p.add_argument("--output", type=Path, required=True)
    a = p.parse_args()
    base = yaml.safe_load(a.base_values.read_text())
    user = yaml.safe_load(base.get("extraConfigFiles", {}).get("user.yaml", "")) or {}
    segment = user.setdefault("woodpecker", {}).setdefault("client", {}).setdefault("segmentAppend", {})
    segment["maxBatchEntries"] = 1000 if a.profile == "batch" else 1
    wal = user.setdefault("streaming", {}).setdefault("walRateLimit", {})
    memory = wal.setdefault("nodeMemory", {})
    watermarks = memory.setdefault("adaptiveRateLimit", {}).setdefault("slowdown", {})
    slow = a.profile == "admission-slow"
    wal["defaultBurst"] = "128kb" if slow else "20m"
    memory["slowdownThreshold"] = .0001 if slow else .85
    memory["recoverThreshold"] = 0 if slow else .8
    watermarks.update(hwm="16kb" if slow else "4mb", lwm="8kb" if slow else "256kb")
    watermarks["startupDelayInterval"] = "0s"
    values = {"image": {"all": a.milvus_image}, "woodpecker": {"image": a.wp_image},
              "dataNode": {"resources": {"limits": {"memory": "2Gi"}}},
              "extraConfigFiles": {"user.yaml": yaml.safe_dump(user, sort_keys=False)}}
    a.output.parent.mkdir(parents=True, exist_ok=True)
    a.output.write_text(json.dumps(values, indent=2) + "\n")
    print("Wrote " + a.profile + " Helm override: " + str(a.output))


if __name__ == "__main__":
    main()
