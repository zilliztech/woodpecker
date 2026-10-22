#!/usr/bin/env python3
"""Bounded insert/query/vector-search workload through the Milvus REST API."""
import json
import os
import random
import time
import urllib.request

BASE = os.getenv("MILVUS_URL", "http://127.0.0.1:" + os.getenv("MILVUS_PORT", "19530"))


def call(path, payload):
    request = urllib.request.Request(BASE + "/v2/vectordb/" + path,
                                     data=json.dumps(payload).encode(),
                                     headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=90) as response:
        body = json.load(response)
    if body.get("code") != 0:
        raise RuntimeError(f"{path}: {body}")
    return body.get("data")


def main():
    name = os.getenv("COLLECTION", "full_stack_" + str(time.time_ns()))
    batches = int(os.getenv("BATCHES", "12"))
    batch_size = int(os.getenv("BATCH_SIZE", "100"))
    interval = float(os.getenv("INTERVAL", "2"))
    if not (1 <= batches <= 10000 and 1 <= batch_size <= 10000 and 0 <= interval <= 60):
        raise ValueError("Use 1..10000 batches/rows and an interval of 0..60 seconds")
    rng = random.Random(42)
    call("collections/create", {"collectionName": name, "dimension": 8, "autoID": False,
         "metricType": "COSINE", "consistencyLevel": "Strong",
         "params": {"shardsNum": 1}})
    print(json.dumps({"stage": "created", "collection": name}), flush=True)
    first = None
    started = time.monotonic()
    for batch in range(batches):
        rows = [{"id": batch * batch_size + i, "vector": [rng.random() for _ in range(8)]}
                for i in range(batch_size)]
        if first is None:
            first = rows[0]["vector"]
        result = call("entities/insert", {"collectionName": name, "data": rows})
        if result.get("insertCount") != batch_size:
            raise RuntimeError(f"Unexpected insert count: {result}")
        result = call("entities/query", {"collectionName": name, "filter": "id == 0",
                      "outputFields": ["id"], "consistencyLevel": "Strong"})
        if not result or result[0].get("id") != 0:
            raise RuntimeError(f"Inserted row is not queryable: {result}")
        result = call("entities/search", {"collectionName": name, "data": [first],
                      "limit": 3, "outputFields": ["id"], "consistencyLevel": "Strong"})
        if not result or result[0].get("id") != 0:
            raise RuntimeError(f"Self vector was not the nearest result: {result}")
        print(json.dumps({"stage": "batch", "batch": batch + 1,
                          "inserted": (batch + 1) * batch_size, "query_and_search": "passed"}), flush=True)
        time.sleep(interval)
    print(json.dumps({"stage": "passed", "collection": name, "rows": batches * batch_size,
                      "duration_seconds": round(time.monotonic() - started, 2),
                      "retained": True}), flush=True)


if __name__ == "__main__":
    main()
