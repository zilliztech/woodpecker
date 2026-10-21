#!/usr/bin/env python3
"""Retry interrupted or stalled host Docker image downloads with a time limit."""
import os
import subprocess
import sys


def main():
    image = sys.argv[1]
    timeout = int(os.getenv("IMAGE_PULL_TIMEOUT", "300"))
    if timeout <= 0:
        raise ValueError("IMAGE_PULL_TIMEOUT must be positive")
    for attempt in range(1, 4):
        print(f"Pulling {image}, attempt {attempt}/3 (timeout {timeout}s)", flush=True)
        try:
            if subprocess.run(["docker", "pull", image], timeout=timeout).returncode == 0:
                return
        except subprocess.TimeoutExpired:
            print("Image download stalled; restarting the registry connection", file=sys.stderr, flush=True)
    raise SystemExit(f"Unable to pull {image} after 3 attempts")


if __name__ == "__main__":
    main()
