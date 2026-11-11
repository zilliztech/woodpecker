#!/usr/bin/env python3
"""Record the real native subprocess protocol without altering stdout or exits.

Only used by the explicitly configured acceptance executable wrapper. No server
or diagnostic implementation is replaced. The original response remains private
to the local receipt directory and is compared to its actual adapter import.
"""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import uuid


def main():
    binary = os.environ["WP_NATIVE_ACCEPTANCE_BINARY"]
    directory = Path(os.environ["WP_NATIVE_ACCEPTANCE_CAPTURE"])
    child = subprocess.Popen([binary, *sys.argv[1:]], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    def cancel(*_):
        if child.poll() is None:
            child.send_signal(signal.SIGINT)
    signal.signal(signal.SIGINT, cancel)
    signal.signal(signal.SIGTERM, cancel)
    try:
        out, err = child.communicate(timeout=40)
    except subprocess.TimeoutExpired:
        cancel()
        try:
            out, err = child.communicate(timeout=2)
        except subprocess.TimeoutExpired:
            child.kill()
            out, err = child.communicate()
    if len(out) > 8 << 20 or len(err) > 64 << 10:
        sys.exit(1)
    token = uuid.uuid4().hex
    (directory / (token + ".native.json")).write_bytes(out)
    (directory / (token + ".exit.json")).write_text(json.dumps({"exit": child.returncode}) + "\n")
    sys.stdout.buffer.write(out)
    # Match the genuine command's stderr and exit; never generate findings here.
    sys.stderr.buffer.write(err)
    sys.exit(child.returncode if child.returncode >= 0 else 7)


if __name__ == "__main__":
    main()
