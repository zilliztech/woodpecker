#!/usr/bin/env python3
"""WP-only workspace acceptance, including a fake Agent; no cluster mutation."""
import subprocess
import sys
from pathlib import Path
if __name__ == '__main__':
 subprocess.run([sys.executable,str(Path(__file__).with_name('workspace_acceptance.py')),*sys.argv[1:]],check=True)
