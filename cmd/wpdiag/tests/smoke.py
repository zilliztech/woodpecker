#!/usr/bin/env python3
"""Current wp CLI acceptance against isolated HTTP fixtures; no browser or model."""
import subprocess
import sys
from pathlib import Path
if __name__ == '__main__':
 subprocess.run([sys.executable,str(Path(__file__).with_name('workspace_acceptance.py')),'--cli-only',*sys.argv[1:]],check=True)
