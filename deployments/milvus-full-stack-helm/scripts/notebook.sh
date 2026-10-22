#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE="${STATE:-$ROOT/.local}"
mkdir -p "$STATE"
STATE="$(cd "$STATE" && pwd)"
export STATE
if [ -z "${JUPYTER_PYTHON:-}" ]; then
  JUPYTER_BOOTSTRAP_PYTHON="${JUPYTER_BOOTSTRAP_PYTHON:-python3}"
  "$JUPYTER_BOOTSTRAP_PYTHON" -c 'import sys; assert sys.version_info >= (3, 11), "Notebook launcher requires Python 3.11+"'
  if [ ! -x "$STATE/notebook-venv/bin/python" ]; then
    "$JUPYTER_BOOTSTRAP_PYTHON" -m venv "$STATE/notebook-venv"
  fi
  JUPYTER_PYTHON="$STATE/notebook-venv/bin/python"
  if ! "$JUPYTER_PYTHON" -c 'from importlib.metadata import version; assert version("jupyterlab")=="4.6.4"; assert version("ipykernel")=="7.4.0"; assert version("nbclient")=="0.11.0"' 2>/dev/null; then
    "$JUPYTER_PYTHON" -m pip install -r "$ROOT/notebooks/requirements.txt"
  fi
fi
# Keep all runtime files and kernel discovery local to this example.
export JUPYTER_CONFIG_DIR="$STATE/jupyter/config"
export JUPYTER_DATA_DIR="$STATE/jupyter/data"
export JUPYTER_RUNTIME_DIR="$STATE/jupyter/runtime"
export IPYTHONDIR="$STATE/jupyter/ipython"
PATH="$(dirname "$JUPYTER_PYTHON"):$PATH"
export PATH
mkdir -p "$JUPYTER_CONFIG_DIR" "$JUPYTER_DATA_DIR" "$JUPYTER_RUNTIME_DIR" "$IPYTHONDIR"
exec "$JUPYTER_PYTHON" -m jupyterlab --ip=127.0.0.1 --port="${NOTEBOOK_PORT:-8891}" \
  --ServerApp.port_retries=0 --no-browser --ServerApp.root_dir="$ROOT" \
  --LabApp.default_url=/lab/tree/notebooks/full-stack-walkthrough.ipynb
