#!/usr/bin/env python3
"""Load every *.test.py stack suite explicitly; unittest discovery skips dots."""
import importlib.util
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[2]


def suite(directory):
    result = unittest.TestSuite()
    files = sorted(directory.glob("*.test.py"))
    if not files:
        raise RuntimeError("No stack test files discovered")
    for index, path in enumerate(files):
        name = "stack_ci_test_" + str(index)
        spec = importlib.util.spec_from_file_location(name, path)
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
        tests = unittest.defaultTestLoader.loadTestsFromModule(module)
        if not tests.countTestCases():
            raise RuntimeError("Stack test file has no tests: " + str(path))
        result.addTests(tests)
    return result


if __name__ == "__main__":
    tests = suite(ROOT / "deployments/milvus-full-stack-helm/tests")
    raise SystemExit(0 if unittest.TextTestRunner(verbosity=2).run(tests).wasSuccessful() else 1)
