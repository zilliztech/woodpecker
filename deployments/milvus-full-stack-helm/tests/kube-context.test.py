#!/usr/bin/env python3
"""Actionable kubeconfig diagnostics without switching contexts or exposing credentials."""
import contextlib
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("kube_context", ROOT / "scripts/kube-context.py")
kube = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(kube)


def config(current="local"):
    return {"current-context": current, "contexts": [{"name": "local", "context": {"cluster": "local-cluster"}},
            {"name": "shared context", "context": {"cluster": "shared", "namespace": "dev"}}],
            "users": [{"name": "never-print-this", "user": {"token": "secret-token"}}]}


class KubeContextTests(unittest.TestCase):
    def run_resolve(self, value, explicit="", exists=True, error=None):
        replies = [subprocess.CompletedProcess([], 0, json.dumps(value), "")]
        if error is not None: replies.append(subprocess.CompletedProcess([], 1, "", error))
        else: replies.append(subprocess.CompletedProcess([], 0, "{}", ""))
        with patch.object(kube, "invoke", side_effect=replies) as calls, patch.object(Path, "is_file", return_value=exists), \
                contextlib.redirect_stderr(io.StringIO()) as output:
            result = kube.resolve(explicit, "stack-ns", "status")
        self.assertNotIn("secret-token", output.getvalue())
        self.assertNotIn("never-print-this", output.getvalue())
        self.assertFalse(any("use-context" in c.args[0] for c in calls.call_args_list))
        return result, output.getvalue(), calls

    def test_default_context_is_validated_without_mutating_kubeconfig(self):
        result, text, calls = self.run_resolve(config())
        self.assertEqual(result, "local")
        self.assertEqual(text, "")
        self.assertEqual(calls.call_args_list[1].args[0][:3], ["kubectl", "--context", "local"])

    def test_explicit_context_overrides_the_default_without_switching_it(self):
        result, _, calls = self.run_resolve(config(), explicit="shared context")
        self.assertEqual(result, "shared context")
        self.assertEqual(calls.call_args_list[1].args[0][2], "shared context")

    def test_existing_contexts_without_current_selection_are_listed_with_copyable_commands(self):
        result, text, calls = self.run_resolve(config(""))
        self.assertIsNone(result)
        self.assertIn("none is selected", text)
        self.assertIn("cluster=shared", text)
        self.assertIn("./stack.sh status --context 'shared context' --namespace stack-ns", text)
        self.assertIn("kubectl config use-context CONTEXT_NAME", text)
        self.assertEqual(calls.call_count, 1)

    def test_missing_files_are_distinguished_from_an_existing_file_with_no_contexts(self):
        for exists, message in ((False, "No kubeconfig file was found"), (True, "contains no contexts")):
            with self.subTest(exists=exists):
                result, text, calls = self.run_resolve({"contexts": []}, exists=exists)
                self.assertIsNone(result)
                self.assertIn(message, text)
                self.assertIn("export KUBECONFIG=/absolute/path/to/your/kubeconfig", text)
                self.assertEqual(calls.call_count, 1)

    def test_unknown_context_never_attempts_a_connection(self):
        result, text, calls = self.run_resolve(config(), explicit="stale-context")
        self.assertIsNone(result)
        self.assertIn("not in the loaded kubeconfig", text)
        self.assertIn("Selected context: stale-context", text)
        self.assertEqual(calls.call_count, 1)

    def test_connection_failure_explains_network_and_auth_without_printing_raw_stderr(self):
        for error, reason in (("connection refused secret-token", "API is unreachable"),
                              ("Unauthorized secret-token", "authentication failed"),
                              ("x509: certificate expired secret-token", "certificate verification failed")):
            with self.subTest(error=error):
                result, text, _ = self.run_resolve(config(), error=error)
                self.assertIsNone(result)
                self.assertIn(reason, text)
                self.assertIn("kubectl --context local get pods -n stack-ns", text)

    def test_unreadable_config_does_not_claim_that_no_contexts_exist(self):
        with patch.object(kube, "invoke", return_value=subprocess.CompletedProcess([], 1, "", "invalid syntax secret-token")), \
                contextlib.redirect_stderr(io.StringIO()) as output:
            self.assertIsNone(kube.resolve("", "example", "status"))
        text = output.getvalue()
        self.assertIn("Cannot read kubeconfig", text)
        self.assertNotIn("No contexts are available", text)
        self.assertNotIn("secret-token", text)

    def test_explicit_kubeconfig_search_paths_are_shown_without_reading_file_contents(self):
        with patch.dict(kube.os.environ, {"KUBECONFIG": "/first/config:/second/config"}):
            self.assertEqual(kube.config_paths(), [Path("/first/config"), Path("/second/config")])


if __name__ == "__main__": unittest.main()
