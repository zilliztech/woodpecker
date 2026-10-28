"""Offline source/registry counterexamples. No credentials, network or cluster."""
import base64
import contextlib
import copy
import io
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import release_check as r

ARGS = ["--milvus-ref", "v3.0.4", "--wp-server-ref", "v0.1.47",
        "--milvus-image", "milvusdb/milvus:v3.0.4", "--wp-image", "milvusdb/woodpecker:v0.1.47"]


def manifest():
    return {"mediaType": "application/vnd.oci.image.index.v1+json", "digest": "sha256:" + "a" * 64,
            "manifests": [{"digest": "sha256:" + "b" * 64, "platform": {"os": "linux", "architecture": "arm64"}},
                          {"digest": "sha256:" + "c" * 64, "platform": {"os": "linux", "architecture": "amd64"}}]}


def positive_checks():
    return [{"role": role, "ancestry": "ahead", "outcome": "required_commit_in_ancestry"} for role in r.REQUIRED_ROLES]


class ReleaseCheckTests(unittest.TestCase):
    def test_plan_is_offline_and_writes_nothing(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "absent"
            with patch.object(subprocess, "run", side_effect=AssertionError("Plan executed")), contextlib.redirect_stdout(io.StringIO()) as text:
                self.assertEqual(r.main(ARGS + ["--output", str(path)]), 0)
            self.assertFalse(json.loads(text.getvalue())["execute"])
            self.assertFalse(path.exists())

    def test_literal_requirements_comments_quotes_and_blocks(self):
        for declaration in ("require " + r.MODULE + " v0.1.46 // indirect", 'require (\n"' + r.MODULE + '" v0.1.47\n)',
                            "// " + r.MODULE + " v99.0.0\nrequire (\nother/module v1.0.0\n" + r.MODULE + " v0.1.46\n)"):
            self.assertIn(r.dependency(declaration), ("v0.1.46", "v0.1.47"))

    def test_replaced_excluded_duplicate_and_unknown_requirements_are_not_guessed(self):
        normal = "require " + r.MODULE + " v0.1.46\n"
        for suffix in ("replace " + r.MODULE + " => ../local", "replace (\n" + r.MODULE + " v0.1.46 => other/repo v0.1.47\n)",
                       "exclude " + r.MODULE + " v0.1.46", normal, "require " + r.MODULE + " master"):
            with self.subTest(suffix=suffix), self.assertRaises(r.Unverified):
                r.dependency(normal + suffix)
        with self.assertRaises(r.Unverified):
            r.dependency("require unrelated v1.0.0")

    def test_platform_digest_is_exact_and_missing_or_ambiguous_never_passes(self):
        raw = manifest()
        self.assertEqual(r.image_manifest(raw, "linux/arm64")["platform_digest"], "sha256:" + "b" * 64)
        for case in ("missing", "ambiguous", "no_digest", "single"):
            changed = copy.deepcopy(raw)
            if case == "missing":
                changed["manifests"] = changed["manifests"][1:]
            elif case == "ambiguous":
                changed["manifests"].append(changed["manifests"][0])
            elif case == "no_digest":
                changed["digest"] = ""
            else:
                changed["mediaType"] = "application/vnd.oci.image.manifest.v1+json"
            with self.subTest(case=case), self.assertRaises(r.Unverified):
                r.image_manifest(changed, "linux/arm64")

    def test_all_positive_metadata_still_requires_runtime_verification(self):
        images = {role: {"metadata_available": True} for role in ("milvus", "wp_server")}
        self.assertEqual(r.decision(positive_checks(), images, []), ("requires_runtime_verification", 0))
        for checks in ([], positive_checks()[:3], [positive_checks()[0]] * 4):
            self.assertEqual(r.decision(checks, images, [])[0], "inconclusive")

    def test_diverged_backport_is_unknown_and_behind_is_not_qualified(self):
        images = {role: {"metadata_available": True} for role in ("milvus", "wp_server")}
        checks = positive_checks()
        checks[0].update(ancestry="diverged", outcome="required_commit_not_in_ancestry")
        self.assertEqual(r.decision(checks, images, []), ("inconclusive", 1))
        checks[0]["ancestry"] = "behind"
        self.assertEqual(r.decision(checks, images, []), ("source_prerequisites_not_met", 3))

    def test_server_upgrade_does_not_upgrade_the_embedded_client(self):
        checks = positive_checks()
        client = next(c for c in checks if c["role"] == "embedded_client_wp_host_provider")
        client.update(ancestry="behind", outcome="required_commit_not_in_ancestry")
        self.assertEqual(r.decision(checks, {role: {"metadata_available": True} for role in ("milvus", "wp_server")}, [])[1], 3)

    def make_check(self, folder):
        return r.Check(r.arguments(ARGS + ["--output", str(folder)]))

    def test_two_module_declarations_must_agree_without_mvs_guessing(self):
        with tempfile.TemporaryDirectory() as folder:
            check = self.make_check(folder)
            check.resolve = lambda repo, ref: {"sha": "1" * 40}
            check.ancestry = lambda *args: None
            check.source_file = lambda repo, sha, path, artifact: "require " + r.MODULE + (" v0.1.46" if path == "go.mod" else " v0.1.47")
            check.inspect_source()
            self.assertNotIn("embedded_wp_candidate", check.report["sources"])
            self.assertIn("embedded_wp_declarations_missing_or_different", [g["code"] for g in check.report["gaps"]])

    def test_metadata_errors_and_expired_budget_do_not_execute_or_become_health(self):
        with tempfile.TemporaryDirectory() as folder:
            check = self.make_check(folder)
            for error in (subprocess.TimeoutExpired("metadata", 1), OSError("private error")):
                with patch.object(subprocess, "run", side_effect=error), self.assertRaises(r.Unverified):
                    check.command(["gh", "api", "unused"])
            for value in ("null", "[]", "invalid JSON"):
                with patch.object(subprocess, "run", return_value=subprocess.CompletedProcess([], 0, value, "private error")), self.assertRaises(r.Unverified):
                    check.command(["gh", "api", "unused"])
            check.deadline = 0
            with patch.object(subprocess, "run", side_effect=AssertionError("Expired command executed")), self.assertRaises(r.Unverified):
                check.command(["gh", "api", "unused"])

    def test_known_commits_with_unverified_merge_base_are_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            check = self.make_check(folder)
            check.github = lambda *args: {"base_sha": "1" * 40, "status": "ahead", "merge_base_sha": "2" * 40}
            check.ancestry("example", r.WP, "1" * 40, "3" * 40)
            self.assertEqual(check.report["checks"][0]["outcome"], "unverified")

    def test_pseudo_version_resolves_commit_and_checks_timestamp(self):
        with tempfile.TemporaryDirectory() as folder:
            check = self.make_check(folder)
            paths = []
            def lookup(path, query):
                paths.append(path)
                return {"sha": "d4511a259ce796a5584bdef5436d2599a7906d0a", "committed_at": "2026-10-27T08:33:49Z"}
            check.github = lookup
            version = "v0.1.48-0.20261027083349-d4511a259ce7"
            self.assertEqual(check.resolve(r.WP, version)["resolution_ref"], "d4511a259ce7")
            self.assertTrue(paths[0].endswith("/commits/d4511a259ce7"))
            with self.assertRaises(r.Unverified):
                check.resolve(r.WP, "v0.1.48-0.20261027083348-d4511a259ce7")

    def test_frozen_refs_and_public_metadata_only_even_when_all_prerequisites_exist(self):
        with tempfile.TemporaryDirectory() as folder:
            check = self.make_check(folder)
            calls = []
            def command(argv):
                calls.append(argv)
                if argv[0] == "docker":
                    return manifest()
                path = argv[2]
                if "/commits/" in path:
                    return {"sha": "1" * 40, "committed_at": "2026-10-28T00:00:00Z"}
                if "/contents/" in path:
                    self.assertIn("?ref=" + "1" * 40, path)
                    content = "require " + r.MODULE + " v0.1.47\n"
                    return {"encoding": "base64", "content": base64.b64encode(content.encode()).decode(), "sha": "2" * 40}
                required = path.split("/compare/", 1)[1].split("...", 1)[0]
                return {"base_sha": required, "merge_base_sha": required, "status": "ahead"}
            check.command = command
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(check.run(), 0)
            saved = json.loads((check.folder / "release-check.json").read_text())
            self.assertFalse(saved["full_trace_acceptance_complete"])
            self.assertFalse(saved["runtime_source_image_binding_verified"])
            self.assertFalse(saved["kubernetes_actions"])
            self.assertFalse(saved["image_layers_downloaded"])
            self.assertEqual(saved["status"], "requires_runtime_verification")
            self.assertTrue(all(cmd[0] in ("gh", "docker") for cmd in calls))
            self.assertTrue(all("pull" not in cmd and "run" not in cmd for cmd in calls))

    def test_untrusted_cli_values_are_rejected_before_any_work(self):
        for extra in (["--milvus-ref", "--bad"], ["--wp-server-ref", "../private"], ["--wp-image", "https://user:secret@private/image"],
                      ["--milvus-image", "$(command)"], ["--budget", "0"]):
            with self.subTest(extra=extra), contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                r.arguments(ARGS + extra)


if __name__ == "__main__":
    unittest.main()
