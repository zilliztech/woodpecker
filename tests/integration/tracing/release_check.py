#!/usr/bin/env python3
"""Read-only source/image prerequisites for #459; offline plan by default.

This does not execute an image, alter Kubernetes, resolve the complete Go build
graph or certify runtime trace coverage. Cherry-picks/reverts and source/image
binding need separate review; the full-stack workload is the actual acceptance.
"""
import argparse
import base64
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import shlex
import subprocess
import time
import urllib.parse
import uuid

MILVUS = "milvus-io/milvus"
WP = "zilliztech/woodpecker"
MODULE = "github.com/zilliztech/woodpecker"
FIXES = {"milvus_wal_context": "5eebaa9ad4ee5eadd82071cecc1b839df9cf1441",
         "wp_queued_append_and_rpc": "c7e4185da64ea5bcac3f988ace4c10b819644383",
         "wp_host_provider": "d4511a259ce796a5584bdef5436d2599a7906d0a",
         "wp_server_trace_baseline": "c45123a65de000be22d5d981e649da5c6e73e5c4"}
SHA = re.compile(r"[0-9a-f]{40}\Z")
DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
VERSION = re.compile(r"v[0-9]+\.[0-9]+\.[0-9]+(?:-[A-Za-z0-9.-]+)?(?:\+incompatible)?\Z")
PSEUDO = re.compile(r"v[0-9]+\.[0-9]+\.[0-9]+-(?:[A-Za-z0-9.-]+\.)?([0-9]{14})-([0-9a-f]{12})(?:\+incompatible)?\Z")
REQUIRED_ROLES = {"milvus_wal_context", "embedded_client_wp_queued_append_and_rpc",
                  "embedded_client_wp_host_provider", "wp_server_trace_baseline"}


class Unverified(RuntimeError):
    pass


def dependency(text):
    """Only qualify one literal, unreplaced WP requirement; ambiguity fails shut.

    This is a deliberately narrow declaration reader, not a Go MVS resolver.
    Unknown relevant syntax, exclusions and replacements require build review.
    """
    block, versions = None, []
    for original in text.splitlines():
        line = original.split("//", 1)[0].strip()
        if not line:
            continue
        if line == ")":
            block = None
            continue
        start = re.fullmatch(r"(require|replace|exclude)\s*\(", line)
        if start:
            block = start[1]
            continue
        if MODULE not in line:
            continue
        try:
            tokens = shlex.split(line)
        except ValueError:
            raise Unverified("unparsed_wp_module_declaration") from None
        kind, row = (block, tokens) if block else (tokens[0], tokens[1:])
        if kind in ("replace", "exclude"):
            raise Unverified("wp_module_replacement_or_exclusion")
        if kind != "require" or len(row) != 2 or row[0] != MODULE or not VERSION.fullmatch(row[1]):
            raise Unverified("unparsed_wp_module_declaration")
        versions.append(row[1])
    if len(versions) != 1:
        raise Unverified("missing_or_duplicate_wp_requirement")
    return versions[0]


def image_manifest(value, platform):
    if not isinstance(value, dict) or not isinstance(value.get("digest"), str) or not DIGEST.fullmatch(value["digest"]):
        raise Unverified("image_digest_unavailable")
    if value.get("mediaType") not in ("application/vnd.docker.distribution.manifest.list.v2+json", "application/vnd.oci.image.index.v1+json"):
        raise Unverified("single_manifest_platform_requires_separate_verification")
    parts = platform.split("/")
    manifests = value.get("manifests")
    if not isinstance(manifests, list) or any(not isinstance(m, dict) or not isinstance(m.get("platform"), dict) for m in manifests):
        raise Unverified("image_index_shape_unverified")
    matches = [m for m in manifests if m.get("platform", {}).get("os") == parts[0]
               and m.get("platform", {}).get("architecture") == parts[1]
               and (len(parts) == 2 or m.get("platform", {}).get("variant") == parts[2])]
    if len(matches) != 1 or not isinstance(matches[0].get("digest"), str) or not DIGEST.fullmatch(matches[0]["digest"]):
        raise Unverified("requested_platform_missing_or_ambiguous")
    return {"index_digest": value["digest"], "platform_digest": matches[0]["digest"], "platform": matches[0]["platform"]}


def decision(checks, images, gaps):
    # A known ancestor does not prove a semantic revert is absent. Conversely,
    # divergence does not prove absence of a cherry-picked equivalent fix.
    missing = any(c.get("ancestry") == "behind" for c in checks)
    incomplete = (len(checks) != 4 or {c.get("role") for c in checks} != REQUIRED_ROLES or
                  any(c.get("outcome") != "required_commit_in_ancestry" or c.get("ancestry") not in ("ahead", "identical") for c in checks))
    complete_images = set(images) == {"milvus", "wp_server"} and all(i.get("metadata_available") is True for i in images.values())
    if missing:
        return "source_prerequisites_not_met", 3
    if incomplete or not complete_images or gaps:
        return "inconclusive", 1
    return "requires_runtime_verification", 0


class Check:
    def __init__(self, args):
        self.a = args
        self.deadline = time.monotonic() + args.budget
        self.calls = 0
        self.folder = args.output / (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8])
        self.folder.mkdir(parents=True, mode=0o700)
        self.report = {"schema_version": "wpdiag.trace-release-check/v1", "started_at": datetime.now(timezone.utc).isoformat(),
                       "selection": {"milvus_ref": args.milvus_ref, "wp_server_ref": args.wp_server_ref,
                                     "platform": args.platform}, "sources": {}, "checks": [], "images": {}, "gaps": [],
                       "full_trace_acceptance_complete": False, "runtime_source_image_binding_verified": False,
                       "limits": {"metadata_calls": 24, "command_seconds": 40, "work_seconds": args.budget},
                       "remaining_verification": ["Actual embedded dependency/build provenance and digest-pinned runtime images",
                           "Equivalent cherry-picks/reverts, alternate modules/workspaces/build flags are not certified by ancestry",
                           "Real single/batch/slow/recovery, query/search and retained parent/link witnesses in FULL_STACK.md"],
                       "kubernetes_actions": False, "image_layers_downloaded": False}
        self.cache = {}

    def save(self, name, value):
        raw = (json.dumps(value, indent=2, allow_nan=False) + "\n").encode()
        if len(raw) > 2 << 20 or sum(p.stat().st_size for p in self.folder.iterdir() if p.is_file()) + len(raw) > 16 << 20:
            raise Unverified("artifact_byte_budget")
        (self.folder / name).write_bytes(raw)

    def command(self, argv):
        self.calls += 1
        remaining = self.deadline - time.monotonic()
        if self.calls > 24 or remaining <= 0:
            raise Unverified("metadata_call_or_time_budget")
        try:
            p = subprocess.run(argv, capture_output=True, text=True, timeout=min(40, remaining))
        except (OSError, subprocess.TimeoutExpired):
            raise Unverified("metadata_command_unavailable_or_timed_out") from None
        # Do not publish raw error bodies, gh auth state or Docker config.
        if p.returncode or len(p.stdout.encode()) > 2 << 20:
            raise Unverified("metadata_lookup_failed")
        try:
            value = json.loads(p.stdout)
            if not isinstance(value, dict):
                raise ValueError("Expected metadata object")
            return value
        except ValueError:
            raise Unverified("invalid_metadata_response") from None

    def github(self, path, query):
        return self.command(["gh", "api", "repos/" + path, "--jq", query])

    def resolve(self, repo, ref):
        key = (repo, ref)
        if key not in self.cache:
            pseudo = PSEUDO.fullmatch(ref) if repo == WP else None
            lookup = pseudo[2] if pseudo else ref
            info = self.github(repo + "/commits/" + urllib.parse.quote(lookup, safe=""),
                               "{sha, committed_at: .commit.committer.date, html_url}")
            if not isinstance(info.get("sha"), str) or not SHA.fullmatch(info["sha"]):
                raise Unverified("source_ref_not_resolved")
            if pseudo:
                try:
                    committed = datetime.fromisoformat(info["committed_at"].replace("Z", "+00:00"))
                except (KeyError, ValueError, TypeError, AttributeError):
                    raise Unverified("pseudo_version_timestamp_unverified") from None
                if not info["sha"].startswith(lookup) or committed.astimezone(timezone.utc).strftime("%Y%m%d%H%M%S") != pseudo[1]:
                    raise Unverified("pseudo_version_commit_or_timestamp_mismatch")
            self.cache[key] = dict(info, repository=repo, requested_ref=ref, resolution_ref=lookup)
        return self.cache[key]

    def source_file(self, repo, sha, path, artifact):
        data = self.github(repo + "/contents/" + path + "?ref=" + sha, "{encoding,content,sha,path}")
        if (data.get("encoding") != "base64" or not isinstance(data.get("content"), str) or
            not isinstance(data.get("sha"), str) or not SHA.fullmatch(data["sha"])):
            raise Unverified("source_content_encoding_unverified")
        try:
            raw = base64.b64decode("".join(data["content"].split()), validate=True)
            text = raw.decode()
        except (ValueError, UnicodeError):
            raise Unverified("source_content_unreadable") from None
        if len(raw) > 256 << 10:
            raise Unverified("source_file_byte_budget")
        self.save(artifact, {"repository": repo, "commit": sha, "path": path,
                             "git_blob": data["sha"], "sha256": hashlib.sha256(raw).hexdigest(), "content": text})
        return text

    def ancestry(self, role, repo, required, target):
        item = {"role": role, "repository": repo, "required_commit": required, "target_commit": target}
        try:
            info = self.github(repo + "/compare/" + required + "..." + target,
                "{status, base_sha:.base_commit.sha, merge_base_sha:.merge_base_commit.sha, ahead_by, behind_by}")
            if info.get("base_sha") != required or info.get("status") not in ("ahead", "behind", "identical", "diverged"):
                raise Unverified("ancestry_response_unverified")
            if info["status"] in ("ahead", "identical") and info.get("merge_base_sha") != required:
                raise Unverified("ancestor_merge_base_unverified")
            item.update(ancestry=info["status"], comparison=info,
                        outcome="required_commit_in_ancestry" if info["status"] in ("ahead", "identical") else "required_commit_not_in_ancestry")
            if info["status"] == "diverged":
                item["uncertainty"] = "Equivalent release-branch cherry-picks/backports require source review; divergence is not proof of incompatibility"
        except Unverified as error:
            item.update(outcome="unverified", problem=str(error))
        self.report["checks"].append(item)

    def inspect_source(self):
        milvus = self.resolve(MILVUS, self.a.milvus_ref)
        self.report["sources"]["milvus"] = milvus
        self.ancestry("milvus_wal_context", MILVUS, FIXES["milvus_wal_context"], milvus["sha"])
        versions = {}
        for path, name in (("go.mod", "milvus-root-go.mod.json"), ("pkg/go.mod", "milvus-pkg-go.mod.json")):
            try:
                versions[path] = dependency(self.source_file(MILVUS, milvus["sha"], path, name))
            except Unverified as error:
                self.report["gaps"].append({"source": path, "code": str(error)})
        self.report["sources"]["milvus"]["declared_wp_requirements"] = versions
        if len(versions) == 2 and len(set(versions.values())) == 1:
            version = next(iter(versions.values()))
            client = self.resolve(WP, version)
            self.report["sources"]["embedded_wp_candidate"] = client
            for fix in ("wp_queued_append_and_rpc", "wp_host_provider"):
                self.ancestry("embedded_client_" + fix, WP, FIXES[fix], client["sha"])
        else:
            self.report["gaps"].append({"code": "embedded_wp_declarations_missing_or_different", "message": "Go MVS/workspace/replacement review required; no guessed effective version"})
        server = self.resolve(WP, self.a.wp_server_ref)
        self.report["sources"]["wp_server"] = server
        # #479 changes queued/client RPC handling, not the server. A newer
        # server image cannot update Milvus's client; conversely do not require
        # client-only commits on an otherwise supported server trace baseline.
        self.ancestry("wp_server_trace_baseline", WP, FIXES["wp_server_trace_baseline"], server["sha"])

    def run(self):
        try:
            self.inspect_source()
        except Unverified as error:
            self.report["gaps"].append({"code": str(error), "source": "source_metadata"})
        for role, image in (("milvus", self.a.milvus_image), ("wp_server", self.a.wp_image)):
            item = {"reference": image, "metadata_available": False}
            try:
                manifest = self.command(["docker", "buildx", "imagetools", "inspect", image, "--format", "{{json .Manifest}}"])
                self.save(role + "-manifest.json", manifest)
                item.update(image_manifest(manifest, self.a.platform), metadata_available=True)
            except Unverified as error:
                item["problem"] = str(error)
            self.report["images"][role] = item
        # Require all four checks, not an empty subset after an early failure.
        if len(self.report["checks"]) != 4:
            self.report["gaps"].append({"code": "required_source_checks_incomplete"})
        status, code = decision(self.report["checks"], self.report["images"], self.report["gaps"])
        self.report.update(status=status, metadata_calls=self.calls, finished_at=datetime.now(timezone.utc).isoformat())
        self.save("release-check.json", self.report)
        print(json.dumps({"status": status, "full_trace_acceptance_complete": False,
                          "output": str(self.folder), "source_checks": self.report["checks"], "images": self.report["images"]}, indent=2))
        return code


def arguments(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--milvus-ref", required=True)
    p.add_argument("--wp-server-ref", required=True)
    p.add_argument("--milvus-image", required=True)
    p.add_argument("--wp-image", required=True)
    p.add_argument("--platform", choices=("linux/arm64", "linux/amd64", "linux/arm64/v8"), default="linux/arm64")
    p.add_argument("--output", type=Path, default=Path("/tmp/wp-trace-release-459"))
    p.add_argument("--budget", type=int, default=180)
    p.add_argument("--execute", action="store_true")
    a = p.parse_args(argv)
    for ref in (a.milvus_ref, a.wp_server_ref):
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._/-]{0,199}", ref) or ".." in ref.split("/"):
            p.error("Use an explicit bounded source ref or SHA")
    for image in (a.milvus_image, a.wp_image):
        if "://" in image or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:/@-]{0,511}", image) or "@" in image and not DIGEST.fullmatch(image.rsplit("@", 1)[1]):
            p.error("Use an image tag or sha256 reference without credentials or URLs")
    if not 60 <= a.budget <= 360:
        p.error("budget must be 60..360 seconds")
    return a


def main(argv=None):
    a = arguments(argv)
    if not a.execute:
        print(json.dumps({"execute": False, "issue": 459, "milvus_ref": a.milvus_ref, "wp_server_ref": a.wp_server_ref,
            "milvus_image": a.milvus_image, "wp_image": a.wp_image, "platform": a.platform, "required_commits": FIXES,
            "reads": "GitHub fixed repositories: immutable source refs/two go.mod declarations and ancestry; registry manifest metadata",
            "does_not": ["download image layers", "run binaries", "operate Kubernetes", "certify actual embedded modules or full trace coverage"]}, indent=2))
        return 0
    return Check(a).run()


if __name__ == "__main__":
    raise SystemExit(main())
