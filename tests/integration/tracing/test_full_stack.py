"""Graph counterexamples; these fixtures are not deployed trace receipts."""
import contextlib
import copy
import io
import tempfile
from pathlib import Path
import unittest
from unittest.mock import patch

import full_stack as f


SERVICES = {"proxy": "proxy", "sn": "streamingnode", "wp": "wp_server"}


def fixture(tid="a"):
    return {"traceID": tid, "processes": {k: {"serviceName": k} for k in SERVICES}, "spans": []}


def append(raw, sid, process, operation, parent=None, scope=""):
    span = {"traceID": raw["traceID"], "spanID": sid, "processID": process, "operationName": operation,
            "tags": [{"key": "otel.scope.name", "value": scope}] if scope else [], "references": []}
    if parent:
        span["references"].append({"refType": "CHILD_OF", "traceID": raw["traceID"], "spanID": parent})
    raw["spans"].append(span)
    return span


def connected():
    raw = fixture()
    append(raw, "p", "proxy", "Proxy-Insert")
    append(raw, "s", "sn", "wal.appendimpl", "p", "milvus.streaming.wal")
    append(raw, "c", "sn", "Write", "s", "LogWriter")
    append(raw, "w", "wp", "AddEntry", "c", "LogStore")
    return raw


class GraphTests(unittest.TestCase):
    def test_jaeger_process_scope_is_retained_without_guessing_from_service(self):
        raw = connected()
        raw["spans"][1].update(processID="wal-process", tags=[])
        raw["processes"]["wal-process"] = {"serviceName": "sn", "tags": [{"key": "otel.library.name", "value": "milvus.streaming.wal"}]}
        self.assertIsNotNone(f.child_proof(raw, SERVICES))
        raw["spans"][1]["tags"] = [{"key": "otel.scope.name", "value": "AppendOp"}]
        self.assertIsNone(f.child_proof(raw, SERVICES))

    def test_actual_wal_and_client_ancestry_are_required(self):
        raw = connected()
        proof = f.child_proof(raw, SERVICES)
        self.assertEqual(proof["span_path"], ["p", "s", "c", "w"])
        for mutate in (lambda r: r["spans"][1].update(tags=[]),
                       lambda r: r["spans"][1].update(operationName="Execute", tags=[{"key": "otel.scope.name", "value": "AppendOp"}]),
                       lambda r: r["spans"][2].update(processID="wp"),
                       lambda r: r["spans"][3].update(references=[]),
                       lambda r: r["spans"][3].update(operationName="ReadBatch")):
            bad = copy.deepcopy(raw)
            mutate(bad)
            self.assertIsNone(f.child_proof(bad, SERVICES))

    def test_service_presence_or_follows_from_is_not_serial_ancestry(self):
        raw = connected()
        raw["spans"][3]["references"][0]["refType"] = "FOLLOWS_FROM"
        self.assertIsNone(f.child_proof(raw, SERVICES))

    def test_exact_batch_link_to_a_retained_input_is_required(self):
        raw = connected()
        raw["spans"].pop()
        batch = fixture("b")
        root = append(batch, "batch", "sn", "Execute", scope="BatchAppendOp")
        root["references"] = [{"refType": "FOLLOWS_FROM", "traceID": "a", "spanID": "c"}]
        append(batch, "w", "wp", "AddEntries", "batch", "LogStore")
        proof = f.linked_proof(raw, batch, SERVICES)
        self.assertEqual(proof["kind"], "async_link")
        self.assertEqual(proof["input_span_id"], "c")
        self.assertIsNone(f.child_proof(batch, SERVICES))
        for key, value in (("traceID", "unrelated"), ("spanID", "unretained"), ("refType", "CHILD_OF")):
            bad = copy.deepcopy(batch)
            bad["spans"][0]["references"][0][key] = value
            self.assertIsNone(f.linked_proof(raw, bad, SERVICES))
        batch["spans"][0]["processID"] = "wp"
        self.assertIsNone(f.linked_proof(raw, batch, SERVICES))

    def test_batch_link_to_proxy_alone_cannot_invent_wal_or_client(self):
        raw = connected()
        batch = fixture("b")
        root = append(batch, "batch", "sn", "Execute", scope="BatchAppendOp")
        root["references"] = [{"refType": "FOLLOWS_FROM", "traceID": "a", "spanID": "p"}]
        append(batch, "w", "wp", "AddEntries", "batch", "LogStore")
        self.assertIsNone(f.linked_proof(raw, batch, SERVICES))

    def test_conflicting_scope_aliases_cannot_supply_wal(self):
        raw = connected()
        raw["spans"][1]["tags"].append({"key": "otel.library.name", "value": "AppendOp"})
        self.assertIsNone(f.child_proof(raw, SERVICES))

    def test_corrupt_duplicate_cycles_and_multiple_parents_fail(self):
        for kind in ("duplicate", "cycle", "parents", "identity"):
            raw = connected()
            if kind == "duplicate":
                span = copy.deepcopy(raw["spans"][1])
                span["operationName"] = "other"
                raw["spans"].append(span)
            elif kind == "cycle":
                raw["spans"][0]["references"] = [{"refType": "CHILD_OF", "traceID": "a", "spanID": "w"}]
            elif kind == "parents":
                raw["spans"][3]["references"].append({"refType": "CHILD_OF", "traceID": "a", "spanID": "p"})
            else:
                raw["spans"][0]["traceID"] = "unrelated"
            with self.subTest(kind=kind), self.assertRaises(RuntimeError):
                f.graph(raw)


class PlanTests(unittest.TestCase):
    def test_composed_equivalence_keeps_target_and_uncertainty(self):
        finding = {"id": "one-run", "rule_id": "trace.relationship_coverage", "rule_version": 1,
                   "outcome": "partial_observed_path", "severity": "unknown", "evidence_strength": "unavailable",
                   "flow": "write", "stage": "append", "target": {"trace_id": "a"},
                   "summary": "Missing retained parents", "unknown": ["parent_not_retained"]}
        original = {"results": [{"findings": [finding]}]}
        composed = copy.deepcopy(original)
        composed["results"][0]["findings"][0]["id"] = "another-run"
        self.assertEqual(f.signatures(original), f.signatures(composed))
        for key, value in (("unknown", []), ("target", {"trace_id": "unrelated"}), ("stage", "consume")):
            different = copy.deepcopy(composed)
            different["results"][0]["findings"][0][key] = value
            self.assertNotEqual(f.signatures(original), f.signatures(different))

    def test_plan_is_offline_and_creates_no_artifacts(self):
        with tempfile.TemporaryDirectory() as directory:
            out = Path(directory) / "absent"
            with patch.object(f, "run", side_effect=AssertionError("No execution")), contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(f.main(["--context", "example", "--namespace", "fixture", "--output", str(out)]), 0)
            self.assertFalse(out.exists())

    def test_workload_bounds_rejected_before_opt_in(self):
        for args in (["--requests", "33"], ["--rows", "0"], ["--dimension", "129"], ["--budget", "119"], ["--concurrency", "9"], ["--namespace", "--bad"]):
            with self.subTest(args=args), contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                f.arguments(["--context", "example", "--namespace", "fixture"] + args)


if __name__ == "__main__":
    unittest.main()
