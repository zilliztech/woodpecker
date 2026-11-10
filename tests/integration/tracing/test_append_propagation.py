"""Counterexamples for raw export validation; no backend or diagnostic command."""
import copy
import unittest

import append_propagation as export


def fixture(batch=False):
    tid, parent, caller, server, execution = "a" * 32, "1" * 16, "2" * 16, "3" * 16, "4" * 16
    upstream = "b" * 32 if batch else tid
    receipt = {"case": "batch" if batch else "single", "execution_trace_id": tid,
               "input_trace_ids": [upstream], "input_spans": [{"trace_id": upstream, "span_id": parent}]}
    ref = lambda kind, trace, span: {"refType": kind, "traceID": trace, "spanID": span}
    def span(sid, name, process, refs=(), kind=None):
        return {"traceID": tid, "spanID": sid, "operationName": name, "processID": process,
                "references": list(refs), "tags": [{"key": "span.kind", "value": kind}] if kind else []}
    spans = [span(execution, "Execute", "client", [ref("FOLLOWS_FROM" if batch else "CHILD_OF", upstream, parent)]),
             span(caller, "AddEntry", "client", [ref("CHILD_OF", tid, execution)], "client"),
             span(server, "AddEntry", "server", [ref("CHILD_OF", tid, caller)], "server")]
    if not batch:
        spans.append(span(parent, "write", "client"))
    trace = {"traceID": tid, "spans": spans, "processes": {
        "client": {"serviceName": "wp-client-trace-fixture"}, "server": {"serviceName": "wp-server-trace-fixture"}}}
    return receipt, trace


class ExportTests(unittest.TestCase):
    def test_single_error_and_batch_preserve_identity(self):
        for case in ("single", "single-error", "batch"):
            receipt, trace = fixture(case == "batch")
            receipt["case"] = case
            self.assertEqual(export.validate_trace(receipt, trace)["propagation_status"], "passed")

    def test_missing_client_parent_and_cross_trace_parent_fail(self):
        receipt, trace = fixture()
        for mutate in (
                lambda t: t["spans"].pop(1),
                lambda t: t["spans"][2]["references"][0].update(traceID="c" * 32),
                lambda t: t["spans"][1]["tags"].clear(),
                lambda t: t["spans"][2]["references"].clear(),
                lambda t: t["processes"]["server"].update(serviceName="unrelated")):
            with self.subTest(mutate=mutate):
                broken = copy.deepcopy(trace)
                mutate(broken)
                with self.assertRaises(AssertionError):
                    export.validate_trace(receipt, broken)

    def test_batch_links_cannot_be_lost_or_replaced_with_serial_parent(self):
        receipt, trace = fixture(True)
        for refs in ([], [{"refType": "CHILD_OF", "traceID": "a" * 32, "spanID": "2" * 16}]):
            with self.subTest(refs=refs):
                broken = copy.deepcopy(trace)
                broken["spans"][0]["references"] = refs
                with self.assertRaises(AssertionError):
                    export.validate_trace(receipt, broken)


if __name__ == "__main__":
    unittest.main()
