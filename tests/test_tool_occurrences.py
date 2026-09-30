"""Regressions for repeated tool results across history windows and restarts."""
import json
from pathlib import Path
import tempfile

import omp_audit_proxy as ap
from test_readable_json import exchange


CALL_ID = "call1"
TOOL_NAME = "mcp__fixture_read"
ARGUMENTS = {"path": "file.txt"}
SCOPE = "tool-occurrence-regression"


def _anthropic_history(outputs):
    messages = [{"role": "user", "content": "repeat the same tool"}]
    for output in outputs:
        messages.extend([
            {"role": "assistant", "content": [{"type": "tool_use", "id": CALL_ID,
                "name": TOOL_NAME, "input": ARGUMENTS}]},
            {"role": "user", "content": [{"type": "tool_result", "tool_use_id": CALL_ID,
                "content": output}]},
        ])
    return messages


def _record(key, tracker, outputs=(), call=False, protocol="anthropic-messages", parent=None):
    record = exchange(key, _anthropic_history(outputs), "fixture reasoning", call=call)
    record.update(audit_session_id="occurrence-regression-session", audit_agent_id="main")
    if protocol == "openai-responses":
        items = [] if parent else [{"role": "user", "content": "repeat the same tool"}]
        for output in outputs:
            if not parent:
                items.append({"type": "function_call", "call_id": CALL_ID, "name": TOOL_NAME,
                              "arguments": json.dumps(ARGUMENTS)})
            items.append({"type": "function_call_output", "call_id": CALL_ID, "output": output})
        body = {"instructions": "fixture system", "input": items}
        if parent:
            body["previous_response_id"] = parent
        assistant = record["chatml"]["messages"][-1]
        record["protocol"] = protocol
        record["request"]["body"] = body
        record["chatml"]["messages"] = ap.derive_chatml_request(protocol, body) + [assistant]
    record["tool_trace"] = tracker.observe_request(key, protocol, record["request"]["body"], SCOPE)
    record["tool_trace"].extend(tracker.observe_calls(
        key, record["chatml"]["messages"][-1].get("tool_calls", []), SCOPE, "resp_" + key))
    return record


def _turns(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))["turns"]


def _results(turn):
    return [event["content"] for event in turn["tool_results"]]


def run_checks(directory_parent=None):
    checks = []
    with tempfile.TemporaryDirectory(prefix="tool-occurrences-", dir=directory_parent) as folder:
        root = Path(folder)

        # An old X must not consume the outstanding call that clearly returned Y.
        # Otherwise its double-counted result can suppress a later real X.
        xy_dir = root / "changed-result"
        writer, tracker = ap.SessionJsonWriter(str(xy_dir)), ap.ToolTraceTracker()
        path = writer.write(_record("xy-1", tracker, call=True))
        writer.write(_record("xy-2", tracker, ("X",), call=True))
        writer.write(_record("xy-3", tracker, ("X", "Y")))
        checks.append(("tool_occurrences_changed_output_keeps_only_new_result",
                       [_results(turn) for turn in _turns(path)] == [[], ["X"], ["Y"]]))
        writer, tracker = ap.SessionJsonWriter(str(xy_dir)), ap.ToolTraceTracker()
        writer.write(_record("xy-4", tracker, ("X", "Y"), call=True))
        writer.write(_record("xy-5", tracker, ("X",)))
        checks.append(("tool_occurrences_changed_output_restart_keeps_new_tail_result",
                       [_results(turn) for turn in _turns(path)][-2:] == [[], ["X"]]))

        # Only one result is sent in each truncated request. Restoring complete
        # history must compare against all three exported occurrences, not one.
        tail_dir = root / "tail-history"
        writer, tracker = ap.SessionJsonWriter(str(tail_dir)), ap.ToolTraceTracker()
        path = writer.write(_record("tail-0", tracker, call=True))
        for index in range(1, 4):
            writer.write(_record("tail-" + str(index), tracker, ("X",), call=index < 3))
        checks.append(("tool_occurrences_three_truncated_results_are_preserved",
                       [_results(turn) for turn in _turns(path)] == [[], ["X"], ["X"], ["X"]]))
        writer = ap.SessionJsonWriter(str(tail_dir))
        writer.write(_record("tail-full", tracker, ("X", "X", "X")))
        full_turn = _turns(path)[-1]
        checks.append(("tool_occurrences_restored_full_history_has_no_duplicates",
                       not full_turn["tool_calls"] and not full_turn["tool_results"]))
        writer.write(_record("tail-new-call", tracker, ("X", "X", "X"), call=True))
        writer.write(_record("tail-latest", tracker, ("X", "X")))
        last_results = _turns(path)[-1]["tool_results"]
        checks.append(("tool_occurrences_uncertain_result_uses_latest_history_source",
                       len(last_results) == 1 and last_results[0]["content"] == "X"
                       and last_results[0].get("source") == "request.messages[4].content[0]"
                       and last_results[0].get("provenance_uncertain") is True))

        # Responses delta inputs have local positions. Separate parents each
        # represent a new result; restored absolute history must not export them.
        responses_dir = root / "responses-history"
        writer, tracker = ap.SessionJsonWriter(str(responses_dir)), ap.ToolTraceTracker()
        path = writer.write(_record("resp-0", tracker, call=True, protocol="openai-responses"))
        for index in range(1, 4):
            writer.write(_record("resp-" + str(index), tracker, ("X",), call=index < 3,
                                 protocol="openai-responses", parent="resp_resp-" + str(index - 1)))
        checks.append(("tool_occurrences_distinct_response_parents_keep_each_result",
                       [_results(turn) for turn in _turns(path)] == [[], ["X"], ["X"], ["X"]]))
        writer, tracker = ap.SessionJsonWriter(str(responses_dir)), ap.ToolTraceTracker()
        writer.write(_record("resp-full", tracker, ("X", "X", "X"), protocol="openai-responses"))
        full_turn = _turns(path)[-1]
        checks.append(("tool_occurrences_response_deltas_restore_without_history_duplicates",
                       not full_turn["tool_calls"] and not full_turn["tool_results"]))
    return checks


if __name__ == "__main__":
    checks = run_checks()
    for name, ok in checks:
        print(("PASS " if ok else "FAIL ") + name)
    print(f"{sum(ok for _, ok in checks)}/{len(checks)} passed")
    raise SystemExit(not all(ok for _, ok in checks))
