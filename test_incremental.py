"""Lossless, incremental journal checks; all data and upstreams are local fixtures."""
import copy
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from unittest.mock import patch

import omp_audit_proxy as ap


def fixture(exchange, response="resp_1", prompt="first", history=None):
    body = {"messages": history or [{"role": "system", "content": "fixture system"},
                                     {"role": "user", "content": prompt}]}
    ids = [{"id": "request_only", "source": "header:x-request-id"}]
    if response:
        ids.append({"id": response, "source": "body:json.id"})
    return {"exchange_id": exchange, "protocol": "openai-completions", "request": {"body": body},
            "response": {"status": 200}, "classification": {"ids": ids},
            "chatml": {"messages": [*body["messages"], {"role": "assistant", "content": "answer",
                                                         "thinking": "visible reasoning"}]},
            "tool_trace": [{"event": "tool_call", "tool_name": "read", "arguments": {"path": "fixture"}}]}


def run_checks(directory_parent=None):
    checks = []
    with tempfile.TemporaryDirectory(prefix="omp-incremental-", dir=directory_parent) as temp:
        root = Path(temp)
        writer = ap.SessionJsonWriter(str(root))
        first = fixture("a")
        path = Path(writer.write(first))
        first_bytes = path.read_bytes()
        second = fixture("b", "resp_2")
        second["request"]["body"]["messages"].extend([
            {"role": "assistant", "content": "answer", "reasoning_content": "visible reasoning"},
            {"role": "user", "content": "next"}])
        writer.write(second)
        checks.append(("journal_uses_one_jsonl_for_changing_response_ids", list(root.glob("*.jsonl")) == [path]
                       and not list(root.glob("session-*-rl.json"))))
        checks.append(("journal_appends_without_rewriting_previous_bytes", path.read_bytes().startswith(first_bytes)))
        frames = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
        checks.append(("journal_groups_changing_ids_by_initial_context_crc32",
                       frames[0]["stream_id"] == frames[1]["stream_id"]
                       and frames[1]["stream_id"].startswith("crc32:")
                       and frames[1]["response_id"] == "resp_2"))
        recovered = list(ap.iter_session_records(path))
        checks.append(("journal_exactly_reconstructs_raw_thinking_chatml_and_tools", recovered == [first, second]
                       and recovered[-1]["chatml"]["messages"][-1]["thinking"] == "visible reasoning"))
        checks.append(("journal_has_explicit_think_presence", frames[0]["thinking"]["present"]
                       and frames[0]["thinking"]["characters"] == len("visible reasoning")))
        before = path.read_bytes()
        restarted = ap.SessionJsonWriter(temp)
        restarted.write(copy.deepcopy(second))
        checks.append(("journal_restart_reexport_is_idempotent", path.read_bytes() == before))
        no_id = fixture("c", None)
        restarted.write(no_id)
        third_frame = json.loads(path.read_text(encoding="utf-8").splitlines()[-1])
        checks.append(("journal_no_id_uses_crc32_not_request_id", third_frame["stream_id"] == frames[0]["stream_id"]
                       and third_frame["response_id"] is None))
        explicit = fixture("d", "resp_1", "different system/user")
        restarted.write(explicit)
        checks.append(("journal_same_response_id_wins_over_changed_context",
                       json.loads(path.read_text(encoding="utf-8").splitlines()[-1])["grouped_by"] == "response_id"))
        chained = fixture("e", "resp_3", "another prompt")
        chained["request"]["body"]["previous_response_id"] = "resp_1"
        restarted.write(chained)
        checks.append(("journal_previous_response_id_links_continuations",
                       json.loads(path.read_text(encoding="utf-8").splitlines()[-1])["grouped_by"] == "previous_response_id"))
        unknown = fixture("f", None)
        unknown["request"]["body"] = {}
        restarted.write(unknown)
        checks.append(("journal_unkeyed_is_explicitly_unclassified", unknown["stream_id"] == "unclassified:f"))
        unsafe = fixture("g", "../../outside", "unique")
        restarted.write(unsafe)
        checks.append(("journal_response_id_cannot_control_file_path", Path(restarted.path) == root / "sessions-rl.jsonl"))
        same_id_other_provider = fixture("h", "resp_1", "separate provider prompt")
        same_id_other_provider["upstream"] = "https://different.example/v1"
        restarted.write(same_id_other_provider)
        checks.append(("journal_response_aliases_are_provider_scoped",
                       same_id_other_provider["stream_id"] != first["stream_id"]))
        no_context_a, no_context_b = fixture("i", "reused"), fixture("j", "reused")
        no_context_a["request"]["body"] = {}
        no_context_b["request"]["body"] = {}
        no_context_b["upstream"] = "https://different.example/v1"
        restarted.write(no_context_a)
        restarted.write(no_context_b)
        checks.append(("journal_no_context_response_ids_are_provider_scoped",
                       no_context_a["stream_id"] != no_context_b["stream_id"]))
        conflict = root / "path-conflict"
        conflict.mkdir()
        result = subprocess.run([sys.executable, "-B", ap.__file__, "--proxy-only", "--port", "0",
                                 "--route", "/fixture/=http://127.0.0.1:1=openai-completions",
                                 "--log", str(conflict / "sessions-rl.jsonl")], capture_output=True, timeout=10)
        checks.append(("journal_rejects_index_path_alias_before_writing", result.returncode == 2
                       and not list(conflict.iterdir())))

        collisions = ap.SessionJsonWriter(str(root / "collision"))
        with patch.object(ap.zlib, "crc32", return_value=123):
            c1, c2 = fixture("c1", None, "one"), fixture("c2", None, "two")
            collisions.write(c1)
            collisions.write(c2)
        checks.append(("journal_crc32_collision_keeps_distinct_sha256_anchors", c1["stream_id"] != c2["stream_id"]))

        damaged = root / "damaged"
        damaged.mkdir()
        damaged_path = damaged / "sessions-rl.jsonl"
        damaged_path.write_bytes(first_bytes + b'{"unfinished":')
        original = damaged_path.read_bytes()
        refused = False
        try:
            ap.SessionJsonWriter(str(damaged)).write(fixture("bad"))
        except ValueError:
            refused = True
        checks.append(("journal_refuses_incomplete_tail_without_overwrite", refused and damaged_path.read_bytes() == original))

        growth = ap.SessionJsonWriter(str(root / "growth"))
        history = [{"role": "system", "content": "SYSTEM_SENTINEL_" + "S" * 50000},
                   {"role": "user", "content": "USER_SENTINEL_" + "U" * 50000}]
        expected, full_bytes = [], 0
        for i in range(12):
            item = fixture("growth-" + str(i), "response-" + str(i), history=copy.deepcopy(history))
            growth.write(item)
            expected.append(item)
            full_bytes += len(ap._canon(item).encode())
            history.extend([{"role": "assistant", "content": "answer-" + str(i), "reasoning_content": "think-" + str(i)},
                            {"role": "user", "content": "question-" + str(i)}])
        actual = list(ap.iter_session_records(growth.path))
        lines = Path(growth.path).read_text(encoding="utf-8").splitlines()
        checks.append(("journal_growth_is_delta_not_repeated_full_history", actual == expected
                       and Path(growth.path).stat().st_size < full_bytes / 4
                       and all("SYSTEM_SENTINEL_" not in line and "USER_SENTINEL_" not in line for line in lines[1:])))
        tail_size = Path(growth.path).stat().st_size
        updated = copy.deepcopy(expected[-1])
        updated["exchange_id"] = "growth-partial"
        updated["chatml"]["messages"][-1]["thinking"] += " more reasoning"
        growth.write(updated)
        checks.append(("journal_partial_response_only_appends_new_thinking",
                       Path(growth.path).stat().st_size - tail_size < 3000
                       and list(ap.iter_session_records(growth.path))[-1] == updated))
        checks.append(("journal_index_has_no_request_or_tool_payload", "request" not in ap.audit_index(updated)
                       and "tool_trace" not in ap.audit_index(updated) and ap.audit_index(updated)["thinking"]["present"]))

        concurrent = str(root / "concurrent")
        worker = ("import json,sys; from omp_audit_proxy import SessionJsonWriter; "
                  "w=SessionJsonWriter(sys.argv[1]); r=json.loads(sys.argv[2]); "
                  "[(r.update(exchange_id=sys.argv[3]+'-'+str(i)), w.write(r)) for i in range(3)]")
        processes = [subprocess.Popen([sys.executable, "-B", "-X", "utf8", "-c", worker, concurrent,
                     json.dumps(fixture("worker", None)), str(i)], cwd=os.path.dirname(ap.__file__),
                     stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                     creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0) for i in range(3)]
        try:
            results = [process.communicate(timeout=30) for process in processes]
            concurrent_rows = list(ap.iter_session_records(Path(concurrent) / "sessions-rl.jsonl"))
            checks.append(("journal_concurrent_processes_keep_every_exchange_once",
                           all(p.returncode == 0 for p in processes) and len(concurrent_rows) == 9
                           and len({r["exchange_id"] for r in concurrent_rows}) == 9))
        finally:
            for process in processes:
                if process.poll() is None:
                    process.kill()
                    process.communicate()

    cases = [({"a": [1, 2, 3]}, {"a": [1, 9, 2, 3]}),
             ({"a": [1, 2, 3]}, {"a": [1]}), ({"a": {"x": 1}}, {"a": {"y": 2}}),
             ({"a": True}, {"a": 1}), ({"a": [False]}, {"a": [0]}),
             ({"a": 1}, {"a": 1.0}), ({"a": "reason"}, {"a": "reason longer"})]
    checks.append(("journal_delta_preserves_json_types_insertions_and_deletions", all(
        ap._canon(ap.apply_json_delta(a, ap._json_delta(a, b))) == ap._canon(b) for a, b in cases)))
    history = {"messages": [{"role": "assistant", "content": [
        {"type": "thinking", "thinking": "historic thought"}, {"type": "text", "text": "answer"}]}]}
    parsed = ap.derive_chatml_request("anthropic-messages", history)[0]
    checks.append(("historic_anthropic_thinking_keeps_a_separate_field", parsed.get("thinking") == "historic thought"
                   and parsed["content"] == "answer"))
    return checks


if __name__ == "__main__":
    checks = run_checks()
    for name, ok in checks:
        print(("PASS " if ok else "FAIL ") + name)
    print(f"{sum(ok for _, ok in checks)}/{len(checks)} passed")
    raise SystemExit(not all(ok for _, ok in checks))
