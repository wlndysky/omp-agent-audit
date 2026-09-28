"""Verify the JSON users open, including THINK and complete tool payloads."""
import copy
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from unittest.mock import patch

import omp_audit_proxy as ap


def exchange(key, messages, thinking, content=None, call=False, result=False, system="fixture system"):
    body = {"system": system, "messages": copy.deepcopy(messages)}
    assistant = {"role": "assistant", "thinking": thinking, "content": content}
    trace = []
    if call:
        assistant["tool_calls"] = [{"id": "call1", "name": "mcp__fixture_read", "arguments": {"path": "file.txt"}}]
    if call or result:
        trace.append({"event": "tool_call" if call else "tool_call_context", "tool_call_id": "call1",
                      "tool_name": "mcp__fixture_read", "arguments": {"path": "file.txt"}})
    if result:
        trace.append({"event": "tool_result", "tool_call_id": "call1", "tool_name": "mcp__fixture_read",
                      "arguments": {"path": "file.txt"}, "content": "工具返回全文\nsecond line", "is_error": False})
    return {"exchange_id": key, "protocol": "anthropic-messages", "request": {"body": body},
            "response": {"status": 200, "sse": True},
            "classification": {"ids": [{"id": "resp_" + key, "source": "body:json.id"}]},
            "chatml": {"messages": ap.derive_chatml_request("anthropic-messages", body) + [assistant]},
            "tool_trace": trace}


def run_checks(directory_parent=None):
    checks = []
    with tempfile.TemporaryDirectory(prefix="readable-json-", dir=directory_parent) as folder:
        root = Path(folder)
        writer = ap.SessionJsonWriter(folder)
        messages = [{"role": "user", "content": "first prompt"}]
        first = exchange("a", messages, "第一段完整思考\nnext line", call=True)
        path = Path(writer.write(first))
        first_bytes = path.read_bytes()
        document = json.loads(first_bytes)
        turn = document["turns"][0]
        checks.append(("readable_output_is_real_json_not_renamed_jsonl", path.suffix == ".json"
                       and document["schema_version"] == 3 and not list(root.glob("*.jsonl"))))
        checks.append(("readable_thinking_is_full_text_at_turn_top_level", turn["thinking"] == "第一段完整思考\nnext line"))
        checks.append(("readable_tool_call_has_name_and_full_arguments",
                       turn["tool_calls"][0]["tool_name"] == "mcp__fixture_read"
                       and turn["tool_calls"][0]["arguments"] == {"path": "file.txt"}))
        messages += [{"role": "assistant", "content": [
            {"type": "thinking", "thinking": "第一段完整思考\nnext line"},
            {"type": "tool_use", "id": "call1", "name": "mcp__fixture_read", "input": {"path": "file.txt"}}]},
            {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "call1", "content": "工具返回全文\nsecond line"}]}]
        second = exchange("b", messages, "第二段完整思考", "answer", result=True)
        writer.write(second)
        document = json.loads(path.read_text(encoding="utf-8"))
        turn2 = document["turns"][1]
        checks.append(("readable_tool_result_has_actual_content_arguments_and_status",
                       turn2["tool_results"][0]["content"] == "工具返回全文\nsecond line"
                       and turn2["tool_results"][0]["arguments"] == {"path": "file.txt"}
                       and turn2["tool_results"][0]["is_error"] is False))
        checks.append(("readable_new_turn_does_not_repeat_first_user_or_assistant_thought",
                       not turn2["input_messages"] and "第一段完整思考" not in json.dumps(turn2, ensure_ascii=False)))
        checks.append(("readable_append_keeps_all_existing_turn_bytes",
                       path.read_bytes().startswith(first_bytes[:-len(writer.FOOTER)])))
        checks.append(("readable_file_has_no_patch_operations_or_payload_references",
                       all("changes" not in t and "request" not in t for t in document["turns"])
                       and all("thinking" in t and "tool_calls" in t and "tool_results" in t for t in document["turns"])))
        before = path.read_bytes()
        ap.SessionJsonWriter(folder).write(copy.deepcopy(second))
        checks.append(("readable_restart_duplicate_export_does_not_rewrite_or_repeat", path.read_bytes() == before))
        messages += [{"role": "assistant", "content": [
            {"type": "thinking", "thinking": "第二段完整思考"}, {"type": "text", "text": "answer"}]},
            {"role": "user", "content": "continue"}]
        third = exchange("c", messages, "第三段", "another answer", result=True)
        ap.SessionJsonWriter(folder).write(third)
        document = json.loads(path.read_text(encoding="utf-8"))
        turn3 = document["turns"][2]
        checks.append(("readable_replayed_tools_are_not_duplicated", not turn3["tool_calls"] and not turn3["tool_results"]))
        checks.append(("readable_new_user_input_is_kept", turn3["input_messages"][0]["content"] == "continue"))
        checks.append(("readable_internal_evidence_keeps_every_original_exchange",
                       [r["exchange_id"] for r in ap.iter_session_records(folder)] == ["a", "b", "c"]))
        path.write_bytes(path.read_bytes()[:-4])
        ap.SessionJsonWriter(folder).write(copy.deepcopy(third))
        checks.append(("readable_interrupted_footer_recovers_from_durable_evidence",
                       json.loads(path.read_text(encoding="utf-8")) == document))
        path.unlink()
        ap.SessionJsonWriter(folder).write(copy.deepcopy(third))
        checks.append(("readable_deleted_view_is_rebuilt_without_losing_thinking",
                       json.loads(path.read_text(encoding="utf-8")) == document))
        modified = copy.deepcopy(document)
        modified["turns"][0]["thinking"] = "manual edit"
        path.write_text(json.dumps(modified, ensure_ascii=False), encoding="utf-8")
        refused = False
        try:
            ap.SessionJsonWriter(folder).write(copy.deepcopy(third))
        except ValueError:
            refused = True
        checks.append(("readable_valid_manual_changes_are_not_silently_overwritten", refused
                       and json.loads(path.read_text(encoding="utf-8"))["turns"][0]["thinking"] == "manual edit"))

        failure_dir = root / "failure"
        failed_writer = ap.SessionJsonWriter(str(failure_dir))
        with patch.object(failed_writer, "_write_document", side_effect=OSError("synthetic export failure")):
            try:
                failed_writer.write(copy.deepcopy(first))
            except OSError:
                pass
        repaired = Path(ap.SessionJsonWriter(str(failure_dir)).write(copy.deepcopy(first)))
        checks.append(("readable_export_failure_preserves_durable_thinking",
                       json.loads(repaired.read_text(encoding="utf-8"))["turns"][0]["thinking"] == first["chatml"]["messages"][-1]["thinking"]))

        cache_dir = root / "cache-hints"
        cache_writer = ap.SessionJsonWriter(str(cache_dir))
        cached_messages = [{"role": "user", "content": [
            {"type": "text", "text": "fixture reminder"},
            {"type": "text", "text": "CACHE_PROMPT_SENTINEL", "cache_control": {"type": "ephemeral"}}]}]
        cached_system = [{"type": "text", "text": "fixture system"}]
        cached_first = exchange("cache-a", cached_messages, "cache thought", call=True, system=cached_system)
        original_body = copy.deepcopy(cached_first["request"]["body"])
        cache_path = Path(cache_writer.write(cached_first))
        cached_bytes = cache_path.read_bytes()
        moved_messages = copy.deepcopy(cached_messages)
        del moved_messages[0]["content"][1]["cache_control"]
        moved_messages += [{"role": "assistant", "content": [
            {"type": "thinking", "thinking": "cache thought"},
            {"type": "tool_use", "id": "call1", "name": "mcp__fixture_read", "input": {"path": "file.txt"}}]},
            {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "call1",
                                           "content": "工具返回全文\nsecond line", "cache_control": {"type": "ephemeral"}}]}]
        moved_system = [{**cached_system[0], "cache_control": {"type": "ephemeral", "ttl": "1h"}}]
        cached_second = exchange("cache-b", moved_messages, "next thought", "answer", result=True, system=moved_system)
        next_path = Path(ap.SessionJsonWriter(str(cache_dir)).write(cached_second))
        cache_doc = json.loads(next_path.read_text(encoding="utf-8"))
        checks.append(("readable_cache_hint_movement_does_not_split_conversation", cache_path == next_path
                       and len(list(cache_dir.glob("session-*-rl.json"))) == 1 and len(cache_doc["turns"]) == 2))
        checks.append(("readable_cache_hint_change_keeps_incremental_append", next_path.read_bytes().startswith(
                       cached_bytes[:-len(cache_writer.FOOTER)]) and not cache_doc["turns"][1]["input_messages"]
                       and not cache_doc["turns"][1]["tool_calls"]))
        checks.append(("readable_cache_hints_remain_intact_in_raw_evidence",
                       cached_first["request"]["body"] == original_body
                       and [record["request"]["body"] for record in ap.iter_session_records(str(cache_dir))]
                       == [cached_first["request"]["body"], cached_second["request"]["body"]]))

        semantic = copy.deepcopy(original_body)
        semantic["messages"][0]["content"][1]["text"] += " changed"
        nested_a = {"messages": [{"role": "user", "content": [
            {"type": "image", "source": {"cache_control": "actual payload A"}}]}]}
        nested_b = copy.deepcopy(nested_a)
        nested_b["messages"][0]["content"][0]["source"]["cache_control"] = "actual payload B"
        checks.append(("context_normalization_preserves_real_text_and_nested_payload_changes",
                       ap.initial_context("anthropic-messages", semantic) != ap.initial_context("anthropic-messages", original_body)
                       and ap.initial_context("anthropic-messages", nested_a) != ap.initial_context("anthropic-messages", nested_b)))

        legacy_dir = root / "legacy-cache-hints"
        normalized_context = ap.initial_context
        def legacy_context(protocol, body):
            if protocol != "anthropic-messages":
                return normalized_context(protocol, body)
            return {"system": body.get("system"), "first_user": next(
                (message.get("content") for message in body.get("messages", []) if message.get("role") == "user"), None)}
        with patch.object(ap, "initial_context", side_effect=legacy_context):
            legacy_path = Path(ap.SessionJsonWriter(str(legacy_dir)).write(copy.deepcopy(cached_first)))
        legacy_bytes = legacy_path.read_bytes()
        resumed_path = Path(ap.SessionJsonWriter(str(legacy_dir)).write(copy.deepcopy(cached_second)))
        checks.append(("readable_restart_reuses_legacy_cached_anchor_filename", resumed_path == legacy_path
                       and resumed_path.read_bytes().startswith(legacy_bytes[:-len(cache_writer.FOOTER)])
                       and len(json.loads(resumed_path.read_text(encoding="utf-8"))["turns"]) == 2))

        metadata_dir = root / "http-metadata"
        metadata = {"exchange_id": "usage-query", "method": "GET", "path": "/usages",
                    "protocol": "openai-completions", "request": {"body_bytes": 0},
                    "response": {"status": 200, "body": {"usages": {"remaining": 10}}},
                    "chatml": {"messages": [{"role": "assistant", "content": None, "thinking": None}]},
                    "tool_trace": []}
        metadata_writer = ap.SessionJsonWriter(str(metadata_dir))
        evidence_path = Path(metadata_writer.write(metadata))
        checks.append(("usage_queries_do_not_create_empty_public_json", not list(metadata_dir.glob("*.json"))
                       and evidence_path.parent == metadata_dir / ".audit-state"))
        replayed_metadata = list(ap.iter_session_records(str(metadata_dir)))
        checks.append(("usage_query_response_is_preserved_in_internal_evidence", len(replayed_metadata) == 1
                       and replayed_metadata[0]["response"] == metadata["response"]))
        ap.SessionJsonWriter(str(metadata_dir)).write(copy.deepcopy(metadata))
        checks.append(("usage_query_restart_does_not_recreate_empty_public_json", not list(metadata_dir.glob("*.json"))
                       and len(list(ap.iter_session_records(str(metadata_dir)))) == 1))
        get_conversation = copy.deepcopy(first)
        get_conversation["exchange_id"], get_conversation["method"] = "get-with-content", "GET"
        get_path = Path(metadata_writer.write(get_conversation))
        checks.append(("get_with_actual_reasoning_or_tools_is_not_filtered", get_path.suffix == ".json"
                       and json.loads(get_path.read_text(encoding="utf-8"))["turns"][0]["thinking"]
                       == first["chatml"]["messages"][-1]["thinking"]))

        growth_dir = root / "growth"
        growth = ap.SessionJsonWriter(str(growth_dir))
        history = [{"role": "user", "content": "LONG_PROMPT_SENTINEL" + "X" * 50000}]
        full_size = 0
        for i in range(12):
            item = exchange("g" + str(i), history, "thought " + str(i), "answer " + str(i))
            output = Path(growth.write(item))
            full_size += len(ap._canon(item).encode())
            history.extend([{"role": "assistant", "content": [
                {"type": "thinking", "thinking": "thought " + str(i)}, {"type": "text", "text": "answer " + str(i)}]},
                {"role": "user", "content": "continue"}])
        visible = output.read_text(encoding="utf-8")
        turns = json.loads(visible)["turns"]
        checks.append(("readable_size_grows_with_new_content_not_full_snapshots",
                       output.stat().st_size < full_size / 5 and visible.count("LONG_PROMPT_SENTINEL") == 1))
        checks.append(("readable_identical_new_prompts_at_new_positions_are_preserved",
                       sum(m.get("content") == "continue" for t in turns for m in t["input_messages"]) == 11))
        other = exchange("other", [{"role": "user", "content": "different flow"}], "other think")
        other_path = Path(growth.write(other))
        checks.append(("readable_different_crc_groups_have_distinct_json_names", other_path != output
                       and len(list(growth_dir.glob("session-*-rl.json"))) == 2))

        concurrent = root / "concurrent"
        worker = ("import json,sys; from omp_audit_proxy import SessionJsonWriter; "
                  "w=SessionJsonWriter(sys.argv[1]); r=json.loads(sys.argv[2]); "
                  "[(r.update(exchange_id=sys.argv[3]+'-'+str(i)), w.write(r)) for i in range(3)]")
        processes = [subprocess.Popen([sys.executable, "-B", "-X", "utf8", "-c", worker, str(concurrent),
                     json.dumps(first), str(i)], cwd=os.path.dirname(ap.__file__), stdout=subprocess.PIPE,
                     stderr=subprocess.PIPE, creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
                     for i in range(3)]
        try:
            for process in processes:
                process.communicate(timeout=30)
            files = list(concurrent.glob("session-*-rl.json"))
            concurrent_turns = json.loads(files[0].read_text(encoding="utf-8"))["turns"]
            checks.append(("readable_concurrent_append_keeps_valid_json_and_every_turn", len(files) == 1
                           and all(p.returncode == 0 for p in processes) and len(concurrent_turns) == 9
                           and len({t["exchange_id"] for t in concurrent_turns}) == 9))
        finally:
            for process in processes:
                if process.poll() is None:
                    process.kill()
                    process.communicate()
    return checks


if __name__ == "__main__":
    checks = run_checks()
    for name, ok in checks:
        print(("PASS " if ok else "FAIL ") + name)
    print(f"{sum(ok for _,ok in checks)}/{len(checks)} passed")
    raise SystemExit(not all(ok for _,ok in checks))
