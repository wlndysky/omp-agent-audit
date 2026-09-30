#!/usr/bin/env python3
"""
test_audit_proxy.py — omp_audit_proxy 的本机 mock 验证。
不访问任何真实上游：mock 上游与本代理都跑在 127.0.0.1 临时端口。

覆盖：普通 JSON、SSE、response-id 提取（anthropic/openai）、无 ID fallback hash
（稳定性+区分度）、客户端断连、凭据脱敏、ChatML 派生视图。
"""

import http.client
import json
import os
import queue
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch

import omp_audit_proxy as ap

SECRET_HEADER = "SUPERSECRET-BEARER"
SECRET_APIKEY = "SUPERSECRET-APIKEY"
SECRET_COOKIE = "SUPERSECRET-COOKIE"
SECRET_QUERY = "SUPERSECRET-QUERYKEY"

ANTHROPIC_SSE = (
    'event: message_start\n'
    'data: {"type":"message_start","message":{"id":"msg_mock_01","role":"assistant","usage":{"input_tokens":42}}}\n\n'
    'event: content_block_start\n'
    'data: {"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}\n\n'
    'event: content_block_delta\n'
    'data: {"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"让我想想"}}\n\n'
    'event: content_block_stop\n'
    'data: {"type":"content_block_stop","index":0}\n\n'
    'event: content_block_start\n'
    'data: {"type":"content_block_start","index":1,"content_block":{"type":"text","text":""}}\n\n'
    'event: content_block_delta\n'
    'data: {"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":"你好"}}\n\n'
    'event: content_block_stop\n'
    'data: {"type":"content_block_stop","index":1}\n\n'
    'event: content_block_start\n'
    'data: {"type":"content_block_start","index":2,"content_block":{"type":"tool_use","id":"toolu_mock_1","name":"bash"}}\n\n'
    'event: content_block_delta\n'
    'data: {"type":"content_block_delta","index":2,"delta":{"type":"input_json_delta","partial_json":"{\\"command\\": \\"ls\\"}"}}\n\n'
    'event: content_block_stop\n'
    'data: {"type":"content_block_stop","index":2}\n\n'
    'event: message_delta\n'
    'data: {"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":17}}\n\n'
    'event: message_stop\n'
    'data: {"type":"message_stop"}\n\n'
)

OPENAI_SSE = (
    'data: {"id":"chatcmpl-mock-7","choices":[{"delta":{"role":"assistant","reasoning_content":"想一下"},"index":0}]}\n\n'
    'data: {"id":"chatcmpl-mock-7","choices":[{"delta":{"content":"回答"},"index":0}]}\n\n'
    'data: {"id":"chatcmpl-mock-7","choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1","function":{"name":"read","arguments":"{\\"path\\":"}}]},"index":0}]}\n\n'
    'data: {"id":"chatcmpl-mock-7","choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\\"/x\\"}"}}]},"index":0}]}\n\n'
    'data: {"id":"chatcmpl-mock-7","choices":[{"delta":{},"finish_reason":"tool_calls","index":0}],"usage":{"total_tokens":9}}\n\n'
    'data: [DONE]\n\n'
)

# 无任何 id 字段的 SSE（触发 fallback hash）
NOID_SSE = (
    'data: {"choices":[{"delta":{"content":"无ID响应"},"index":0}]}\n\n'
    'data: {"choices":[{"delta":{},"finish_reason":"stop","index":0}]}\n\n'
    'data: [DONE]\n\n'
)


RESPONSES_CALL = {"type": "function_call", "id": "fc_item_1", "call_id": "call_mcp_1",
                  "name": "mcp__audit_echo", "arguments": '{"text":"pong"}'}
RESPONSES_JSON = {"id": "resp_mock_1", "status": "completed", "output": [RESPONSES_CALL],
                  "usage": {"input_tokens": 5, "output_tokens": 8}}
RESPONSES_SSE = ''.join('data: ' + json.dumps(event) + '\n\n' for event in [
    {"type": "response.created", "response": {"id": "resp_mock_1"}},
    {"type": "response.output_item.added", "item": {**RESPONSES_CALL, "arguments": ""}},
    {"type": "response.function_call_arguments.delta", "item_id": "fc_item_1", "delta": '{"text":'},
    {"type": "response.function_call_arguments.delta", "item_id": "fc_item_1", "delta": '"pong"}'},
    {"type": "response.function_call_arguments.done", "item_id": "fc_item_1",
     "arguments": RESPONSES_CALL["arguments"]},
    {"type": "response.output_item.done", "item": RESPONSES_CALL},
    {"type": "response.completed", "response": RESPONSES_JSON},
])


class MockUpstream(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    completed_client_closed = threading.Event()

    def log_message(self, *a):
        pass

    def _sse(self, payload: str, extra_headers=None):
        data = payload.encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        for k, v in (extra_headers or {}).items():
            self.send_header(k, v)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        request = json.loads(self.rfile.read(length))
        if self.path.startswith("/up-ant/"):
            self._sse(ANTHROPIC_SSE, {"request-id": "req_header_mock_1"})
        elif self.path.startswith("/up-oai/chat/completions"):
            self._sse(OPENAI_SSE)
        elif self.path.startswith("/up-noid/chat/completions"):
            self._sse(NOID_SSE, {"x-request-id": "request-only-not-a-response"})
        elif self.path.startswith("/up-responses/responses"):
            if request.get("stream"):
                self._sse(RESPONSES_SSE)
            else:
                data = json.dumps(RESPONSES_JSON).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
        elif self.path.startswith("/up-json/chat/completions"):
            body = json.dumps({
                "id": "chatcmpl-json-9",
                "choices": [{"message": {"role": "assistant", "content": "非流式回答",
                                          "reasoning_content": "非流式思考"},
                             "finish_reason": "stop"}],
                "usage": {"total_tokens": 5},
            }).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif self.path.startswith("/up-complete/v1/messages"):
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Connection", "close")
            self.end_headers()
            self.close_connection = True
            try:
                self.wfile.write(ANTHROPIC_SSE.encode())
                self.wfile.flush()
                if self.completed_client_closed.wait(5):
                    self.wfile.write(b":" + b"k" * (1024 * 1024) + b"\n\n")
                    self.wfile.flush()
            except OSError:
                pass
        elif self.path.startswith("/up-slow/v1/messages"):
            # 断连测试：先发一小段，延迟后发大块（客户端已断开）
            first = b'event: message_start\ndata: {"type":"message_start","message":{"id":"msg_slow"}}\n\n'
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.end_headers()  # 无 Content-Length -> 代理走 chunked
            try:
                self.wfile.write(first)
                self.wfile.flush()
                time.sleep(0.5)
                self.wfile.write(b"data: " + b"x" * (1024 * 1024) + b"\n\n")
                self.wfile.flush()
            except OSError:
                pass
        else:
            self.send_response(404)
            self.send_header("Content-Length", "2")
            self.end_headers()
            self.wfile.write(b"{}")


def post(port, path, body_obj, headers=None, read_all=True):
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=15)
    body = json.dumps(body_obj).encode()
    h = {"Content-Type": "application/json", "Authorization": f"Bearer {SECRET_HEADER}",
         "X-API-Key": SECRET_APIKEY, "Cookie": SECRET_COOKIE}
    h.update(headers or {})
    conn.request("POST", path, body=body, headers=h)
    resp = conn.getresponse()
    data = resp.read() if read_all else resp.read1(64)
    status = resp.status
    if not read_all:
        conn.close()  # 模拟客户端中途断开
    else:
        conn.close()
    return status, data


def tool_regressions():
    checks = []
    checks.append(("public_distribution_has_no_private_upstreams", ap.DEFAULT_ROUTES == []))
    unconfigured = subprocess.run([
        sys.executable, "-B", "-X", "utf8", os.path.abspath(ap.__file__), "--proxy-only",
    ], capture_output=True, text=True, encoding="utf-8", timeout=10,
        creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
    checks.append(("proxy_only_requires_explicit_route", unconfigured.returncode == 2
                   and "--route" in unconfigured.stderr and "listening" not in unconfigured.stderr))
    ant = {"messages": [
        {"role": "assistant", "content": [
            {"type": "tool_use", "id": "builtin", "name": "read", "input": {"path": "demo.txt"}},
            {"type": "tool_use", "id": "mcp", "name": "mcp__audit_echo", "input": {"text": "hello"}}]},
        {"role": "user", "content": [
            {"type": "tool_result", "tool_use_id": "mcp", "is_error": True,
             "content": [{"type": "text", "text": "permission denied"}]},
            {"type": "tool_result", "tool_use_id": "builtin", "content": "local file"}]}]}
    tracker = ap.ToolTraceTracker()
    events = tracker.observe_request("cold", "anthropic-messages", ant, "route")
    results = [e for e in events if e["event"] == "tool_result"]
    checks.append(("anthropic_cold_history_names_args_results", results[0]["tool_name"] == "mcp__audit_echo"
                   and results[0]["arguments"] == {"text": "hello"} and results[1]["tool_name"] == "read"
                   and results[1]["content"] == "local file"))
    checks.append(("anthropic_error_flag_preserved", results[0]["is_error"] is True and results[1]["is_error"] is False))
    checks.append(("context_does_not_invent_original_exchange", all(e["call_exchange_id"] is None and e["correlated"] for e in results)))
    again = tracker.observe_request("repeat", "anthropic-messages", ant, "route")
    checks.append(("repeated_anthropic_history_stays_correlated", all(e["replayed_context"] and e["correlated"]
                   for e in again if e["event"] == "tool_result")))

    tracker = ap.ToolTraceTracker()
    a = {"id": "same", "name": "read", "arguments": {"path": "a"}}
    b = {"id": "same", "name": "read", "arguments": {"path": "b"}}
    tracker.observe_calls("A", [a], "route", "resp_A")
    tracker.observe_calls("B", [b], "route", "resp_B")
    result = {"tool_call_id": "same", "content": "out", "position": 1}
    linked = tracker.observe_results("C", [result], "route", [{**b, "position": 0}])[0]
    checks.append(("reused_id_matches_arguments_not_fifo", linked["call_exchange_id"] == "B"))
    ambiguous = tracker.observe_results("D", [result], "route")[0]
    checks.append(("ambiguous_id_does_not_guess", ambiguous["call_exchange_id"] is None
                   and ambiguous["call_exchange_ambiguous"] and not ambiguous["correlated"]))
    previous = tracker.observe_results("E", [result], "route", previous_response_id="resp_A")[0]
    checks.append(("previous_response_resolves_ambiguous_ids", previous["call_exchange_id"] == "A"))
    unmatched = tracker.observe_results("F", [result], "different_route")[0]
    checks.append(("route_isolation", not unmatched["correlated"]))
    tracker.observe_calls("B2", [b], "route", "resp_B2")
    same_signature = tracker.observe_results("G", [result], "route", [{**b, "position": 0}])[0]
    checks.append(("identical_concurrent_calls_keep_origin_ambiguous", same_signature["call_exchange_id"] is None
                   and same_signature["correlated"] and same_signature["arguments"] == b["arguments"]))

    repeated = ap.ToolTraceTracker()
    repeated.observe_calls("repeat-call-1", [a], "repeat-scope", "repeat-response-1")
    same_result = {"tool_call_id": "same", "content": "identical", "source": "request.messages[2]"}
    repeated.observe_results("repeat-result-1", [same_result], "repeat-scope")
    repeated.observe_calls("repeat-call-2", [a], "repeat-scope", "repeat-response-2")
    second_result = repeated.observe_results("repeat-result-2", [same_result], "repeat-scope")[0]
    checks.append(("repeated_identical_call_result_is_not_asserted_replay",
                   second_result["replayed_context"] is not True and second_result["result_origin_ambiguous"]))
    duplicate_calls = ap.ToolTraceTracker()
    duplicate_calls.observe_calls("same-response", [a, a], "repeat-scope")
    checks.append(("duplicate_ids_within_response_remain_distinct_cached_calls",
                   len(duplicate_calls._calls) == 2))

    cold_tracker = ap.ToolTraceTracker()
    future = cold_tracker.observe_results("H", [result], "route", [{**b, "position": 2}])[0]
    checks.append(("future_context_call_does_not_match_past_result", not future["correlated"]))
    checks.append(("unreported_error_status_is_unknown", future["is_error"] is None))
    small = ap.ToolTraceTracker(max_entries=2)
    for i in range(8):
        small.observe_calls(str(i), [{**a, "id": str(i)}], "route")
        small.observe_results(str(i), [{"tool_call_id": str(i), "content": str(i)}], "route")
    checks.append(("correlation_memory_is_bounded", len(small._calls) == 2 and len(small._seen_results) == 2))

    for protocol, payload in [("anthropic-messages", ANTHROPIC_SSE), ("openai-responses", RESPONSES_SSE)]:
        tap = ap.SseTap(protocol)
        # Byte-sized feed covers split JSON strings, UTF-8 and SSE boundaries.
        for byte in payload.encode():
            tap.feed(bytes([byte]))
        tap.flush()
        msg = tap.chatml_assistant()
        checks.append((protocol + "_assembly_is_idempotent", msg == tap.chatml_assistant()
                       and len(msg.get("tool_calls", [])) == 1))
    tap = ap.SseTap("anthropic-messages")
    tap._extract("content_block_start", {"content_block": {
        "type": "tool_use", "id": "init", "name": "read", "input": {"path": "demo"}}})
    checks.append(("anthropic_initial_arguments_without_deltas", tap.chatml_assistant()["tool_calls"][0]["arguments"] == {"path": "demo"}))
    tap = ap.SseTap("openai-responses")
    tap._extract(None, {"type": "response.completed", "response": RESPONSES_JSON})
    checks.append(("responses_completed_only_recovers_call", tap.chatml_assistant()["tool_calls"][0]["id"] == "call_mcp_1"))
    rsp_history = {"instructions": "sys", "input": [RESPONSES_CALL,
        {"type": "function_call_output", "call_id": "call_mcp_1", "output": "pong"}]}
    rsp_event = next(e for e in cold_tracker.observe_request("I", "openai-responses", rsp_history, "route")
                     if e["event"] == "tool_result")
    checks.append(("responses_context_cold_start", rsp_event["tool_name"] == "mcp__audit_echo"
                   and rsp_event["arguments"] == {"text": "pong"} and rsp_event["content"] == "pong"))
    chatml = ap.derive_chatml_request("openai-responses", rsp_history)
    checks.append(("responses_context_chatml", [m["role"] for m in chatml] == ["system", "assistant", "tool"]
                   and chatml[-1]["tool_call_id"] == "call_mcp_1"))
    h1 = ap.fallback_stream_id("openai-responses", {"instructions": "sys", "input": "one"})
    h2 = ap.fallback_stream_id("openai-responses", {"instructions": "sys", "input": "two"})
    checks.append(("responses_string_input_hash_distinguishes_users", bool(h1) and h1 != h2))
    checks.append(("unknown_tool_name_not_mislabeled_builtin", ap.tool_kind_hint("custom_echo") == "custom_or_unknown"))
    for protocol, payload in (("anthropic-messages", ANTHROPIC_SSE),
                              ("openai-completions", OPENAI_SSE), ("openai-responses", RESPONSES_SSE)):
        complete = ap.SseTap(protocol)
        complete.feed(payload.encode())
        complete.flush()
        checks.append((protocol + "_terminal_event_marks_capture_complete", complete.complete))
    unfinished = ap.SseTap("anthropic-messages")
    unfinished.feed(b'event: message_delta\ndata: {"type":"message_delta","delta":{"stop_reason":"end_turn"}}\n\n')
    checks.append(("stop_reason_without_terminal_event_is_not_complete", not unfinished.complete))
    return checks


def cli_smoke(mock_port, body):
    """Run the delivered Python CLI itself against the loopback fixture."""
    with tempfile.TemporaryDirectory(prefix="omp-audit-cli-") as directory:
        path = os.path.join(directory, "audit.jsonl")
        process = subprocess.Popen([
            sys.executable, "-B", "-u", "-X", "utf8", os.path.abspath(ap.__file__),
            "--port", "0", "--log", path,
            "--route", f"/mock/=http://127.0.0.1:{mock_port}/up-noid=openai-completions",
        ], stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True, encoding="utf-8",
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
        startup = queue.Queue()
        threading.Thread(target=lambda: startup.put(process.stderr.readline()), daemon=True).start()
        try:
            first = startup.get(timeout=10)
            port = int(first.strip().rsplit(":", 1)[1])
            connection = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
            connection.request("GET", ap.HEALTH_PATH)
            response = connection.getresponse()
            healthy = response.status == 200 and json.loads(response.read()) == {"ok": True}
            connection.close()
            status, data = post(port, "/mock/chat/completions", body)
            deadline = time.monotonic() + 5
            documents = []
            while time.monotonic() < deadline:
                try:
                    documents = [json.load(open(os.path.join(directory, name), encoding="utf-8"))
                                 for name in os.listdir(directory) if name.startswith("session-") and name.endswith("-rl.json")]
                except (ValueError, OSError):
                    documents = []
                if any(t.get("tool_results") for d in documents for t in d["turns"]):
                    break
                time.sleep(0.02)
            payloads = [e for d in documents for t in d["turns"] for e in t["tool_results"]]
            return [
                ("cli_starts_and_healthcheck_passes", healthy),
                ("cli_custom_route_forwards_unchanged", status == 200 and data == NOID_SSE.encode()),
                ("cli_writes_mcp_tool_trace", any(e.get("tool_name") == "mcp__audit_echo"
                  and e.get("content") == "pong" and e.get("arguments") == {"text": "pong"} for e in payloads)),
                ("cli_default_output_is_readable_json_without_jsonl_indexes",
                 bool(documents) and not os.path.exists(path) and not os.path.exists(os.path.join(directory, "tools.jsonl"))
                 and all("thinking" in t and "tool_calls" in t for d in documents for t in d["turns"])),
            ]
        finally:
            process.terminate()
            process.wait(timeout=10)
            process.stderr.close()


def launcher_regressions():
    handler = type("LauncherTestHandler", (ap.AuditHandler,), {
        "routes": [], "control_token": "synthetic-control-token", "logger": None, "tool_logger": None,
    })
    server = ap.AuditHTTPServer(("127.0.0.1", 0), handler)
    observations = {}
    original_environment = dict(os.environ)
    checks = []

    def register(body, auth="Bearer synthetic-control-token"):
        connection = http.client.HTTPConnection("127.0.0.1", server.server_port, timeout=5)
        connection.request("POST", ap.CONTROL_PATH, body=json.dumps(body),
                           headers={"Content-Type": "application/json", "Authorization": auth})
        response = connection.getresponse()
        status, output = response.status, json.loads(response.read())
        connection.close()
        return status, output

    class Child:
        def __init__(self, args, env):
            observations.update(args=args, env=env, extension=args[2])
            with open(args[2], encoding="utf-8") as fh:
                observations["source"] = fh.read()

        def wait(self, **kwargs):
            checks.append(("launcher_control_rejects_other_clients", register({"routes": []}, "Bearer wrong")[0] == 403))
            route = {"provider": "fixture", "api": "openai-completions", "upstream": "https://upstream.example/v1"}
            checks.append(("launcher_control_rejects_url_credentials", register({"routes": [
                {**route, "upstream": "https://name:secret@upstream.example/v1"}]})[0] == 400))
            checks.append(("launcher_control_rejects_self_loop", register({"routes": [
                {**route, "upstream": f"http://127.0.0.1:{server.server_port}"}]})[0] == 400))
            checks.append(("launcher_control_rejects_stale_audit_endpoints", register({"routes": [
                {**route, "upstream": "http://127.0.0.1:9/_audit/old"}]})[0] == 400))
            status, response = register({"routes": [route], "ready": True})
            again_status, again = register({"routes": [route]})
            checks.append(("launcher_routes_registered_idempotently", status == again_status == 200
                           and response == again and len(handler.routes) == 1))
            checks.append(("launcher_preserves_openai_v1_base", handler.routes[0].base_path == "/v1"))
            anthropic = {"provider": "messages-fixture", "api": "anthropic-messages",
                         "upstream": "https://upstream.example/gateway/v1"}
            ant_status, ant_response = register({"routes": [anthropic]})
            ant_route = next(r for r in handler.routes if r.hint == "anthropic-messages")
            checks.append(("launcher_normalizes_anthropic_v1_base", ant_status == 200
                           and ant_route.base_path == "/gateway"))
            variants = ["/gateway", "/gateway/", "/gateway/v1///"]
            variant_results = [register({"routes": [{**anthropic,
                "upstream": "https://upstream.example" + suffix}]}) for suffix in variants]
            checks.append(("launcher_equivalent_anthropic_bases_share_route",
                           all(code == 200 and value == ant_response for code, value in variant_results)))
            checks.append(("explicit_routes_keep_literal_v1_base",
                           ap.Route("/manual/", anthropic["upstream"], anthropic["api"]).base_path == "/gateway/v1"))
            return 0

        def poll(self):
            return 0

    try:
        with patch.object(ap.subprocess, "Popen", Child):
            status = ap.launch_omp(server, handler, "omp", ["--no-session", "--thinking", "high"])
        checks.extend([
            ("launcher_preserves_omp_arguments", observations["args"][3:] == ["--no-session", "--thinking", "high"]),
            ("launcher_removes_temporary_extension", not os.path.exists(observations["extension"])),
            ("launcher_environment_is_child_only", dict(os.environ) == original_environment
             and observations["env"]["OMP_AUDIT_CONTROL_TOKEN"] == "synthetic-control-token"),
            ("launcher_returns_child_exit_code", status == 0),
            ("launcher_extension_contains_no_instance_secret", "synthetic-control-token" not in observations["source"]),
        ])
    finally:
        server.server_close()
    return checks


def shim_route_regressions():
    seen = []
    class EndpointProbe(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass
        def do_POST(self):
            self.rfile.read(int(self.headers.get("Content-Length", 0)))
            seen.append(self.path)
            valid = self.path in ("/gateway/v1/messages?beta=true", "/gateway/v1/chat/completions")
            body = b'{"id":"response-fixture","choices":[]}' if valid else b'{"error":"wrong_path"}'
            self.send_response(200 if valid else 404)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
    upstream = ap.AuditHTTPServer(("127.0.0.1", 0), EndpointProbe)
    base = f"http://127.0.0.1:{upstream.server_port}/gateway/v1"
    handler = type("ShimTestHandler", (ap.AuditHandler,), {
        "routes": [ap.Route("/manual/", base, "openai-completions")],
        "control_token": "synthetic-control-token", "logger": None, "tool_logger": None, "session_writer": None,
    })
    proxy = ap.AuditHTTPServer(("127.0.0.1", 0), handler)
    for server in (upstream, proxy):
        threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        status, registered = post(proxy.server_port, ap.CONTROL_PATH, {"routes": [{
            "provider": "shim-fixture", "api": "openai-completions", "upstream": base}]},
            headers={"Authorization": "Bearer synthetic-control-token"})
        if status != 200:
            raise RuntimeError("Unable to register fixture route")
        route_path = json.loads(registered)["routes"][0]["baseUrl"].split(str(proxy.server_port), 1)[1]
        body = {"messages": [{"role": "user", "content": "fixture"}]}
        anthropic_status, _ = post(proxy.server_port, route_path + "/v1/messages?beta=true", body)
        openai_status, _ = post(proxy.server_port, route_path + "/chat/completions", body)
        manual_status, _ = post(proxy.server_port, "/manual/v1/messages?beta=true", body)
        exact_status, exact_registered = post(proxy.server_port, ap.CONTROL_PATH, {"routes": [{
            "provider": "fetch-fixture", "api": "openai-completions", "upstream": base, "exact_path": True}]},
            headers={"Authorization": "Bearer synthetic-control-token"})
        exact_path = json.loads(exact_registered)["routes"][0]["baseUrl"].split(str(proxy.server_port), 1)[1]
        fetch_status, _ = post(proxy.server_port, exact_path + "//messages?beta=true", body)
        return [
            ("shim_openai_catalog_anthropic_wire_avoids_duplicate_v1",
             anthropic_status == 200 and seen[0] == "/gateway/v1/messages?beta=true"),
            ("shim_same_route_retains_openai_v1_endpoint",
             openai_status == 200 and seen[1] == "/gateway/v1/chat/completions"),
            ("shim_does_not_rewrite_explicit_manual_routes",
             manual_status == 404 and seen[2] == "/gateway/v1/v1/messages?beta=true"),
            ("fetch_routes_preserve_shim_wire_path_and_query_exactly", exact_status == fetch_status == 200
             and seen[3] == "/gateway/v1/messages?beta=true"),
        ]
    finally:
        for server in (proxy, upstream):
            server.shutdown()
            server.server_close()


def launcher_fetch_regressions():
    """Exercise the actual launcher JS without a network or model call."""
    node = shutil.which("node")
    if not node:
        return []
    script = r'''
import assert from "node:assert/strict";
const checks = [];
const captured = [];
const catalog = [
  {provider: "chat", id: "fixture", api: "openai-completions", baseUrl: "https://chat.example/v1"},
  {provider: "messages", id: "fixture", api: "anthropic-messages", baseUrl: "https://chat.example/gateway/v1"},
  {provider: "nested", id: "fixture", api: "openai-completions", baseUrl: "https://chat.example/v1/nested"},
];
const initial = JSON.stringify(catalog);
process.env.OMP_AUDIT_CONTROL_URL = "http://127.0.0.1:8123";
process.env.OMP_AUDIT_CONTROL_TOKEN = "test-only";
const nativeFetch = async (input, init) => {
  const url = input instanceof Request ? input.url : String(input);
  if (url.endsWith("/__audit/routes")) {
    const body = JSON.parse(init.body);
    assert(body.routes.every(r => r.exact_path === true));
    return Response.json({routes: body.routes.map(r => ({provider: r.provider,
      baseUrl: "http://127.0.0.1:8123/_audit/" + r.provider}))});
  }
  captured.push({input, init, url});
  return new Response("ok");
};
nativeFetch.preconnect = () => "preserved";
globalThis.fetch = nativeFetch;
const extension = await import("data:text/javascript;base64," + SOURCE);
let start;
extension.default({on: (name, handler) => { if (name === "session_start") start = handler; },
  registerProvider: () => { throw new Error("registry must not be rewritten"); },
  setModel: () => { throw new Error("model must not be rewritten"); }});
const ctx = {model: catalog[0], modelRegistry: {getAll: () => catalog}};
await start({}, ctx);
assert.equal(JSON.stringify(catalog), initial);
checks.push(["launcher_fetch_preserves_catalog_and_discovery_endpoints", true]);
const controller = new AbortController();
const options = {method: "POST", body: "exact body", headers: {Authorization: "test-only"}, signal: controller.signal};
await fetch("https://chat.example/v1/chat/completions?key=a%2Bb", options);
assert.equal(captured.at(-1).url, "http://127.0.0.1:8123/_audit/chat//chat/completions?key=a%2Bb");
assert.equal(captured.at(-1).init, options);
checks.push(["launcher_fetch_keeps_auth_body_options_signal_and_query", true]);
const request = new Request("https://chat.example/gateway/v1/messages?beta=true", options);
await fetch(request);
const forwarded = captured.at(-1).input;
assert.equal(forwarded.url, "http://127.0.0.1:8123/_audit/messages//messages?beta=true");
assert.equal(forwarded.headers.get("authorization"), "test-only");
assert.equal(await forwarded.text(), "exact body");
controller.abort();
assert.equal(forwarded.signal.aborted, true);
checks.push(["launcher_fetch_keeps_request_objects_and_cancellation", true]);
await fetch(new URL("https://chat.example/v1/nested/chat/completions"));
assert.equal(captured.at(-1).url, "http://127.0.0.1:8123/_audit/nested//chat/completions");
checks.push(["launcher_fetch_uses_longest_endpoint_match", true]);
for (const url of ["https://chat.example/v10/chat/completions", "https://chat.example.evil/v1/chat/completions",
    "https://unrelated.example/v1/messages"]) {
  await fetch(url);
  assert.equal(captured.at(-1).url, url);
}
assert.equal(fetch.preconnect(), "preserved");
checks.push(["launcher_fetch_respects_origin_boundaries_and_native_properties", true]);
await start({}, ctx);
await fetch("https://chat.example/v1/models");
assert.equal(captured.at(-1).url, "http://127.0.0.1:8123/_audit/chat//models");
assert.equal(JSON.stringify(catalog), initial);
checks.push(["launcher_fetch_repeated_session_start_does_not_nest_routing", true]);
process.stdout.write(JSON.stringify(checks));
'''
    source = ap.base64.b64encode(ap.LAUNCHER_EXTENSION.encode()).decode()
    run = subprocess.run([node, "--input-type=module", "-e", script.replace("SOURCE", json.dumps(source))],
                         capture_output=True, text=True, encoding="utf-8", timeout=20)
    if run.returncode:
        raise RuntimeError("Launcher fetch regression failed: " + run.stderr[-2500:])
    return [tuple(check) for check in json.loads(run.stdout)]


def session_archive_regressions(directory_parent=None):
    from test_incremental import run_checks
    from test_readable_json import run_checks as readable_checks
    return run_checks(directory_parent) + readable_checks(directory_parent)


def windows_launcher_regressions():
    if os.name != "nt":
        return []
    launcher = os.path.join(os.path.dirname(os.path.abspath(ap.__file__)), "start-audit.cmd")
    with tempfile.TemporaryDirectory(prefix="omp-audit-cmd-") as directory:
        project = os.path.join(directory, "project with spaces")
        os.makedirs(project)
        probe = os.path.join(directory, "probe.py")
        with open(probe, "w", encoding="utf-8") as fh:
            fh.write("import json, os, sys\nprint(json.dumps({'args': sys.argv[1:], 'cwd': os.getcwd()}))\n"
                     "raise SystemExit(7)\n")
        with open(os.path.join(project, "python.cmd"), "w", encoding="utf-8") as fh:
            fh.write('@echo off\n"' + sys.executable + '" "' + probe + '" %*\n')
        environment = os.environ.copy()
        environment.pop("OMP_AUDIT_LOG_DIR", None)
        command = '""' + launcher + '" -- --model "fixture/model id""'
        def run():
            shell_command = '"' + os.environ.get("COMSPEC", "cmd.exe") + '" /d /s /c ' + command
            return subprocess.run(shell_command,
                cwd=project, env=environment, capture_output=True, text=True, encoding="utf-8",
                errors="replace", timeout=10, creationflags=subprocess.CREATE_NO_WINDOW)
        result = run()
        if not result.stdout.strip():
            raise RuntimeError("Windows launcher probe failed: " + result.stderr)
        observed = json.loads(result.stdout)
        args = observed["args"]
        expected_log = os.path.join(os.path.dirname(launcher), "..")
        checks = [
            ("windows_launcher_preserves_project_directory", observed["cwd"] == project),
            ("windows_launcher_uses_parent_log_directory",
             os.path.normpath(args[args.index("--sessions-dir") + 1]) == os.path.normpath(expected_log)),
            ("windows_launcher_forwards_arguments_and_exit_code",
             args[-3:] == ["--", "--model", "fixture/model id"] and result.returncode == 7),
        ]
        override = os.path.join(directory, "custom logs")
        environment["OMP_AUDIT_LOG_DIR"] = override
        override_args = json.loads(run().stdout)["args"]
        checks.append(("windows_launcher_accepts_log_directory_override",
                       os.path.normpath(override_args[override_args.index("--sessions-dir") + 1]) ==
                       os.path.normpath(override)))
        return checks


def real_omp_smoke(api="openai-completions", *, discovered=False):
    """Optional real OMP + fake provider, using an isolated temporary agent dir."""
    if not shutil.which("omp"):
        raise RuntimeError("--real-omp requires an installed omp executable")
    with tempfile.TemporaryDirectory(prefix="omp-audit-real-") as directory:
        agent_dir = os.path.join(directory, "agent")
        workspace = os.path.join(directory, "workspace")
        os.makedirs(agent_dir)
        os.makedirs(workspace)
        fixture = os.path.join(workspace, "fixture.txt")
        with open(fixture, "w", encoding="utf-8") as fh:
            fh.write("AUDIT_READ_OK\n")
        requests = []
        request_paths = []
        fixture_thinking = "AUDIT_THINK_FULL\n完整的模拟思考文本"
        provider_id = "kimi-code" if discovered else "audit-fixture"
        model_id = "k3" if discovered else "fixture"
        discovery_requests = []
        is_anthropic = api == "anthropic-messages"
        base_path = "/gateway/v1" if is_anthropic else "/v1"
        expected_path = base_path + ("/messages" if is_anthropic else "/chat/completions")

        class Provider(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args):
                pass

            def do_GET(self):
                discovery_requests.append(self.path)
                data = json.dumps({"data": [{"id": model_id, "protocol": None, "context_length": 32768},
                                             {"id": "k3-256k", "protocol": None}]}).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                requests.append(body)
                request_paths.append(self.path)
                if self.path.split("?", 1)[0] != expected_path:
                    data = b'{"error":{"type":"resource_not_found_error","message":"Unexpected endpoint"}}'
                    self.send_response(404)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(data)))
                    self.end_headers()
                    self.wfile.write(data)
                    return
                messages = body.get("messages", [])
                completed = any(m.get("role") == "tool" for m in messages)
                if is_anthropic:
                    completed = any(
                        isinstance(m.get("content"), list) and any(
                            isinstance(block, dict) and block.get("type") == "tool_result"
                            for block in m["content"]) for m in messages)
                delta = {"role": "assistant", "reasoning_content": fixture_thinking}
                if completed:
                    delta["content"] = "AUDIT_LOCAL_OK"
                else:
                    delta["tool_calls"] = [{"index": 0, "id": "call_fixture", "type": "function",
                        "function": {"name": "read", "arguments": json.dumps({"path": fixture})}}]
                chunks = [
                    {"id": "chatcmpl-fixture-" + str(len(requests)), "object": "chat.completion.chunk",
                     "choices": [{"index": 0, "delta": delta, "finish_reason": None}]},
                    {"id": "chatcmpl-fixture-" + str(len(requests)), "object": "chat.completion.chunk",
                     "choices": [{"index": 0, "delta": {}, "finish_reason": "stop" if completed else "tool_calls"}],
                     "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}},
                ]
                data = ("".join("data: " + json.dumps(c) + "\n\n" for c in chunks) + "data: [DONE]\n\n").encode()
                if is_anthropic:
                    content = ({"type": "text", "text": ""} if completed else
                               {"type": "tool_use", "id": "call_fixture", "name": "read", "input": {}})
                    content_delta = ({"type": "text_delta", "text": "AUDIT_LOCAL_OK"} if completed else
                                     {"type": "input_json_delta", "partial_json": json.dumps({"path": fixture})})
                    chunks = [
                        {"type": "message_start", "message": {"id": "msg-fixture-" + str(len(requests)),
                         "type": "message", "role": "assistant", "model": "fixture", "content": [],
                         "stop_reason": None, "stop_sequence": None,
                         "usage": {"input_tokens": 10, "output_tokens": 0}}},
                        {"type": "content_block_start", "index": 0,
                         "content_block": {"type": "thinking", "thinking": ""}},
                        {"type": "content_block_delta", "index": 0,
                         "delta": {"type": "thinking_delta", "thinking": fixture_thinking}},
                        {"type": "content_block_stop", "index": 0},
                        {"type": "content_block_start", "index": 1, "content_block": content},
                        {"type": "content_block_delta", "index": 1, "delta": content_delta},
                        {"type": "content_block_stop", "index": 1},
                        {"type": "message_delta", "delta": {"stop_reason": "end_turn" if completed else "tool_use",
                         "stop_sequence": None}, "usage": {"output_tokens": 5}},
                        {"type": "message_stop"},
                    ]
                    data = "".join("event: " + c["type"] + "\ndata: " + json.dumps(c) + "\n\n"
                                   for c in chunks).encode()
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

        mock = ap.AuditHTTPServer(("127.0.0.1", 0), Provider)
        threading.Thread(target=mock.serve_forever, daemon=True).start()
        models_path = os.path.join(agent_dir, "models.yml")
        configuration = {"providers": {provider_id: {
            "baseUrl": f"http://127.0.0.1:{mock.server_port}" + base_path, "api": api, "auth": "none",
            "models": [{"id": model_id, "name": "Audit Fixture", "reasoning": False,
                        "input": ["text"], "contextWindow": 32768, "maxTokens": 1024}],
        }}}
        if discovered:
            configuration["providers"][provider_id].pop("auth")
            configuration["providers"][provider_id]["apiKey"] = "synthetic-local-only"
            configuration["providers"][provider_id]["models"][0]["compat"] = {"kimiApiFormat": "openai"}
        original = json.dumps(configuration).encode()
        with open(models_path, "wb") as fh:
            fh.write(original)
        environment = os.environ.copy()
        environment.pop("OMP_PROFILE", None)
        environment["PI_CODING_AGENT_DIR"] = agent_dir
        logpath = os.path.join(directory, "logs", "audit.jsonl")
        try:
            command = [
                sys.executable, "-B", "-u", "-X", "utf8", os.path.abspath(ap.__file__), "--log", logpath, "--",
                "--cwd", workspace, "--no-extensions", "--no-skills", "--no-rules", "--no-lsp",
                "--no-session", "--no-title", "--tools", "read", "--model", provider_id + "/" + model_id,
                "--thinking", "off", "-p", "Read fixture.txt with the read tool, then report its contents.",
            ]
            probe_path = os.path.join(directory, "catalog-probe.json")
            if discovered:
                extension = os.path.join(directory, "probe.mjs")
                with open(extension, "w", encoding="utf-8") as fh:
                    fh.write('import {writeFileSync} from "node:fs";\n'
                             'export default function(pi) { pi.on("session_start", async (_e, ctx) => {\n'
                             'const before = ctx.modelRegistry.find("kimi-code", "k3").baseUrl;\n'
                             'await ctx.modelRegistry.refreshProvider("kimi-code", "online");\n'
                             'const after = ctx.modelRegistry.find("kimi-code", "k3").baseUrl;\n'
                             'writeFileSync(' + json.dumps(probe_path) + ', JSON.stringify({before, after}));\n'
                             '}); }\n')
                command.extend(["--extension", extension])
            run = subprocess.run(command, env=environment, cwd=workspace, capture_output=True, text=True, encoding="utf-8", errors="replace",
                timeout=55, creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
            if run.returncode != 0:
                raise RuntimeError("Real OMP smoke failed: " + run.stderr[-6500:] + run.stdout[-2500:])
            with open(models_path, "rb") as fh:
                unchanged = fh.read() == original
            session_archives = [r for r in ap.iter_session_records(os.path.join(directory, "logs"))
                                if r.get("method") == "POST"]
            tool_results = [e for archive in session_archives for e in archive.get("tool_trace", [])
                            if e.get("event") == "tool_result"]
            documents = []
            for name in os.listdir(os.path.join(directory, "logs")):
                if name.startswith("session-") and name.endswith("-rl.json"):
                    with open(os.path.join(directory, "logs", name), encoding="utf-8") as fh:
                        documents.append(json.load(fh))
            readable_turns = [turn for document in documents for turn in document["turns"]]
            checks = [
                ("real_omp_launcher_exit_success", run.returncode == 0 and "AUDIT_LOCAL_OK" in run.stdout),
                ("real_omp_configuration_unchanged", unchanged),
                ("real_omp_read_tool_audited", any(e.get("tool_name") == "read"
                  and "AUDIT_READ_OK" in str(e.get("content")) and e.get("correlated") for e in tool_results)),
                ("real_omp_two_model_rounds_forwarded", len(requests) >= 2),
                ("real_omp_readable_json_keeps_full_thinking", len(readable_turns) == 2
                 and all(turn["thinking"] == fixture_thinking for turn in readable_turns)),
                ("real_omp_readable_json_keeps_tool_arguments_and_result",
                 any(call.get("tool_name") == "read" and call.get("arguments") == {"path": fixture}
                     for turn in readable_turns for call in turn["tool_calls"])
                 and any(result.get("tool_name") == "read" and "AUDIT_READ_OK" in str(result.get("content"))
                         for turn in readable_turns for result in turn["tool_results"])),
                ("real_omp_exports_incremental_journal_with_tool_result", len(session_archives) == 2
                 and any(event.get("event") == "tool_result" and "AUDIT_READ_OK" in str(event.get("content"))
                         for archive in session_archives for event in archive["tool_trace"])),
                ("real_omp_exact_upstream_paths", len(request_paths) == 2
                 and all(path.split("?", 1)[0] == expected_path for path in request_paths)),
            ]
            if discovered:
                upstream = configuration["providers"][provider_id]["baseUrl"]
                with open(probe_path, encoding="utf-8") as fh:
                    probe = json.load(fh)
                def cache_models():
                    db = sqlite3.connect(os.path.join(agent_dir, "models.db"))
                    try:
                        return [model for (raw,) in db.execute("SELECT models FROM model_cache WHERE provider_id = ?",
                                                              (provider_id,)) for model in json.loads(raw)]
                    finally:
                        db.close()
                first_cache = cache_models()
                restarted = subprocess.run(command, env=environment, cwd=workspace, capture_output=True, text=True,
                    encoding="utf-8", errors="replace", timeout=55,
                    creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
                second_cache = cache_models()
                def clean_discovery_cache(models):
                    # Kimi also caches bundled fallback rows at their canonical
                    # endpoint; only dynamically discovered IDs use the fixture.
                    return (all('/_audit/' not in m.get('baseUrl', '') for m in models)
                            and all(any(m['id'] == model and m['baseUrl'] == upstream for m in models)
                                    for model in (model_id, 'k3-256k')))
                all_archives = list(ap.iter_session_records(os.path.join(directory, "logs")))
                checks.extend([
                    ("real_omp_catalog_refresh_keeps_original_endpoints", probe == {"before": upstream, "after": upstream}),
                    ("real_omp_discovered_cache_contains_original_endpoints", clean_discovery_cache(first_cache)),
                    ("real_omp_second_launch_succeeds_without_stale_routes", restarted.returncode == 0
                     and "AUDIT_LOCAL_OK" in restarted.stdout and len(requests) == 4),
                    ("real_omp_second_refresh_does_not_pollute_cache", clean_discovery_cache(second_cache)),
                    ("real_omp_discovery_and_restart_requests_are_audited", len(discovery_requests) >= 2
                     and sum(r.get("method") == "POST" for r in all_archives) == 4),
                ])
            else:
                before_failed_run = len(requests)
                configuration["providers"][provider_id]["models"].append({
                    "id": "other", "baseUrl": f"http://127.0.0.1:{mock.server_port}/different/v1"})
                with open(models_path, "w", encoding="utf-8") as fh:
                    json.dump(configuration, fh)
                mixed = subprocess.run(command, env=environment, cwd=workspace, capture_output=True, text=True,
                    encoding="utf-8", errors="replace", timeout=55,
                    creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
                all_archives = list(ap.iter_session_records(os.path.join(directory, "logs")))
                after_mixed_run = len(requests)
                configuration["providers"][provider_id]["api"] = "google-generative-ai"
                configuration["providers"][provider_id]["models"] = [configuration["providers"][provider_id]["models"][0]]
                with open(models_path, "w", encoding="utf-8") as fh:
                    json.dump(configuration, fh)
                failed = subprocess.run(command, env=environment, cwd=workspace, capture_output=True, text=True,
                    encoding="utf-8", errors="replace", timeout=55,
                    creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
                checks.extend([
                    ("real_omp_mixed_provider_routes_selected_model", mixed.returncode == 0
                     and "AUDIT_LOCAL_OK" in mixed.stdout and after_mixed_run == before_failed_run + 2),
                    ("real_omp_mixed_provider_uses_exact_upstream_paths", len(request_paths) == 4
                     and all(path.split("?", 1)[0] == expected_path for path in request_paths)),
                    ("real_omp_unsafe_routing_stops_before_inference", failed.returncode != 0
                     and len(requests) == after_mixed_run and "Unsupported or missing endpoint" in failed.stderr),
                ])
            audited_model_requests = [record for record in all_archives if record.get("method") == "POST"]
            checks.extend([
                ("real_omp_independent_identical_prompts_have_distinct_sessions", len(audited_model_requests) == 4
                 and len({record.get("audit_session_id") for record in audited_model_requests}) == 2
                 and all(record.get("audit_session_id") and record.get("audit_agent_id")
                         for record in audited_model_requests)
                 and len({record.get("stream_id") for record in audited_model_requests}) == 2),
                ("real_omp_private_identity_never_reaches_upstream", all("_omp_audit_v1" not in body for body in requests)),
            ])
            if discovered:
                return [(name.replace("real_omp_", "real_omp_kimi_"), ok) for name, ok in checks]
            return [(name.replace("real_omp_", "real_omp_anthropic_") if is_anthropic else name, ok)
                    for name, ok in checks]
        finally:
            mock.shutdown()
            mock.server_close()


def main():
    mock = ap.AuditHTTPServer(("127.0.0.1", 0), MockUpstream)
    mock.daemon_threads = True
    mock_port = mock.server_address[1]
    threading.Thread(target=mock.serve_forever, daemon=True).start()

    logdir = tempfile.mkdtemp(prefix="omp-audit-test-")
    logpath = os.path.join(logdir, "audit.jsonl")
    toolpath = os.path.join(logdir, "tools.jsonl")

    routes = [
        ap.Route("/up-ant/", f"http://127.0.0.1:{mock_port}/up-ant", "anthropic-messages"),
        ap.Route("/up-oai/", f"http://127.0.0.1:{mock_port}/up-oai", "openai-completions"),
        ap.Route("/up-noid/", f"http://127.0.0.1:{mock_port}/up-noid", "openai-completions"),
        ap.Route("/up-json/", f"http://127.0.0.1:{mock_port}/up-json", "openai-completions"),
        ap.Route("/up-slow/", f"http://127.0.0.1:{mock_port}/up-slow", "anthropic-messages"),
        ap.Route("/up-complete/", f"http://127.0.0.1:{mock_port}/up-complete", "anthropic-messages"),
        ap.Route("/up-responses/", f"http://127.0.0.1:{mock_port}/up-responses", "openai-responses"),
    ]
    ap.AuditHandler.routes = routes
    ap.AuditHandler.raw_log = True  # Explicit raw mode preserves protocol-fixture assertions.
    ap.AuditHandler.logger = ap.JsonlLogger(logpath, max_bytes=10 * 1024 * 1024, backups=2)
    ap.AuditHandler.tool_logger = ap.JsonlLogger(toolpath)
    ap.AuditHandler.session_writer = ap.SessionJsonWriter(logdir)
    ap.TOOL_TRACE = ap.ToolTraceTracker()
    ap.AuditHandler.upstream_connect_timeout = 5.0

    proxy = ap.AuditHTTPServer(("127.0.0.1", 0), ap.AuditHandler)
    proxy.daemon_threads = True
    proxy_port = proxy.server_address[1]
    threading.Thread(target=proxy.serve_forever, daemon=True).start()

    ant_body = {
        "model": "mock-model", "max_tokens": 1024, "stream": True,
        "system": "你是审计测试系统",
        "messages": [{"role": "user", "content": "打个招呼"}],
    }
    oai_body = {
        "model": "mock-model", "stream": True,
        "messages": [{"role": "system", "content": "sys"}, {"role": "user", "content": "hi"}],
    }
    # 带 tool_result 的第二轮 anthropic 请求（验证工具结果结构入 ChatML）
    ant_body_tool = {
        "model": "mock-model", "stream": True, "system": "你是审计测试系统",
        "messages": [
            {"role": "user", "content": "打个招呼"},
            {"role": "assistant", "content": [
                {"type": "thinking", "thinking": "让我想想", "signature": "sig"},
                {"type": "text", "text": "你好"},
                {"type": "tool_use", "id": "toolu_mock_1", "name": "bash", "input": {"command": "ls"}}]},
            {"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": "toolu_mock_1",
                 "content": [{"type": "text", "text": "file1.txt"}]}]},
        ],
    }

    results = []
    status, data = post(proxy_port, "/up-ant/v1/messages", ant_body)
    results.append(("anthropic_sse_status", status == 200))
    results.append(("anthropic_sse_passthrough", b"msg_mock_01" in data))

    status, data = post(proxy_port, f"/up-ant/v1/messages?beta=true&key={SECRET_QUERY}", ant_body_tool)
    results.append(("anthropic_tool_round_status", status == 200))

    status, data = post(proxy_port, "/up-oai/chat/completions", oai_body)
    results.append(("openai_sse_status", status == 200))
    results.append(("openai_sse_passthrough", b"chatcmpl-mock-7" in data))

    oai_body_tool = {
        **oai_body,
        "messages": [
            *oai_body["messages"],
            {"role": "assistant", "content": None, "tool_calls": [{
                "id": "call_1", "type": "function",
                "function": {"name": "read", "arguments": "{\"path\":\"/x\"}"},
            }]},
            {"role": "tool", "tool_call_id": "call_1", "name": "read", "content": "file contents"},
        ],
    }
    status, _ = post(proxy_port, "/up-oai/chat/completions", oai_body_tool)
    results.append(("openai_tool_round_status", status == 200))

    # Synthetic names and a loopback mock upstream; no real API calls.
    rsp_body = {"model": "mock-model", "stream": True, "input": "echo pong",
                "tools": [{"type": "function", "name": "mcp__audit_echo",
                           "parameters": {"type": "object"}}]}
    status, data = post(proxy_port, "/up-responses/responses", rsp_body)
    results.append(("responses_sse_exact_passthrough", status == 200 and data == RESPONSES_SSE.encode()))
    rsp_tool_body = {**rsp_body, "previous_response_id": "resp_mock_1", "input": [
        {"type": "function_call_output", "call_id": "call_mcp_1", "output": "pong"}]}
    status, _ = post(proxy_port, "/up-responses/responses", rsp_tool_body)
    results.append(("responses_result_round_status", status == 200))
    status, data = post(proxy_port, "/up-responses/responses", {**rsp_body, "stream": False})
    results.append(("responses_json_passthrough", status == 200 and json.loads(data) == RESPONSES_JSON))

    # Recover a tool exchange when the proxy missed the original response.
    cold_body = {**oai_body, "messages": [
        {"role": "user", "content": "test MCP audit"},
        {"role": "assistant", "tool_calls": [{"id": "cold_mcp", "type": "function",
         "function": {"name": "mcp__audit_echo", "arguments": '{"text":"pong"}'}}]},
        {"role": "tool", "tool_call_id": "cold_mcp", "content": "pong"},
    ]}
    post(proxy_port, "/up-noid/chat/completions?case=cold", cold_body)
    post(proxy_port, "/up-noid/chat/completions?case=cold", cold_body)
    results.extend(cli_smoke(mock_port, cold_body))

    status, data = post(proxy_port, "/up-json/chat/completions", {**oai_body, "stream": False})
    results.append(("json_status", status == 200))
    results.append(("json_passthrough", b"chatcmpl-json-9" in data))

    post(proxy_port, "/up-noid/chat/completions", oai_body)
    post(proxy_port, "/up-noid/chat/completions", oai_body)  # 同 payload -> 同 hash
    post(proxy_port, "/up-noid/chat/completions",
         {**oai_body, "messages": [{"role": "system", "content": "sys"}, {"role": "user", "content": "different"}]})

    # 客户端断连：raw socket + SO_LINGER(0) 强制 RST，模拟 omp abort 流的行为
    import socket as _socket
    import struct as _struct
    sock = _socket.create_connection(("127.0.0.1", proxy_port), timeout=15)
    body = json.dumps(ant_body).encode()
    sock.sendall(
        b"POST /up-slow/v1/messages HTTP/1.1\r\nHost: 127.0.0.1\r\n"
        b"Content-Type: application/json\r\nContent-Length: " + str(len(body)).encode()
        + b"\r\n\r\n" + body)
    time.sleep(0.3)  # 拿到第一段 SSE
    sock.recv(65536)
    sock.setsockopt(_socket.SOL_SOCKET, _socket.SO_LINGER, _struct.pack("ii", 1, 0))
    sock.close()  # RST

    # OMP may close after the terminal SSE event while transport framing is still pending.
    MockUpstream.completed_client_closed.clear()
    sock = _socket.create_connection(("127.0.0.1", proxy_port), timeout=5)
    received = bytearray()
    try:
        sock.sendall(
            b"POST /up-complete/v1/messages HTTP/1.1\r\nHost: 127.0.0.1\r\n"
            b"Content-Type: application/json\r\nContent-Length: " + str(len(body)).encode()
            + b"\r\n\r\n" + body)
        while b'data: {"type":"message_stop"}\n\n' not in received:
            chunk = sock.recv(65536)
            if not chunk:
                raise RuntimeError("Fixture closed before the terminal SSE event")
            received.extend(chunk)
        sock.setsockopt(_socket.SOL_SOCKET, _socket.SO_LINGER, _struct.pack("ii", 1, 0))
    finally:
        sock.close()
        MockUpstream.completed_client_closed.set()

    time.sleep(1.0)  # 等断连记录落盘
    proxy.shutdown()
    mock.shutdown()
    ap.AuditHandler.logger.close()
    ap.AuditHandler.tool_logger.close()
    proxy.server_close()
    mock.server_close()

    with open(logpath, encoding="utf-8") as fh:
        lines = [json.loads(x) for x in fh if x.strip()]
    raw_log = open(logpath, encoding="utf-8").read()
    session_documents = list(ap.iter_session_records(logdir))
    results.append(("incremental_journal_covers_all_recorded_exchanges",
                    {item["exchange_id"] for item in session_documents} ==
                    {item["exchange_id"] for item in lines if item.get("request")}))
    results.append(("incremental_journal_does_not_reintroduce_redacted_headers",
                    all(secret not in json.dumps(session_documents)
                        for secret in (SECRET_HEADER, SECRET_APIKEY, SECRET_COOKIE, SECRET_QUERY))))

    by_path = {}
    noid_records = []
    for rec in lines:
        by_path.setdefault(rec.get("path", ""), []).append(rec)
        if rec.get("path", "") == "/up-noid/chat/completions":
            noid_records.append(rec)

    r_ant = by_path["/up-ant/v1/messages"][0]
    results.append(("anthropic_id_from_body",
                    r_ant["classification"]["primary_id"] == "msg_mock_01"
                    and r_ant["classification"]["primary_source"] == "body:message_start.message.id"
                    and r_ant["classification"]["confidence"] == "high"))
    results.append(("anthropic_header_id_also_recorded",
                    any(i["id"] == "req_header_mock_1" and i["source"] == "header:request-id"
                        for i in r_ant["classification"]["ids"])))
    last = r_ant["chatml"]["messages"][-1]
    results.append(("anthropic_thinking_captured", last.get("thinking") == "让我想想"))
    results.append(("anthropic_text_captured", last.get("content") == "你好"))
    results.append(("anthropic_tool_call_captured",
                    last.get("tool_calls") == [{"id": "toolu_mock_1", "name": "bash",
                                                "arguments": {"command": "ls"}}]))
    results.append(("anthropic_tool_call_trace_once",
                    [e["tool_call_id"] for e in r_ant["tool_trace"] if e["event"] == "tool_call"]
                    == ["toolu_mock_1"]))
    results.append(("anthropic_stop_reason", last.get("stop_reason") == "tool_use"))
    results.append(("anthropic_usage", r_ant["response"]["usage"]["output_tokens"] == 17))
    results.append(("request_body_logged_raw", r_ant["request"]["body"]["system"] == "你是审计测试系统"))

    r_tool = next(r for p, rs in by_path.items() if p.startswith("/up-ant/v1/messages?beta=true")
                  for r in rs)
    tool_msg = [m for m in r_tool["chatml"]["messages"] if m.get("role") == "tool"]
    results.append(("chatml_tool_result_structured",
                    bool(tool_msg) and tool_msg[0]["tool_results"][0]["tool_use_id"] == "toolu_mock_1"
                    and tool_msg[0]["tool_results"][0]["content"] == "file1.txt"))
    ant_result = next((e for e in r_tool["tool_trace"] if e["event"] == "tool_result"), {})
    results.append(("anthropic_tool_result_correlated",
                    ant_result.get("correlated") is True
                    and ant_result.get("tool_call_id") == "toolu_mock_1"
                    and ant_result.get("tool_name") == "bash"
                    and ant_result.get("call_exchange_id") == r_ant["exchange_id"]
                    and ant_result.get("result_exchange_id") == r_tool["exchange_id"]
                    and ant_result.get("content") == [{"type": "text", "text": "file1.txt"}]))
    results.append(("query_key_redacted_in_path", SECRET_QUERY not in raw_log))

    r_oai = by_path["/up-oai/chat/completions"][0]
    last_oai = r_oai["chatml"]["messages"][-1]
    results.append(("openai_id", r_oai["classification"]["primary_id"] == "chatcmpl-mock-7"))
    results.append(("openai_reasoning", last_oai.get("thinking") == "想一下"))
    results.append(("openai_tool_calls",
                    last_oai.get("tool_calls") == [{"id": "call_1", "name": "read",
                                                    "arguments": {"path": "/x"}}]))
    r_oai_tool = [r for r in by_path["/up-oai/chat/completions"]
                  if any(e.get("event") == "tool_result" for e in r.get("tool_trace", []))][0]
    oai_result = next(e for e in r_oai_tool["tool_trace"] if e["event"] == "tool_result")
    results.append(("openai_tool_result_correlated",
                    oai_result.get("correlated") is True
                    and oai_result.get("tool_call_id") == "call_1"
                    and oai_result.get("tool_name") == "read"
                    and oai_result.get("call_exchange_id") == r_oai["exchange_id"]
                    and oai_result.get("result_exchange_id") == r_oai_tool["exchange_id"]
                    and oai_result.get("content") == "file contents"))

    r_json = by_path["/up-json/chat/completions"][0]
    results.append(("json_id", r_json["classification"]["primary_id"] == "chatcmpl-json-9"))
    results.append(("json_reasoning", r_json["chatml"]["messages"][-1].get("thinking") == "非流式思考"))

    rsp_records = by_path["/up-responses/responses"]
    rsp_first = next(r for r in rsp_records if r["request"]["body"]["input"] == "echo pong" and r["response"]["sse"])
    rsp_result = next(r for r in rsp_records if isinstance(r["request"]["body"]["input"], list))
    rsp_nonstream = next(r for r in rsp_records if not r["response"]["sse"])
    expected = [{"id": "call_mcp_1", "name": "mcp__audit_echo", "arguments": {"text": "pong"}}]
    results.append(("responses_item_id_is_not_call_id", rsp_first["chatml"]["messages"][-1]["tool_calls"] == expected))
    results.append(("responses_json_tools", rsp_nonstream["chatml"]["messages"][-1]["tool_calls"] == expected))
    rsp_event = next(e for e in rsp_result["tool_trace"] if e["event"] == "tool_result")
    results.append(("responses_result_links_previous_response", rsp_event["call_exchange_id"] == rsp_first["exchange_id"]
                    and rsp_event["tool_name"] == "mcp__audit_echo" and rsp_event["content"] == "pong"))
    results.append(("tool_catalog_preserves_mcp_name", rsp_first["tool_catalog"] == [
        {"name": "mcp__audit_echo", "tool_kind_hint": "mcp"}]))
    cold_records = by_path["/up-noid/chat/completions?case=cold"]
    cold_events = [next(e for e in r["tool_trace"] if e["event"] == "tool_result") for r in cold_records]
    results.append(("cold_start_recovers_mcp_name_args_result", all(
        e["correlated"] and e["tool_name"] == "mcp__audit_echo" and e["arguments"] == {"text": "pong"}
        and e["content"] == "pong" and e["call_exchange_id"] is None for e in cold_events)))
    results.append(("repeated_history_is_marked_not_dropped", sorted(e["replayed_context"] for e in cold_events) == [False, True]))
    with open(toolpath, encoding="utf-8") as fh:
        tool_lines = [json.loads(line) for line in fh if line.strip()]
    results.append(("dedicated_log_contains_builtin_and_mcp", {"omp_builtin", "mcp"} <=
                    {e["tool_kind_hint"] for e in tool_lines if e["event"] == "tool_result"}))
    results.append(("dedicated_log_contains_linked_arguments_and_content", any(
        e.get("tool_name") == "mcp__audit_echo" and e.get("arguments") == {"text": "pong"}
        and e.get("content") == "pong" for e in tool_lines)))
    results.extend(tool_regressions())
    results.extend(launcher_regressions())
    results.extend(launcher_fetch_regressions())
    from test_launcher_extension import run_checks as launcher_extension_checks
    results.extend(launcher_extension_checks())
    from test_proxy_timeouts import run_checks as proxy_timeout_checks
    results.extend(proxy_timeout_checks())
    from test_tool_occurrences import run_checks as tool_occurrence_checks
    results.extend(tool_occurrence_checks())
    from test_proxy_redirects import run_checks as proxy_redirect_checks
    results.extend(proxy_redirect_checks())
    results.extend(shim_route_regressions())
    results.extend(session_archive_regressions())
    results.extend(windows_launcher_regressions())
    if "--real-omp" in sys.argv:
        results.extend(real_omp_smoke())
        results.extend(real_omp_smoke("anthropic-messages"))
        results.extend(real_omp_smoke(discovered=True))

    fb_ids = [r["classification"]["primary_id"] for r in noid_records]
    results.append(("fallback_used", all(r["classification"]["confidence"] == "low" for r in noid_records)))
    results.append(("fallback_stable", fb_ids[0] == fb_ids[1] and fb_ids[0].startswith("sha256:")))
    results.append(("fallback_distinct", fb_ids[2] != fb_ids[0]))

    r_slow = by_path["/up-slow/v1/messages"][0]
    results.append(("client_disconnect_recorded", r_slow.get("client_disconnect") is True))
    results.append(("unfinished_client_disconnect_still_marks_truncation",
                    r_slow["response"]["sse_complete"] is False
                    and r_slow.get("response_truncated_by") == "client_disconnect"))
    r_complete = by_path["/up-complete/v1/messages"][0]
    results.append(("disconnect_after_terminal_preserves_complete_capture",
                    r_complete.get("client_disconnect") is True and r_complete["response"]["sse_complete"] is True
                    and "response_truncated_by" not in r_complete
                    and r_complete["chatml"]["messages"][-1]["thinking"] == "让我想想"))
    complete_turns = []
    for name in os.listdir(logdir):
        if name.startswith("session-") and name.endswith("-rl.json"):
            with open(os.path.join(logdir, name), encoding="utf-8") as fh:
                complete_turns.extend(t for t in json.load(fh)["turns"] if t["exchange_id"] == r_complete["exchange_id"])
    results.append(("readable_json_distinguishes_complete_sse_from_socket_disconnect", len(complete_turns) == 1
                    and complete_turns[0]["sse_complete"] is True and "response_truncated_by" not in complete_turns[0]))

    results.append(("redact_authorization", SECRET_HEADER not in raw_log))
    results.append(("redact_x_api_key", SECRET_APIKEY not in raw_log))
    results.append(("redact_cookie", SECRET_COOKIE not in raw_log))
    results.append(("redact_marker_present", '"<redacted>"' in raw_log))

    print(f"日志文件: {logpath} ({len(lines)} 条记录)")
    failed = 0
    for name, ok in results:
        print(f"  {'PASS' if ok else 'FAIL'}  {name}")
        failed += 0 if ok else 1
    print(f"\n{len(results) - failed}/{len(results)} 通过")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
