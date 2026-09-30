#!/usr/bin/env python3
"""Local regressions for dynamic launcher routes and authenticated session IDs."""

import hashlib
import hmac
import http.client
import json
import os
import queue
import shutil
import subprocess
import tempfile
import threading
from http.server import BaseHTTPRequestHandler

import omp_audit_proxy as ap


NODE_HARNESS = r'''
import { createHmac } from "node:crypto";
import install from "./audit.mjs";

const scenario = process.argv[2] || "normal";
const root = "http://127.0.0.1:45871";
const token = "fixture-private-token";
process.env.OMP_AUDIT_CONTROL_URL = root;
process.env.OMP_AUDIT_CONTROL_TOKEN = token;
const registrations = [];
const forwarded = [];
const checks = [];
let failRegistration = false;
const routes = new Map();
const controlFetch = async (input, init) => {
  const req = input instanceof Request ? new Request(input, init) : new Request(String(input), init);
  if (req.url === root + "/__audit/routes") {
    if (req.headers.get("authorization") !== "Bearer " + token) throw new Error("Bad control auth");
    if (failRegistration) return new Response("{}", {status: 503});
    const data = JSON.parse(await req.text());
    const registered = data.routes.map(entry => {
      registrations.push(entry);
      const key = JSON.stringify([entry.provider, entry.api, entry.upstream]);
      if (!routes.has(key)) routes.set(key, root + "/_audit/r" + routes.size);
      return {provider: entry.provider, baseUrl: routes.get(key)};
    });
    return new Response(JSON.stringify({routes: registered}), {headers: {"Content-Type": "application/json"}});
  }
  forwarded.push({url: req.url, method: req.method, body: await req.text(),
    headers: Object.fromEntries(req.headers), signal: req.signal, input, init});
  return new Response("{}", {headers: {"Content-Type": "application/json"}});
};
globalThis.fetch = controlFetch;
const factory = () => {
  const hooks = new Map();
  install({on: (name, handler) => hooks.set(name, handler)});
  return hooks;
};
const modelA = {provider: "provider-a", api: "openai-completions", id: "alpha", baseUrl: "https://a.example/v1"};
const ctx = (model, sessionId = "session-root", agentId = "Main") => ({
  model, modelRegistry: {getAll: () => [modelA]},
  sessionManager: {getSessionId: () => sessionId}, agent: {id: agentId},
});
const main = factory();
process.on("exit", () => {
  if (scenario !== "normal") process.stdout.write("EXIT_STATS:" + JSON.stringify({outbound: forwarded.length}) + "\n");
});
await main.get("session_start")({}, ctx(modelA));
if (scenario === "unsupported") {
  await main.get("context")({}, ctx({...modelA, provider: "devin", api: "devin-agent"}));
} else if (scenario === "stale") {
  await main.get("context")({}, ctx({...modelA, baseUrl: "http://127.0.0.1:11111/_audit/old"}));
} else if (scenario === "registration-failed") {
  failRegistration = true;
  await main.get("context")({}, ctx({...modelA, baseUrl: "https://new.example/v1"}));
} else if (scenario === "identity") {
  await main.get("before_provider_request")({payload: {}}, ctx(modelA, ""));
} else {
  const check = (name, ok) => checks.push([name, Boolean(ok)]);
  const wrapper = globalThis.fetch;
  const worker = factory();
  check("extension_wrapper_installed_once_for_subagents", wrapper === globalThis.fetch);
  const payload = {model: "alpha", messages: [{role: "user", content: "same prompt"}]};
  const first = await main.get("before_provider_request")({payload}, ctx(modelA));
  const again = await main.get("before_provider_request")({payload}, ctx(modelA));
  const vibe = await worker.get("before_provider_request")({payload}, ctx(modelA, "session-vibe", "vibe-worker-1"));
  const expected = createHmac("sha256", token).update("session-root\0Main").digest("hex");
  check("extension_session_signature_valid_and_stable", first._omp_audit_v1.signature === expected &&
    JSON.stringify(first._omp_audit_v1) === JSON.stringify(again._omp_audit_v1));
  check("extension_vibe_uses_own_session_identity", vibe._omp_audit_v1.session_id === "session-vibe" &&
    vibe._omp_audit_v1.agent_id === "vibe-worker-1" && vibe._omp_audit_v1.signature !== expected);
  check("extension_payload_preserved_without_mutation", !payload._omp_audit_v1 && first.messages === payload.messages);

  const modelB = {provider: "provider-b", api: "anthropic-messages", id: "beta", baseUrl: "https://b.example/gateway/v1"};
  await main.get("context")({}, ctx(modelB));
  await globalThis.fetch(modelB.baseUrl + "/messages?beta=true", {method: "POST", body: "{}"});
  const routeB = routes.get(JSON.stringify([modelB.provider, modelB.api, modelB.baseUrl]));
  check("extension_dynamic_provider_registered_before_request", forwarded.at(-1).url === routeB + "//messages?beta=true");
  const modelC = {...modelB, baseUrl: "https://c.example/gateway/v1/"};
  await main.get("context")({}, ctx(modelC));
  await globalThis.fetch("https://c.example/gateway/v1/messages", {method: "POST", body: "{}"});
  const routeC = routes.get(JSON.stringify([modelC.provider, modelC.api, modelC.baseUrl.replace(/\/+$/, "")]));
  check("extension_same_provider_new_endpoint_gets_new_route", routeC !== routeB && forwarded.at(-1).url === routeC + "//messages");
  const modelD = {...modelA, provider: "provider-d", baseUrl: "https://d.example/v1"};
  await Promise.all([main.get("context")({}, ctx(modelD)), worker.get("context")({}, ctx(modelD, "worker-d", "worker"))]);
  check("extension_concurrent_endpoint_registration_coalesced", registrations.filter(e => e.provider === "provider-d").length === 1);

  const controller = new AbortController();
  const options = {method: "POST", body: JSON.stringify(first), headers: {Authorization: "Bearer fixture-auth"}, signal: controller.signal};
  await globalThis.fetch(modelA.baseUrl + "/chat/completions?mode=stream", options);
  const initForward = forwarded.at(-1);
  check("extension_fetch_init_headers_body_and_signal_preserved", initForward.init === options &&
    initForward.headers.authorization === "Bearer fixture-auth" && JSON.parse(initForward.body)._omp_audit_v1.signature === expected);
  controller.abort();
  check("extension_fetch_init_abort_propagates", initForward.signal.aborted);
  const requestController = new AbortController();
  const request = new Request(modelA.baseUrl + "/responses?case=request", {method: "POST", body: "fixture-body",
    headers: {"x-api-key": "fixture-key"}, signal: requestController.signal});
  await globalThis.fetch(request);
  const requestForward = forwarded.at(-1);
  requestController.abort();
  check("extension_request_object_preserves_body_auth_and_abort", requestForward.body === "fixture-body" &&
    requestForward.headers["x-api-key"] === "fixture-key" && requestForward.signal.aborted &&
    requestForward.url.endsWith("//responses?case=request"));
  const count = forwarded.length;
  let blocked = false;
  try { await globalThis.fetch("https://unknown.example/v1/responses/", {method: "POST", body: "{}"}); }
  catch (error) { blocked = String(error).includes("Uncovered model request blocked"); }
  check("extension_unknown_inference_post_blocked", blocked && forwarded.length === count);
  await globalThis.fetch("https://public.example/messages/");
  check("extension_non_inference_get_allowed", forwarded.at(-1).url === "https://public.example/messages/");
  let prefixBlocked = false;
  try { await globalThis.fetch("https://a.example/v10/chat/completions", {method: "POST", body: "{}"}); }
  catch (error) { prefixBlocked = String(error).includes("Uncovered model request blocked"); }
  check("extension_endpoint_matching_respects_path_boundary", prefixBlocked);
  process.stdout.write(JSON.stringify(checks) + "\n");
}
'''


def extension_checks():
    node = shutil.which("node")
    if not node:
        print("SKIP Node launcher extension tests: node not installed")
        return []
    results = []
    with tempfile.TemporaryDirectory(prefix="omp-extension-test-") as directory:
        for name, source in (("audit.mjs", ap.LAUNCHER_EXTENSION), ("harness.mjs", NODE_HARNESS)):
            with open(os.path.join(directory, name), "w", encoding="utf-8") as handle:
                handle.write(source)
        for scenario in ("normal", "unsupported", "stale", "registration-failed", "identity"):
            process = subprocess.run([node, "harness.mjs", scenario], cwd=directory, capture_output=True,
                                     text=True, encoding="utf-8", timeout=30,
                                     creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
            if scenario == "normal":
                if process.returncode:
                    raise RuntimeError(process.stderr)
                results.extend((name, ok) for name, ok in json.loads(process.stdout))
            else:
                results.append(("extension_" + scenario.replace("-", "_") + "_stops_before_outbound",
                                process.returncode == 1 and '[audit] routing failed' in process.stderr
                                and 'EXIT_STATS:{"outbound":0}' in process.stdout))
    return results


def identity_checks():
    token = "fixture-private-token"
    received = queue.Queue()
    records = queue.Queue()

    class Capture:
        def write(self, record):
            records.put(record)

    class Upstream(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_POST(self):
            body = self.rfile.read(int(self.headers["Content-Length"]))
            parsed = json.loads(body)
            received.put({"body": parsed, "body_bytes": len(body), "headers": dict(self.headers),
                          "length_headers": self.headers.get_all("Content-Length"), "path": self.path})
            response = {"id": "fixture-response", "choices": [], "content": [], "output": []}
            if parsed.get("fixture_tool_call"):
                response["choices"] = [{"message": {"role": "assistant", "tool_calls": [{
                    "id": "shared-fixture-call", "type": "function",
                    "function": {"name": "read", "arguments": '{"path":"fixture.txt"}'}}]}}]
            data = json.dumps(response).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

    upstream = ap.AuditHTTPServer(("127.0.0.1", 0), Upstream)
    handler = type("IdentityTestHandler", (ap.AuditHandler,), {
        "routes": [ap.Route("/fixture/", f"http://127.0.0.1:{upstream.server_port}", None)],
        "control_token": token, "logger": Capture(), "tool_logger": None, "session_writer": None, "raw_log": True,
    })
    proxy = ap.AuditHTTPServer(("127.0.0.1", 0), handler)
    for server in (upstream, proxy):
        threading.Thread(target=server.serve_forever, daemon=True).start()

    def marker(session_id, agent_id):
        signature = hmac.new(token.encode(), (session_id + "\0" + agent_id).encode(), hashlib.sha256).hexdigest()
        return {"session_id": session_id, "agent_id": agent_id, "signature": signature}

    def post(path, payload):
        connection = http.client.HTTPConnection("127.0.0.1", proxy.server_port, timeout=5)
        data = json.dumps(payload, ensure_ascii=False).encode()
        connection.request("POST", "/fixture/" + path, body=data,
                           headers={"Content-Type": "application/json", "content-length": str(len(data))})
        response = connection.getresponse()
        status = response.status
        response.read()
        connection.close()
        return status, records.get(timeout=5)

    results = []
    try:
        for path, protocol in (("v1/messages", "anthropic-messages"),
                               ("chat/completions", "openai-completions"), ("responses", "openai-responses")):
            identity = marker("session-" + protocol, "worker-" + protocol)
            payload = {"model": "fixture", "messages": [{"role": "user", "content": "测试输入"}],
                       "input": "测试输入", "_omp_audit_v1": identity}
            status, record = post(path, payload)
            observed = received.get(timeout=5)
            expected = {key: value for key, value in payload.items() if key != "_omp_audit_v1"}
            results.append(("identity_" + protocol + "_authenticated_and_stripped",
                            status == 200 and record.get("audit_session_id") == identity["session_id"]
                            and record.get("audit_agent_id") == identity["agent_id"]
                            and observed["body"] == expected and record["request"]["body"] == expected
                            and identity["signature"] not in json.dumps(record)
                            and identity["signature"] not in json.dumps(observed["headers"])))
            results.append(("identity_" + protocol + "_content_length_recomputed_once",
                            observed["length_headers"] == [str(observed["body_bytes"])]))
        root_status, root_record = post("chat/completions", {"messages": [], "_omp_audit_v1": marker("root", "Main")})
        received.get(timeout=5)
        vibe_status, vibe_record = post("chat/completions", {"messages": [], "_omp_audit_v1": marker("vibe", "worker")})
        received.get(timeout=5)
        results.append(("identity_root_and_vibe_remain_distinct", root_status == vibe_status == 200
                        and root_record["audit_session_id"] == "root" and vibe_record["audit_session_id"] == "vibe"))
        status, call_record = post("chat/completions", {"messages": [], "fixture_tool_call": True,
                                                       "_omp_audit_v1": marker("tool-root", "Main")})
        received.get(timeout=5)
        result_message = {"role": "tool", "tool_call_id": "shared-fixture-call", "content": "file content"}
        trace_checks = []
        for session_id, agent_id in (("tool-root", "Main"), ("tool-vibe", "Main"), ("tool-root", "worker")):
            result_status, result_record = post("chat/completions", {
                "messages": [result_message], "_omp_audit_v1": marker(session_id, agent_id)})
            received.get(timeout=5)
            trace = next(event for event in result_record["tool_trace"] if event["event"] == "tool_result")
            expected_match = session_id == "tool-root" and agent_id == "Main"
            trace_checks.append(result_status == 200 and trace["correlated"] == expected_match
                                and trace["call_exchange_id"] == (call_record["exchange_id"] if expected_match else None))
        results.append(("identity_tool_trace_isolated_by_session_and_agent", status == 200 and all(trace_checks)))
        invalid = [None, {}, {**marker("valid", "Main"), "signature": "0" * 64},
                   {**marker("valid", "Main"), "session_id": "other"},
                   {**marker("valid", "Main"), "agent_id": "other"}, marker("", "Main"),
                   marker("contains\0separator", "Main"), marker("x" * 513, "Main")]
        rejected = []
        for value in invalid:
            status, record = post("chat/completions", {"messages": [], "_omp_audit_v1": value})
            rejected.append(status == 403 and record.get("error") == "invalid_audit_identity"
                            and "audit_session_id" not in record and "request" not in record)
        results.append(("identity_invalid_markers_rejected_without_forwarding", all(rejected) and received.empty()))
        status, record = post("chat/completions", {"messages": []})
        received.get(timeout=5)
        results.append(("identity_untagged_auxiliary_and_manual_requests_allowed", status == 200 and "audit_session_id" not in record))
        handler.control_token = None
        status, record = post("chat/completions", {"messages": [], "_omp_audit_v1": marker("root", "Main")})
        results.append(("identity_marker_rejected_without_launcher_secret", status == 403 and received.empty()
                        and "audit_session_id" not in record))
    finally:
        for server in (proxy, upstream):
            server.shutdown()
            server.server_close()
    return results


def run_checks():
    return extension_checks() + identity_checks()


if __name__ == "__main__":
    results = run_checks()
    for name, ok in results:
        print(("PASS" if ok else "FAIL") + " " + name)
    print(f"{sum(ok for _, ok in results)}/{len(results)} passed")
    raise SystemExit(0 if all(ok for _, ok in results) else 1)
