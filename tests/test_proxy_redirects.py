"""Real-fetch regression for automatic redirects bypassing audit and leaking identity."""
import json
import os
from pathlib import Path
import queue
import shutil
import subprocess
import tempfile
import threading
import urllib.parse
from http.server import BaseHTTPRequestHandler

import omp_audit_proxy as ap


NODE_CLIENT = r'''
import install from "./audit.mjs";

const hooks = new Map();
install({on: (name, callback) => hooks.set(name, callback)});
const model = {provider: "redirect-fixture", id: "fixture", api: "openai-completions",
  baseUrl: process.env.REDIRECT_FIXTURE_BASE};
const ctx = {model, modelRegistry: {getAll: () => [model]},
  sessionManager: {getSessionId: () => "redirect-fixture-session"}, agent: {id: "Main"}};
await hooks.get("session_start")({}, ctx);
const results = [];
for (const status of [301, 302, 303, 307, 308, 0]) {
  const body = await hooks.get("before_provider_request")({payload: {
    model: "fixture", messages: [{role: "user", content: "redirect regression"}],
  }}, ctx);
  const response = await fetch(model.baseUrl + "/chat/completions?redirect=" + status, {
    method: "POST", headers: {"Content-Type": "application/json", Authorization: "Bearer fixture-auth"},
    body: JSON.stringify(body), signal: AbortSignal.timeout(4000),
  });
  results.push({upstream_status: status, status: response.status,
    location: response.headers.get("Location"), connection: response.headers.get("Connection"),
    body: await response.json()});
}
console.log(JSON.stringify(results));
'''


def run_checks():
    node = shutil.which("node")
    if not node:
        print("SKIP Real-fetch redirect tests: node not installed")
        return []
    first_hops, second_hops, records, released = (queue.Queue() for _ in range(4))

    class Capture:
        def write(self, record):
            records.put(record)

    class Target(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_GET(self):
            self.do_POST()

        def do_POST(self):
            body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            second_hops.put({"method": self.command, "path": self.path, "body": body})
            data = b'{"id":"unaudited-second-hop","choices":[]}'
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

    destination = ap.AuditHTTPServer(("127.0.0.1", 0), Target)

    class Upstream(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *_args):
            pass

        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            status = int(urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query)["redirect"][0])
            first_hops.put({"status": status, "path": self.path, "body": body,
                            "authorization": self.headers.get("Authorization")})
            if status:
                self.send_response(status)
                self.send_header("Location", f"http://127.0.0.1:{destination.server_port}/final/{status}")
                self.send_header("X-Response-ID", f"redirect-{status}")
                # Do not send the declared redirect body. The proxy must close
                # upstream immediately instead of trying to drain it forever.
                self.send_header("Content-Length", "4096")
                self.end_headers()
                self.wfile.flush()
                self.connection.settimeout(2)
                try:
                    closed = self.rfile.read(1) == b""
                except OSError:
                    closed = False
                released.put((status, closed))
                self.close_connection = True
                return
            data = json.dumps({"id": "normal-fixture", "choices": [{"message": {
                "role": "assistant", "content": "normal output", "reasoning_content": "normal reasoning",
            }}]}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

    upstream = ap.AuditHTTPServer(("127.0.0.1", 0), Upstream)
    handler = type("RedirectTestHandler", (ap.AuditHandler,), {
        "routes": [], "control_token": "redirect-fixture-token",
        "logger": Capture(), "tool_logger": None, "session_writer": None, "raw_log": True,
    })
    proxy = ap.AuditHTTPServer(("127.0.0.1", 0), handler)
    servers = (upstream, destination, proxy)
    for server in servers:
        threading.Thread(target=server.serve_forever, daemon=True).start()

    try:
        with tempfile.TemporaryDirectory(prefix="omp-redirect-test-") as directory:
            Path(directory, "audit.mjs").write_text(ap.LAUNCHER_EXTENSION, encoding="utf-8")
            Path(directory, "client.mjs").write_text(NODE_CLIENT, encoding="utf-8")
            environment = {**os.environ,
                           "OMP_AUDIT_CONTROL_URL": f"http://127.0.0.1:{proxy.server_port}",
                           "OMP_AUDIT_CONTROL_TOKEN": handler.control_token,
                           "REDIRECT_FIXTURE_BASE": f"http://127.0.0.1:{upstream.server_port}/v1"}
            result = subprocess.run([node, "client.mjs"], cwd=directory, env=environment,
                                    capture_output=True, text=True, encoding="utf-8", timeout=20,
                                    creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
            if result.returncode:
                raise RuntimeError("Real-fetch redirect regression failed: " + result.stderr)
            replies = json.loads(result.stdout)
        evidence = [records.get(timeout=3) for _ in replies]
        first_requests = [first_hops.get(timeout=3) for _ in replies]
        closed_connections = [released.get(timeout=3) for _ in range(5)]
        blocked = replies[:5]
        checks = [("automatic_redirect_" + str(reply["upstream_status"]) + "_returns_502_without_location",
                   reply["status"] == 502 and reply["location"] is None and reply["connection"] == "close"
                   and reply["body"] == {"error": "upstream_redirect_blocked",
                                         "upstream_status": reply["upstream_status"]}) for reply in blocked]
        checks.append(("automatic_redirect_real_fetch_never_reaches_second_upstream", second_hops.empty()))
        checks.append(("automatic_redirect_private_marker_and_signature_never_reach_upstream",
                       all("_omp_audit_v1" not in item["body"] for item in first_requests)
                       and all("signature" not in json.dumps(item) for item in first_requests)
                       and second_hops.empty()))
        checks.append(("automatic_redirect_keeps_original_path_and_authorization",
                       all(item["path"] == "/v1/chat/completions?redirect=" + str(item["status"])
                           and item["authorization"] == "Bearer fixture-auth" for item in first_requests)))
        checks.append(("automatic_redirect_releases_unread_upstream_connections",
                       sorted(closed_connections) == [(status, True) for status in (301, 302, 303, 307, 308)]))
        redirect_evidence = {item["response"]["status"]: item for item in evidence
                             if item.get("error") == "upstream_redirect_blocked"}
        checks.append(("automatic_redirect_audit_keeps_upstream_status_location_and_proxy_error",
                       set(redirect_evidence) == {301, 302, 303, 307, 308}
                       and all(item["proxy_response"]["status"] == 502
                               and item["response"]["headers"].get("Location") ==
                               f"http://127.0.0.1:{destination.server_port}/final/{status}"
                               and item["classification"]["primary_id"] == f"redirect-{status}"
                               for status, item in redirect_evidence.items())))
        normal = next(item for item in evidence if item["response"]["status"] == 200)
        checks.append(("automatic_redirect_guard_preserves_normal_response_and_thinking",
                       replies[-1]["status"] == 200 and replies[-1]["body"]["id"] == "normal-fixture"
                       and not normal.get("error")
                       and normal["chatml"]["messages"][-1]["thinking"] == "normal reasoning"))
        return checks
    finally:
        for server in servers:
            server.shutdown()
            server.server_close()


if __name__ == "__main__":
    checks = run_checks()
    for name, ok in checks:
        print(("PASS " if ok else "FAIL ") + name)
    print(f"{sum(ok for _, ok in checks)}/{len(checks)} passed")
    raise SystemExit(not all(ok for _, ok in checks))
