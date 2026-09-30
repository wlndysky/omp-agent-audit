"""Loopback regressions for upstream connection and close-delimited SSE timeouts."""
import http.client
import json
import queue
import threading
import time
from http.server import BaseHTTPRequestHandler

import omp_audit_proxy as ap


def run_checks():
    records = queue.Queue()
    release_delayed_headers = threading.Event()
    first_frame = b'data: {"id":"timeout-fixture","choices":[{"delta":{"content":"start"}}]}\n\n'
    final_frames = (
        b'data: {"id":"timeout-fixture","choices":[{"delta":{"reasoning_content":"after idle"}}]}\n\n'
        b'data: [DONE]\n\n'
    )

    class Capture:
        def write(self, record):
            records.put(record)

    class Upstream(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *_args):
            pass

        def do_POST(self):
            self.rfile.read(int(self.headers.get("Content-Length", "0")))
            if self.path == "/delayed-headers":
                # Keep headers pending until the client has already received
                # the proxy's timeout response, so this cannot pass by timing.
                release_delayed_headers.wait(timeout=5)
                self.close_connection = True
                return
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Connection", "close")
            self.end_headers()
            self.close_connection = True
            try:
                self.wfile.write(first_frame)
                self.wfile.flush()
                time.sleep(0.5)  # Longer than the proxy's connection timeout.
                self.wfile.write(final_frames)
                self.wfile.flush()
            except OSError:
                pass  # Expected when exercising the unpatched proxy.

    upstream = ap.AuditHTTPServer(("127.0.0.1", 0), Upstream)
    handler = type("TimeoutTestHandler", (ap.AuditHandler,), {
        "routes": [ap.Route("/fixture/", f"http://127.0.0.1:{upstream.server_port}",
                            "openai-completions")],
        "upstream_connect_timeout": 0.15,
        "logger": Capture(), "tool_logger": None, "session_writer": None, "raw_log": True,
    })
    proxy = ap.AuditHTTPServer(("127.0.0.1", 0), handler)
    for server in (upstream, proxy):
        threading.Thread(target=server.serve_forever, daemon=True).start()

    def request(path):
        connection = http.client.HTTPConnection("127.0.0.1", proxy.server_port, timeout=2)
        result = {"status": None, "body": None, "connection": None, "error": None}
        try:
            connection.request("POST", "/fixture/" + path, body=b'{"messages":[]}',
                               headers={"Content-Type": "application/json"})
            response = connection.getresponse()
            result.update(status=response.status, connection=response.getheader("Connection"))
            result["body"] = response.read()
        except (OSError, http.client.HTTPException) as exc:
            result["error"] = type(exc).__name__
        finally:
            connection.close()
        return result, records.get(timeout=3)

    checks = []
    try:
        client, record = request("sse-close")
        checks.append(("close_delimited_sse_survives_idle_beyond_connect_timeout",
                       client["error"] is None and client["status"] == 200
                       and client["body"] == first_frame + final_frames))
        assistant = (record.get("chatml") or {}).get("messages", [{}])[-1]
        checks.append(("close_delimited_sse_keeps_tail_thinking_and_completion",
                       not record.get("error") and not record.get("response_truncated_by")
                       and record.get("response", {}).get("sse_complete") is True
                       and assistant.get("thinking") == "after idle"))
        client, record = request("delayed-headers")
        checks.append(("upstream_header_timeout_returns_504_and_closes_client",
                       client["error"] is None and client["status"] == 504
                       and client["connection"] == "close"
                       and json.loads(client["body"]) == {"error": "upstream_connect_timeout"}))
        checks.append(("upstream_header_timeout_keeps_specific_audit_error",
                       record.get("error") == "upstream_connect_timeout"))
    finally:
        release_delayed_headers.set()
        for server in (proxy, upstream):
            server.shutdown()
            server.server_close()
    return checks


if __name__ == "__main__":
    results = run_checks()
    for name, ok in results:
        print(("PASS " if ok else "FAIL ") + name)
    print(f"{sum(ok for _, ok in results)}/{len(results)} passed")
    raise SystemExit(not all(ok for _, ok in results))
