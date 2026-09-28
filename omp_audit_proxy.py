#!/usr/bin/env python3
"""
omp_audit_proxy.py — 本地（127.0.0.1-only）LLM API 流式审计反向代理，单文件，纯标准库。

用途：作为 OMP（或任何 OpenAI/Anthropic 兼容客户端）与真实上游之间的透明中转，
把每次请求/响应（含 SSE 流）以 JSONL 落盘，并按 response/request ID 分类；
无 ID 时对 system+首条 user 消息做 sha256 作为流标识。

它能看到什么（边界声明）：
  - 经过本代理的 HTTP 请求体原文、响应头、SSE 事件流原文。
  - 模型通过 API 暴露的 thinking/reasoning 增量、文本增量、tool_use/tool_calls 结构，
    以及下一次请求体中回传的 tool_result（Anthropic）/ role:"tool" 消息（OpenAI）。
它看不到什么：
  - 模型未通过 API 暴露的隐藏推理（不存在可记录性）。
  - OMP 进程内的本地工具执行细节（bash 实际执行、文件读写等）——只能看到
    发给模型的调用参数与回传给模型的结果文本，二者均来自 LLM 报文本身。

支持的协议路径：
  - anthropic-messages:  POST {baseUrl}/v1/messages
  - openai-completions:  POST {baseUrl}/chat/completions
  - openai-responses:    POST {baseUrl}/responses

输出：audit.jsonl 保存报文与 ChatML；tools.jsonl 单独保存 OMP 内置/MCP 工具轨迹。
工具结果包含工具名、参数、返回内容、调用/结果 exchange ID、来源与重复历史标记。
直接运行本脚本可启动代理和 OMP，使用临时扩展，不修改原模型配置。
路由可用 --route /local/=https://upstream.example/v1=openai-completions 自定义。

安全：
  - 只绑定 127.0.0.1。
  - 日志脱敏：Authorization / x-api-key / api-key / Cookie / Set-Cookie /
    Proxy-Authorization / x-goog-api-key 请求与响应头一律替换为 "<redacted>"；
    URL 查询参数 key= / api_key= / access_token= 同样脱敏。
    注意：这里只脱敏认证头和指定查询参数；正文中的密码、Flag、源码等原样记录。
  - 日志文件尽力 chmod 0600（Windows 上 os.chmod 只控制只读位，需配合 icacls，
    见交付说明）。

局限性（设计取舍，均已在此声明）：
  - 为简化 tee 逻辑，转发给上游时强制 Accept-Encoding: identity。
    语义影响：上游不再返回 gzip/br 压缩体；对 JSON/SSE 内容本身无影响。
  - 压缩请求体保留为 base64；审计结构化内容时应在客户端关闭请求压缩。
  - 请求/响应体超过上限时日志截断并置 truncated 标记，转发不受影响。
"""

from __future__ import annotations

import argparse
import base64
from collections import OrderedDict
import hashlib
import hmac
import http.client
import json
import logging
import logging.handlers
import os
import secrets
import shutil
import ssl
import socket
import sys
import subprocess
import tempfile
import threading
import time
import urllib.parse
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# --------------------------------------------------------------------------
# 配置
# --------------------------------------------------------------------------

# (路径前缀, 上游 base, 协议提示) —— 前缀最长匹配优先
# Public distribution never embeds a user's providers, model IDs or endpoints.
DEFAULT_ROUTES: list[tuple[str, str, str]] = []

HEALTH_PATH = "/__audit/health"
CONTROL_PATH = "/__audit/routes"
SUPPORTED_APIS = {"anthropic-messages", "openai-completions", "openai-responses"}

# Written only into a temporary local directory; never installed into OMP.
# Runtime registration changes this process's model registry, not models.yml.
LAUNCHER_EXTENSION = r'''
export default function (pi) {
  const root = process.env.OMP_AUDIT_CONTROL_URL;
  const token = process.env.OMP_AUDIT_CONTROL_TOKEN;
  const supported = new Set(["anthropic-messages", "openai-completions", "openai-responses"]);
  pi.on("session_start", async (_event, ctx) => {
    try {
      if (!root || !token) throw new Error("Missing private launcher connection");
      const grouped = new Map();
      for (const model of ctx.modelRegistry.getAll()) {
        const group = grouped.get(model.provider) || [];
        group.push(model);
        grouped.set(model.provider, group);
      }
      const entries = [];
      let skipped = 0;
      for (const [provider, models] of grouped) {
        const endpoints = new Set(models.map(m => JSON.stringify([m.api, m.baseUrl])));
        const model = models[0];
        if (endpoints.size !== 1 || !supported.has(model.api) || !model.baseUrl) {
          skipped++;
          continue;
        }
        // Parent-prepared child registries may already carry these overrides.
        if (model.baseUrl.startsWith(root + "/_audit/")) continue;
        entries.push({provider, api: model.api, upstream: model.baseUrl});
      }
      const response = await fetch(root + "/__audit/routes", {
        method: "POST",
        headers: {"Content-Type": "application/json", "Authorization": "Bearer " + token},
        body: JSON.stringify({routes: entries}),
        signal: AbortSignal.timeout(15000),
      });
      if (!response.ok) throw new Error("Audit route registration failed: " + response.status);
      const payload = await response.json();
      for (const route of payload.routes) {
        pi.registerProvider(route.provider, {baseUrl: route.baseUrl});
      }
      if (ctx.model) {
        const routed = ctx.modelRegistry.find(ctx.model.provider, ctx.model.id);
        if (!routed || !routed.baseUrl.startsWith(root + "/_audit/")) {
          throw new Error("The selected provider cannot be safely auto-routed; use an explicit route instead");
        }
        if (!(await pi.setModel(routed))) throw new Error("Unable to activate the routed model");
      }
      const ready = await fetch(root + "/__audit/routes", {
        method: "POST",
        headers: {"Content-Type": "application/json", "Authorization": "Bearer " + token},
        body: JSON.stringify({routes: [], ready: true}),
        signal: AbortSignal.timeout(15000),
      });
      if (!ready.ok) throw new Error("Audit readiness acknowledgement failed");
      process.stderr.write("[audit] temporary routing ready: " + payload.routes.length +
        " provider(s); " + skipped + " unsupported/mixed provider(s) unchanged.\n");
    } catch (error) {
      process.stderr.write("[audit] startup failed; stopping this OMP session: " + String(error) + "\n");
      ctx.shutdown();
      // Print-mode shutdown hooks can be no-ops. Do not continue un-audited.
      process.exit(1);
    }
  });
}
'''

# 脱敏头（小写比较）
SENSITIVE_HEADERS = {
    "authorization",
    "proxy-authorization",
    "x-api-key",
    "api-key",
    "apikey",
    "cookie",
    "set-cookie",
    "x-goog-api-key",
    "x-auth-token",
    "x-access-token",
}
# 脱敏查询参数
SENSITIVE_QUERY_KEYS = {"key", "api_key", "apikey", "access_token", "token"}

# 日志体大小上限（转发不受限，仅限制落盘）
MAX_REQUEST_BODY_LOG = 16 * 1024 * 1024   # 16 MiB
MAX_RESPONSE_BODY_LOG = 16 * 1024 * 1024  # 16 MiB
MAX_SSE_EVENTS = 100_000

HOP_BY_HOP = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "host",
    "content-length",
    "accept-encoding",
}

# --------------------------------------------------------------------------
# 日志：JSONL + 轮转
# --------------------------------------------------------------------------


class JsonlLogger:
    """线程安全的 JSONL 写入器，带大小轮转（手动实现，避免 RotatingFileHandler 的格式化层）。"""

    def __init__(self, path: str, max_bytes: int = 64 * 1024 * 1024, backups: int = 5):
        self.path = path
        self.max_bytes = max_bytes
        self.backups = backups
        self.lock = threading.Lock()
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self._fh = open(self.path, "a", encoding="utf-8")
        try:
            os.chmod(self.path, 0o600)  # POSIX 有效；Windows 上仅影响只读位
        except OSError:
            pass

    def _rotate(self) -> None:
        self._fh.close()
        for i in range(self.backups - 1, 0, -1):
            src, dst = f"{self.path}.{i}", f"{self.path}.{i + 1}"
            if os.path.exists(src):
                if i + 1 > self.backups:
                    os.remove(src)
                else:
                    os.replace(src, dst)
        if os.path.exists(self.path):
            os.replace(self.path, f"{self.path}.1")
        self._fh = open(self.path, "a", encoding="utf-8")
        try:
            os.chmod(self.path, 0o600)
        except OSError:
            pass

    def write(self, record: dict) -> None:
        line = json.dumps(record, ensure_ascii=False, default=str)
        with self.lock:
            if self._fh.closed:
                return
            try:
                if self._fh.tell() > self.max_bytes:
                    self._rotate()
                self._fh.write(line + "\n")
                self._fh.flush()
            except OSError as exc:  # 磁盘满等：不拖垮代理
                print(f"[audit] log write failed: {exc}", file=sys.stderr)

    def close(self) -> None:
        with self.lock:
            try:
                self._fh.close()
            except OSError:
                pass


# --------------------------------------------------------------------------
# 工具：脱敏 / 协议识别 / fallback hash / ChatML 派生
# --------------------------------------------------------------------------


def redact_headers(headers) -> dict:
    out = {}
    for k, v in headers.items() if isinstance(headers, dict) else headers:
        out[k] = "<redacted>" if k.lower() in SENSITIVE_HEADERS else v
    return out


def redact_query(path: str) -> str:
    if "?" not in path:
        return path
    base, qs = path.split("?", 1)
    pairs = urllib.parse.parse_qsl(qs, keep_blank_values=True)
    safe = [(k, "<redacted>" if k.lower() in SENSITIVE_QUERY_KEYS else v) for k, v in pairs]
    return base + "?" + urllib.parse.urlencode(
        safe, quote_via=lambda s, _safe, _enc, _err: urllib.parse.quote(str(s), safe="<>"))


def detect_protocol(route_hint: str | None, path: str) -> str:
    p = path.lower()
    if p.endswith("/v1/messages") or "/v1/messages" in p:
        return "anthropic-messages"
    if "/chat/completions" in p:
        return "openai-completions"
    if p.endswith("/responses") or "/responses?" in p:
        return "openai-responses"
    return route_hint or "unknown"


def _canon(obj) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def fallback_stream_id(protocol: str, body) -> str | None:
    """无上游 ID 时：system + 首条 user 消息的稳定 hash。支持 anthropic 与 openai 两种请求形。"""
    if not isinstance(body, dict):
        return None
    system = None
    first_user = None
    if protocol == "anthropic-messages":
        system = body.get("system")
        for m in body.get("messages") or []:
            if isinstance(m, dict) and m.get("role") == "user":
                first_user = m.get("content")
                break
    else:  # openai-completions / openai-responses / unknown 按 openai 形尝试
        msgs = body.get("messages") or body.get("input") or []
        if isinstance(msgs, str):
            first_user = msgs
            msgs = []
        sys_parts = [m.get("content") for m in msgs if isinstance(m, dict) and m.get("role") in ("system", "developer")]
        if body.get("instructions"):
            sys_parts.insert(0, body["instructions"])
        system = sys_parts if sys_parts else None
        for m in msgs:
            if isinstance(m, dict) and m.get("role") == "user":
                first_user = m.get("content")
                break
    if system is None and first_user is None:
        return None
    digest = hashlib.sha256(_canon({"system": system, "first_user": first_user}).encode("utf-8")).hexdigest()
    return f"sha256:{digest[:24]}"


def _text_of(content) -> str | None:
    """从 anthropic/openai 的 content（str 或 parts 列表）提取纯文本，用于 ChatML 视图。"""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for c in content:
            if isinstance(c, dict):
                if c.get("type") in ("text", "input_text", "output_text", "summary_text"):
                    parts.append(c.get("text", ""))
                elif c.get("type") == "thinking":
                    parts.append(f"[thinking] {c.get('thinking', '')}")
                elif c.get("type") == "tool_result":
                    parts.append(f"[tool_result {c.get('tool_use_id', '')}]")
                elif c.get("type") == "tool_use":
                    parts.append(f"[tool_use {c.get('name', '')}({c.get('id', '')})]")
                elif c.get("type") in ("image_url", "image", "base64"):
                    parts.append("[image]")
        return "".join(parts) if parts else None
    return None


def derive_chatml_request(protocol: str, body) -> list[dict]:
    """派生 ChatML 视图（有损，不替代原始格式）。"""
    msgs: list[dict] = []
    if not isinstance(body, dict):
        return msgs
    if protocol == "anthropic-messages":
        system = body.get("system")
        sys_text = _text_of(system)
        if sys_text:
            msgs.append({"role": "system", "content": sys_text})
        for m in body.get("messages") or []:
            if not isinstance(m, dict):
                continue
            entry = {"role": m.get("role"), "content": _text_of(m.get("content"))}
            # 保留工具结构（不单压成文本）
            if isinstance(m.get("content"), list):
                tu = [c for c in m["content"] if isinstance(c, dict) and c.get("type") == "tool_use"]
                tr = [c for c in m["content"] if isinstance(c, dict) and c.get("type") == "tool_result"]
                if tu:
                    entry["tool_calls"] = [
                        {"id": c.get("id"), "name": c.get("name"), "arguments": c.get("input")} for c in tu
                    ]
                if tr:
                    entry["tool_results"] = [
                        {"tool_use_id": c.get("tool_use_id"), "is_error": c.get("is_error", False),
                         "content": _text_of(c.get("content"))} for c in tr
                    ]
                    entry["role"] = "tool"
            msgs.append(entry)
    else:
        if body.get("instructions"):
            msgs.append({"role": "system", "content": body["instructions"]})
        items = body.get("messages") or body.get("input") or []
        if isinstance(items, str):
            msgs.append({"role": "user", "content": items})
            return msgs
        for m in items:
            if not isinstance(m, dict):
                continue
            if m.get("type") == "function_call":
                msgs.append({"role": "assistant", "content": None, "tool_calls": [{
                    "id": m.get("call_id"), "name": m.get("name"),
                    "arguments": _arguments(m.get("arguments"))}]})
                continue
            if m.get("type") == "function_call_output":
                msgs.append({"role": "tool", "tool_call_id": m.get("call_id"), "content": m.get("output")})
                continue
            entry = {"role": m.get("role"), "content": _text_of(m.get("content"))}
            if m.get("tool_calls"):
                entry["tool_calls"] = m["tool_calls"]
            if m.get("role") == "tool":
                entry["tool_call_id"] = m.get("tool_call_id")
            msgs.append(entry)
    return msgs


def _arguments(value):
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (ValueError, TypeError):
            pass
    return value


def tool_kind_hint(name) -> str:
    """Names are hints, not proof of the executing implementation."""
    if isinstance(name, str) and name.startswith("mcp__"):
        return "mcp"
    if name in {"bash", "read", "write", "edit", "grep", "find", "glob",
                "ls", "python", "task", "todo", "lsp", "fetch", "web_search",
                "goal", "vibe", "eval", "resolve", "wait", "yield"}:
        return "omp_builtin"
    return "custom_or_unknown"


def extract_request_tool_calls(protocol: str, body) -> list[dict]:
    """Read OMP/MCP calls already in history, including after proxy restart."""
    if not isinstance(body, dict):
        return []
    key = "input" if protocol == "openai-responses" else "messages"
    messages = body.get(key)
    if not isinstance(messages, list):
        return []
    calls = []
    for i, msg in enumerate(messages):
        if not isinstance(msg, dict):
            continue
        if msg.get("type") == "function_call":
            calls.append({"id": msg.get("call_id"), "name": msg.get("name"),
                          "arguments": _arguments(msg.get("arguments")),
                          "position": i, "source": f"request.{key}[{i}]"})
        if msg.get("role") != "assistant":
            continue
        if protocol == "anthropic-messages":
            items = msg.get("content")
            if not isinstance(items, list):
                continue
            for j, item in enumerate(items):
                if isinstance(item, dict) and item.get("type") == "tool_use":
                    calls.append({"id": item.get("id"), "name": item.get("name"),
                                  "arguments": item.get("input"), "position": i,
                                  "source": f"request.{key}[{i}].content[{j}]"})
        else:
            for j, item in enumerate(msg.get("tool_calls") or []):
                if not isinstance(item, dict):
                    continue
                fn = item.get("function") or {}
                calls.append({"id": item.get("id"), "name": fn.get("name"),
                              "arguments": _arguments(fn.get("arguments")), "position": i,
                              "source": f"request.{key}[{i}].tool_calls[{j}]"})
    return calls


def extract_request_tool_results(protocol: str, body) -> list[dict]:
    """Extract tool results echoed in a later model request."""
    if not isinstance(body, dict):
        return []
    results: list[dict] = []
    if protocol == "anthropic-messages":
        messages = body.get("messages") or []
        for message_index, message in enumerate(messages):
            if not isinstance(message, dict):
                continue
            content = message.get("content")
            if not isinstance(content, list):
                continue
            for item_index, item in enumerate(content):
                if isinstance(item, dict) and item.get("type") == "tool_result":
                    results.append({
                        "tool_call_id": item.get("tool_use_id"),
                        "content": item.get("content"),
                        "is_error": bool(item.get("is_error", False)),
                        "source": f"request.messages[{message_index}].content[{item_index}]",
                        "position": message_index,
                    })
    else:
        messages = body.get("messages")
        if isinstance(messages, list):
            for index, message in enumerate(messages):
                if isinstance(message, dict) and message.get("role") == "tool":
                    results.append({
                        "tool_call_id": message.get("tool_call_id"),
                        "name": message.get("name"),
                        "content": message.get("content"),
                        "is_error": message.get("is_error"),
                        "source": f"request.messages[{index}]",
                        "position": index,
                    })
        items = body.get("input")
        if isinstance(items, list):
            for index, item in enumerate(items):
                if isinstance(item, dict) and item.get("type") == "function_call_output":
                    results.append({
                        "tool_call_id": item.get("call_id"),
                        "content": item.get("output"),
                        "is_error": item.get("is_error"),
                        "source": f"request.input[{index}]",
                        "position": index,
                    })
    return results


class ToolTraceTracker:
    """Bounded correlation cache; context history remains the source of truth."""

    def __init__(self, max_entries: int = 4096):
        self._lock = threading.Lock()
        self.max_entries = max_entries
        self._calls = OrderedDict()
        self._seen_results = OrderedDict()

    @staticmethod
    def _signature(call) -> str:
        return hashlib.sha256(_canon([call.get("name"), _arguments(call.get("arguments"))]).encode()).hexdigest()

    def _trim(self, cache):
        while len(cache) > self.max_entries:
            cache.popitem(last=False)

    def observe_request(self, exchange_id: str, protocol: str, body, scope: str) -> list[dict]:
        calls = extract_request_tool_calls(protocol, body)
        # Historical calls are explicitly labelled; they are not new executions.
        events = [{"event": "tool_call_context", "tool_call_id": call.get("id"),
                   "tool_name": call.get("name"), "arguments": call.get("arguments"),
                   "tool_kind_hint": tool_kind_hint(call.get("name")),
                   "context_exchange_id": exchange_id, "source": call["source"]} for call in calls]
        events.extend(self.observe_results(exchange_id, extract_request_tool_results(protocol, body),
                                          scope, calls, (body or {}).get("previous_response_id")
                                          if isinstance(body, dict) else None))
        return events

    def observe_results(self, exchange_id: str, results: list[dict], scope: str | None = None,
                        context_calls=None, previous_response_id=None) -> list[dict]:
        events = []
        with self._lock:
            for result in results:
                call_id = result.get("tool_call_id")
                history = [c for c in (context_calls or []) if call_id and c.get("id") == call_id
                           and c.get("position", -1) < result.get("position", float("inf"))]
                context = history[-1] if history else None
                candidates = [c for (s, cid, _), c in self._calls.items()
                              if call_id and s == scope and cid == call_id]
                if context is not None:
                    candidates = [c for c in candidates if self._signature(c) == self._signature(context)]
                elif previous_response_id:
                    candidates = [c for c in candidates if c.get("response_id") == previous_response_id]
                ambiguous = len(candidates) > 1
                observed = candidates[0] if len(candidates) == 1 else None
                call = context or observed or {}
                fingerprint = hashlib.sha256(_canon([scope, call_id, self._signature(call),
                                                     result.get("content"), result.get("is_error")]).encode()).hexdigest()
                first = self._seen_results.get(fingerprint)
                if first is None:
                    self._seen_results[fingerprint] = exchange_id
                    self._trim(self._seen_results)
                else:
                    self._seen_results.move_to_end(fingerprint)
                events.append({
                    "event": "tool_result",
                    "tool_call_id": call_id,
                    "tool_name": call.get("name") or result.get("name"),
                    "tool_kind_hint": tool_kind_hint(call.get("name") or result.get("name")),
                    "arguments": _arguments(call.get("arguments")),
                    "call_exchange_id": (observed or {}).get("exchange_id"),
                    "call_response_id": (observed or {}).get("response_id"),
                    "result_exchange_id": exchange_id,
                    "correlated": bool(context or observed),
                    "correlation_source": "request_context" if context else ("response_cache" if observed else "unmatched"),
                    "call_exchange_ambiguous": ambiguous,
                    "replayed_context": first is not None,
                    "first_result_exchange_id": first or exchange_id,
                    "is_error": result.get("is_error"),
                    "content": result.get("content"),
                    "source": result.get("source"),
                })
        return events

    def observe_calls(self, exchange_id: str, calls: list[dict], scope: str | None = None, response_id=None) -> list[dict]:
        events = []
        with self._lock:
            for call in calls:
                call_id = call.get("id")
                event = {
                    "event": "tool_call",
                    "tool_call_id": call_id,
                    "tool_name": call.get("name"),
                    "tool_kind_hint": tool_kind_hint(call.get("name")),
                    "call_exchange_id": exchange_id,
                    "call_response_id": response_id,
                    "arguments": _arguments(call.get("arguments")),
                    "source": "response",
                    "correlated": False,
                }
                if call_id:
                    key = (scope, call_id, exchange_id)
                    self._calls[key] = {**call, "exchange_id": exchange_id, "response_id": response_id}
                    self._calls.move_to_end(key)
                    self._trim(self._calls)
                events.append(event)
        return events


TOOL_TRACE = ToolTraceTracker()


# --------------------------------------------------------------------------
# SSE 增量解析 + ID 提取 + 助手消息组装
# --------------------------------------------------------------------------


class SseTap:
    """逐字节 tee 的 SSE 解析器：转发不受影响，仅旁路解析。"""

    def __init__(self, protocol: str):
        self.protocol = protocol
        self.buf = b""
        self.cur_event: str | None = None
        self.cur_data: list[str] = []
        self.events: list[dict] = []
        self.event_bytes = 0
        self.truncated = False
        self.ids: list[dict] = []  # {id, source, confidence}
        self._seen_ids: set[str] = set()
        # 助手消息组装（ChatML 视图用）
        self.thinking = ""
        self.text = ""
        self.tool_calls: list[dict] = []  # {id, name, arguments(json str)}
        self._open_tool: dict[int, dict] = {}  # anthropic index -> partial
        self._response_item_slots: dict[str, dict] = {}  # Responses item_id -> call slot
        self.stop_reason = None
        self.usage = None

    def _add_id(self, rid: str, source: str, confidence: str) -> None:
        key = f"{source}:{rid}"
        if rid and key not in self._seen_ids:
            self._seen_ids.add(key)
            self.ids.append({"id": rid, "source": source, "confidence": confidence})

    def feed(self, chunk: bytes) -> None:
        self.buf += chunk
        while b"\n" in self.buf:
            line, self.buf = self.buf.split(b"\n", 1)
            self._line(line.decode("utf-8", "replace").rstrip("\r"))

    def flush(self) -> None:
        if self.buf:
            self._line(self.buf.decode("utf-8", "replace"))
            self.buf = b""
        self._dispatch()

    def _line(self, line: str) -> None:
        if line == "":
            self._dispatch()
            return
        if line.startswith(":"):
            return  # 注释/keepalive
        if line.startswith("event:"):
            self.cur_event = line[6:].strip()
        elif line.startswith("data:"):
            self.cur_data.append(line[5:].lstrip(" "))
        # id:/retry: 忽略（不影响语义）

    def _dispatch(self) -> None:
        if self.cur_event is None and not self.cur_data:
            return
        data = "\n".join(self.cur_data)
        event = self.cur_event
        self.cur_event, self.cur_data = None, []
        if data == "[DONE]":
            self._store(event, {"done": True})
            return
        obj = None
        try:
            obj = json.loads(data)
        except (ValueError, TypeError):
            pass
        self._store(event, obj if obj is not None else {"raw_data": data})
        if not isinstance(obj, dict):
            return
        self._extract(event, obj)

    def _store(self, event, obj) -> None:
        if self.truncated:
            return
        size = len(json.dumps(obj, default=str))
        if len(self.events) >= MAX_SSE_EVENTS or self.event_bytes + size > MAX_RESPONSE_BODY_LOG:
            self.truncated = True
            return
        self.events.append({"event": event, "data": obj})
        self.event_bytes += size

    def _extract(self, event: str | None, obj: dict) -> None:
        """按协议提取 ID / thinking / 文本 / 工具调用。"""
        if self.protocol == "anthropic-messages":
            if event == "message_start" and isinstance(obj.get("message"), dict):
                msg = obj["message"]
                self._add_id(msg.get("id"), "body:message_start.message.id", "high")
                if msg.get("usage"):
                    self.usage = msg["usage"]
            elif event == "content_block_start":
                cb = obj.get("content_block") or {}
                if cb.get("type") == "tool_use":
                    self._open_tool[obj.get("index", 0)] = {
                        "id": cb.get("id"), "name": cb.get("name"), "arguments": "",
                        "initial_input": cb.get("input"),
                    }
                elif cb.get("type") == "text":
                    self.text += cb.get("text", "")
                elif cb.get("type") == "thinking":
                    self.thinking += cb.get("thinking", "")
            elif event == "content_block_delta":
                d = obj.get("delta") or {}
                t = d.get("type")
                if t == "thinking_delta":
                    self.thinking += d.get("thinking", "")
                elif t == "text_delta":
                    self.text += d.get("text", "")
                elif t == "input_json_delta":
                    slot = self._open_tool.setdefault(obj.get("index", 0), {"id": None, "name": None, "arguments": ""})
                    slot["arguments"] += d.get("partial_json", "")
            elif event == "message_delta":
                if isinstance(obj.get("usage"), dict):
                    self.usage = {**(self.usage or {}), **obj["usage"]}
                self.stop_reason = (obj.get("delta") or {}).get("stop_reason")
        elif self.protocol == "openai-completions":
            if obj.get("id"):
                self._add_id(obj["id"], "body:chunk.id", "high")
            for ch in obj.get("choices") or []:
                d = ch.get("delta") or {}
                msg = ch.get("message") or {}
                src = d if d else msg
                if src.get("reasoning_content"):
                    self.thinking += src["reasoning_content"]
                if src.get("reasoning") and isinstance(src["reasoning"], str):
                    self.thinking += src["reasoning"]
                if src.get("content"):
                    self.text += src["content"]
                for tc in src.get("tool_calls") or []:
                    idx = tc.get("index", 0)
                    while len(self.tool_calls) <= idx:
                        self.tool_calls.append({"id": None, "name": None, "arguments": ""})
                    slot = self.tool_calls[idx]
                    if tc.get("id"):
                        slot["id"] = tc["id"]
                    fn = tc.get("function") or {}
                    if fn.get("name"):
                        slot["name"] = fn["name"]
                    if fn.get("arguments"):
                        slot["arguments"] += fn["arguments"]
                if ch.get("finish_reason"):
                    self.stop_reason = ch["finish_reason"]
            if obj.get("usage"):
                self.usage = obj["usage"]
        elif self.protocol == "openai-responses":
            et = obj.get("type") or event or ""
            if et == "response.created" and isinstance(obj.get("response"), dict):
                self._add_id(obj["response"].get("id"), "body:response.created.response.id", "high")
            elif et in {"response.output_item.added", "response.output_item.done"}:
                item = obj.get("item") or {}
                if item.get("type") == "function_call":
                    call_id = item.get("call_id")
                    item_id = item.get("id")
                    slot = (self._response_item_slots.get(item_id) if item_id else None)
                    slot = slot or next((t for t in self.tool_calls if t.get("id") == call_id), None)
                    if slot is None:
                        slot = {"id": call_id, "name": item.get("name"), "arguments": ""}
                        self.tool_calls.append(slot)
                    else:
                        if call_id:
                            slot["id"] = call_id
                        slot["name"] = item.get("name") or slot.get("name")
                    if item_id:
                        self._response_item_slots[item_id] = slot
                    if item.get("arguments"):
                        slot["arguments"] = item["arguments"]
            elif et.endswith("reasoning_text.delta") or et.endswith("reasoning_summary_text.delta"):
                self.thinking += obj.get("delta", "")
            elif et == "response.output_text.delta":
                self.text += obj.get("delta", "")
            elif et == "response.function_call_arguments.delta":
                item = obj.get("item_id")
                slot = self._response_item_slots.get(item)
                slot = slot or next((t for t in self.tool_calls if t.get("id") == item), None)
                if slot is None:
                    slot = {"id": item, "name": None, "arguments": ""}
                    self.tool_calls.append(slot)
                if item:
                    self._response_item_slots[item] = slot
                slot["arguments"] += obj.get("delta", "")
            elif et == "response.function_call_arguments.done":
                slot = self._response_item_slots.get(obj.get("item_id"))
                if slot is not None:
                    slot["arguments"] = obj.get("arguments", slot["arguments"])
                    slot["name"] = obj.get("name") or slot.get("name")
            elif et == "response.completed" and isinstance(obj.get("response"), dict):
                r = obj["response"]
                for item in r.get("output") or []:
                    self._extract("response.output_item.done", {"type": "response.output_item.done", "item": item})
                self._add_id(r.get("id"), "body:response.completed.response.id", "high")
                self.stop_reason = r.get("status")
                self.usage = r.get("usage")
        else:
            # 未知协议：尽力找顶层 id
            if isinstance(obj.get("id"), str):
                self._add_id(obj["id"], "body:chunk.id", "medium")

    def chatml_assistant(self) -> dict:
        """流结束后组装助手消息（派生视图）。"""
        # Keep assembly idempotent: callers may need both the call list and the
        # complete ChatML message, and assembling must not mutate parser state.
        tool_calls = [*self.tool_calls, *self._open_tool.values()]
        out: dict = {"role": "assistant"}
        if self.thinking:
            out["thinking"] = self.thinking
        out["content"] = self.text or None
        if tool_calls:
            calls = []
            for tc in tool_calls:
                args = _arguments(tc.get("arguments") or tc.get("initial_input"))
                calls.append({"id": tc.get("id"), "name": tc.get("name"), "arguments": args})
            out["tool_calls"] = calls
        if self.stop_reason:
            out["stop_reason"] = self.stop_reason
        return out


# --------------------------------------------------------------------------
# HTTP 代理核心
# --------------------------------------------------------------------------


class Route:
    def __init__(self, prefix: str, upstream: str, hint: str):
        self.prefix = prefix
        self.upstream = upstream.rstrip("/")
        self.hint = hint
        u = urllib.parse.urlsplit(self.upstream)
        self.scheme = u.scheme
        self.host = u.hostname
        self.port = u.port or (443 if u.scheme == "https" else 80)
        self.base_path = u.path


def match_route(routes: list[Route], path: str) -> Route | None:
    best = None
    for r in routes:
        if path.startswith(r.prefix) and (best is None or len(r.prefix) > len(best.prefix)):
            best = r
    return best


class AuditHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "omp-audit/1.0"
    timeout = 86400  # 客户端连接读超时（长 SSE 用）

    # 由 main 注入
    routes: list[Route] = []
    logger: JsonlLogger | None = None
    tool_logger: JsonlLogger | None = None
    control_token: str | None = None
    route_lock = threading.Lock()
    upstream_connect_timeout = 30.0
    verbose = False

    def log_message(self, fmt, *args):  # 静音默认访问日志（JSONL 才是审计面）
        if self.verbose:
            super().log_message(fmt, *args)

    # -- 请求体读取（支持 Content-Length 与 chunked） --
    def _read_body(self) -> bytes:
        te = self.headers.get("Transfer-Encoding", "").lower()
        if "chunked" in te:
            out = bytearray()
            while True:
                size_line = self.rfile.readline(65536).strip()
                try:
                    size = int(size_line.split(b";")[0], 16)
                except ValueError:
                    break
                if size == 0:
                    # 读掉 trailer 直到空行
                    while True:
                        t = self.rfile.readline(65536)
                        if t in (b"\r\n", b"\n", b""):
                            break
                    break
                out += self.rfile.read(size)
                self.rfile.read(2)  # CRLF
            return bytes(out)
        length = self.headers.get("Content-Length")
        if length:
            return self.rfile.read(int(length))
        return b""

    def _handle(self) -> None:
        if self.path == CONTROL_PATH:
            self._register_routes()
            return
        record: dict = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()),
            "ts_epoch": time.time(),
            "exchange_id": uuid.uuid4().hex[:12],
        }
        started = time.time()
        self._response_started = False
        try:
            self._proxy(record)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            record["client_disconnect"] = True
        except socket.timeout:
            record["error"] = "socket_timeout"
        except Exception as exc:  # 代理自身异常不能拖垮服务
            record["error"] = f"{type(exc).__name__}: {exc}"
            try:
                if not self.wfile.closed and not self._response_started:
                    body = json.dumps({"error": "proxy_internal", "detail": str(exc)}).encode()
                    self.send_response(502)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
            except OSError:
                pass
            self.close_connection = True
        finally:
            record["duration_ms"] = round((time.time() - started) * 1000)
            if self.logger:
                self.logger.write(record)
            if self.tool_logger:
                grouping = record.get("classification") or {}
                for event in record.get("tool_trace", []):
                    self.tool_logger.write({"ts": record["ts"], "exchange_id": record["exchange_id"],
                                            "route": record.get("route"), "protocol": record.get("protocol"),
                                            "classification_id": grouping.get("primary_id"),
                                            "conversation_hint": record.get("conversation_hint"), **event})

    def _control_reply(self, status: int, payload: dict) -> None:
        data = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(data)
        self.close_connection = True

    def _register_routes(self) -> None:
        # Control traffic has no model content and must never log its token.
        if self.command != "POST" or not self.control_token:
            self._control_reply(404, {"error": "not_found"})
            return
        provided = self.headers.get("Authorization", "").encode()
        if not hmac.compare_digest(provided, ("Bearer " + self.control_token).encode()):
            self._control_reply(403, {"error": "forbidden"})
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if self.headers.get("Transfer-Encoding") or not 0 < length <= 2 * 1024 * 1024:
                raise ValueError("Invalid control request length")
            body = json.loads(self.rfile.read(length))
            entries = body.get("routes")
            if not isinstance(entries, list) or len(entries) > 2048:
                raise ValueError("Invalid route list")
            routes, response = [], []
            providers = set()
            for entry in entries:
                if not isinstance(entry, dict):
                    raise ValueError("Invalid route entry")
                provider, api, upstream = (entry.get(k) for k in ("provider", "api", "upstream"))
                if not isinstance(provider, str) or not provider or provider in providers:
                    raise ValueError("Missing or duplicate provider")
                if api not in SUPPORTED_APIS or not isinstance(upstream, str):
                    raise ValueError("Unsupported route protocol")
                # Mirror OMP's normalizeAnthropicBaseUrl before replacing the
                # model URL: its client appends /v1/messages to the local URL.
                # This is launcher-only; explicit --route paths stay literal.
                upstream = upstream.strip().rstrip("/")
                if api == "anthropic-messages" and upstream.endswith("/v1"):
                    upstream = upstream[:-3]
                parsed = urllib.parse.urlsplit(upstream)
                if (parsed.scheme not in ("http", "https") or not parsed.hostname
                        or parsed.username or parsed.password or parsed.query or parsed.fragment):
                    raise ValueError("Upstream must be an HTTP(S) base URL without credentials, query or fragment")
                if parsed.hostname in ("localhost", "127.0.0.1", "::1") and parsed.port == self.server.server_port:
                    raise ValueError("Refusing a proxy routing loop")
                providers.add(provider)
                key = hashlib.sha256(_canon([provider, api, upstream]).encode()).hexdigest()[:20]
                prefix = "/_audit/" + key + "/"
                routes.append(Route(prefix, upstream, api))
                response.append({"provider": provider,
                                 "baseUrl": f"http://127.0.0.1:{self.server.server_port}" + prefix.rstrip("/")})
            with self.route_lock:
                existing = {route.prefix for route in self.routes}
                self.routes.extend(route for route in routes if route.prefix not in existing)
            if body.get("ready") is True and hasattr(self.server, "audit_ready"):
                self.server.audit_ready.set()
            self._control_reply(200, {"routes": response})
        except (ValueError, TypeError, AttributeError):
            self._control_reply(400, {"error": "invalid_routes"})

    def _proxy(self, record: dict) -> None:
        raw_path = self.path
        if raw_path.startswith(HEALTH_PATH):
            body = b'{"ok":true}'
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            record["health"] = True
            return

        route = match_route(self.routes, raw_path)
        if route is None:
            body = json.dumps({"error": "no_route", "path": raw_path}).encode()
            self.send_response(404)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            record["error"] = "no_route"
            record["path"] = redact_query(raw_path)
            return

        protocol = detect_protocol(route.hint, raw_path)
        body = self._read_body()
        body_json = None
        try:
            body_json = json.loads(body) if body else None
        except (ValueError, TypeError):
            pass
        conversation_key = fallback_stream_id(protocol, body_json) or "unkeyed"
        # Prompt hashes are grouping hints, not reliable unique session IDs.
        # Correlate by call identity + context; ambiguous origins stay unknown.
        trace_scope = route.prefix
        record["conversation_hint"] = conversation_key
        record["tool_trace"] = TOOL_TRACE.observe_request(record["exchange_id"], protocol, body_json, trace_scope)
        record["tool_catalog"] = []
        for definition in (body_json.get("tools") or []) if isinstance(body_json, dict) else []:
            if isinstance(definition, dict):
                definition = definition.get("function") or definition
                name = definition.get("name")
                if name:
                    record["tool_catalog"].append({"name": name, "tool_kind_hint": tool_kind_hint(name)})

        # 上游路径 = 上游 base path + 去掉前缀的请求路径
        sub_path = raw_path[len(route.prefix):]
        upstream_path = route.base_path + "/" + sub_path if not sub_path.startswith("/") else route.base_path + sub_path
        if not upstream_path.startswith("/"):
            upstream_path = "/" + upstream_path

        record.update({
            "route": route.prefix,
            "protocol": protocol,
            "method": self.command,
            "path": redact_query(raw_path),
            "upstream": f"{route.scheme}://{route.host}:{route.port}{(upstream_path if '?' not in upstream_path else upstream_path.split('?')[0])}",
            "request": {
                "headers": redact_headers(self.headers.items()),
                "body_bytes": len(body),
            },
        })
        if body_json is not None:
            if len(body) <= MAX_REQUEST_BODY_LOG:
                record["request"]["body"] = body_json
            else:
                record["request"]["body_truncated"] = True
        elif body:
            enc = self.headers.get("Content-Encoding", "")
            record["request"]["body_raw_b64"] = base64.b64encode(body[:MAX_REQUEST_BODY_LOG]).decode()
            record["request"]["body_unparsed"] = True
            if enc:
                record["request"]["content_encoding"] = enc

        # ---- 转发 ----
        headers = {}
        for k, v in self.headers.items():
            lk = k.lower()
            if lk in HOP_BY_HOP:
                continue
            headers[k] = v
        headers["Host"] = route.host if route.port in (80, 443) else f"{route.host}:{route.port}"
        headers["Accept-Encoding"] = "identity"  # 设计取舍，见模块 docstring
        headers["Content-Length"] = str(len(body))

        if route.scheme == "https":
            ctx = ssl.create_default_context()
            conn: http.client.HTTPConnection = http.client.HTTPSConnection(
                route.host, route.port, timeout=self.upstream_connect_timeout, context=ctx
            )
        else:
            conn = http.client.HTTPConnection(route.host, route.port, timeout=self.upstream_connect_timeout)

        conn.request(self.command, upstream_path, body=body, headers=headers)
        try:
            resp = conn.getresponse()
        except socket.timeout:
            record["error"] = "upstream_connect_timeout"
            conn.close()
            raise
        # 响应开始后取消读超时（SSE 长流依赖 omp 端自己的 watchdog/abort）
        # 无 Content-Length 的 close-delimited 响应会使 http.client 提前置空 sock
        if conn.sock is not None:
            conn.sock.settimeout(None)

        record["response"] = {
            "status": resp.status,
            "headers": redact_headers(resp.getheaders()),
        }
        # 响应头里的 request-id 类 ID
        header_ids = []
        for name in ("request-id", "x-request-id", "anthropic-request-id", "openai-request-id", "x-completion-id"):
            v = resp.getheader(name)
            if v:
                header_ids.append({"id": v, "source": f"header:{name}", "confidence": "high"})

        # 回写响应头给客户端
        self._response_started = True
        self.send_response(resp.status)
        content_type = resp.getheader("Content-Type", "")
        upstream_len = resp.getheader("Content-Length")
        is_sse = "text/event-stream" in content_type
        for k, v in resp.getheaders():
            lk = k.lower()
            if lk in HOP_BY_HOP or lk == "content-length":
                continue
            self.send_header(k, v)
        use_chunked = is_sse or upstream_len is None
        if use_chunked:
            self.send_header("Transfer-Encoding", "chunked")
        else:
            self.send_header("Content-Length", upstream_len)
        self.end_headers()

        tap = SseTap(protocol)
        resp_bytes = 0
        resp_buf = bytearray()  # 非 SSE 时保留原文
        client_gone = False
        stream_error = None
        response_over_limit = False
        try:
            while True:
                chunk = resp.read1(65536) if hasattr(resp, "read1") else resp.read(65536)
                if not chunk:
                    break
                resp_bytes += len(chunk)
                if is_sse:
                    tap.feed(chunk)
                    # Register before forwarding the chunk: the client may
                    # immediately execute a tool and start its next request.
                    TOOL_TRACE.observe_calls(record["exchange_id"], tap.chatml_assistant().get("tool_calls", []),
                                             trace_scope, tap.ids[0]["id"] if tap.ids else None)
                else:
                    remaining = max(0, MAX_RESPONSE_BODY_LOG - len(resp_buf))
                    resp_buf += chunk[:remaining]
                    response_over_limit |= len(chunk) > remaining
                try:
                    if use_chunked:
                        self.wfile.write(f"{len(chunk):x}\r\n".encode() + chunk + b"\r\n")
                    else:
                        self.wfile.write(chunk)
                    self.wfile.flush()
                except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
                    client_gone = True
                    break
        except (OSError, http.client.HTTPException) as exc:
            stream_error = type(exc).__name__
            self.close_connection = True
        finally:
            if use_chunked and not client_gone and not stream_error:
                try:
                    self.wfile.write(b"0\r\n\r\n")
                    self.wfile.flush()
                except OSError:
                    client_gone = True
            conn.close()

        tap.flush()
        record["response"]["body_bytes"] = resp_bytes
        record["response"]["sse"] = is_sse
        if stream_error:
            record["error"] = "upstream_stream_error"
            record["response_truncated_by"] = stream_error
        if response_over_limit:
            record["response"]["body_truncated"] = True
        if client_gone:
            record["client_disconnect"] = True
            record["response_truncated_by"] = "client_disconnect"

        # ---- ID 分类 ----
        ids = list(tap.ids)
        if not is_sse and resp_buf:
            try:
                obj = json.loads(bytes(resp_buf))
                record["response"]["body"] = obj
                rid = obj.get("id") or (obj.get("message") or {}).get("id") if isinstance(obj, dict) else None
                if rid:
                    ids.insert(0, {"id": rid, "source": "body:json.id", "confidence": "high"})
                # 非流式 anthropic/openai 也组装 chatml
                if isinstance(obj, dict):
                    self._chatml_from_json(protocol, obj, tap)
            except (ValueError, TypeError):
                record["response"]["body_raw_b64"] = base64.b64encode(bytes(resp_buf[:MAX_RESPONSE_BODY_LOG])).decode()
        if is_sse:
            record["response"]["sse_events"] = tap.events
            record["response"]["sse_event_count"] = len(tap.events)
            if tap.truncated:
                record["response"]["sse_log_truncated"] = True
            if tap.usage:
                record["response"]["usage"] = tap.usage

        assistant_message = tap.chatml_assistant()
        response_tool_calls = assistant_message.get("tool_calls", [])
        record["tool_trace"].extend(
            TOOL_TRACE.observe_calls(record["exchange_id"], response_tool_calls, trace_scope,
                                     ids[0]["id"] if ids else None)
        )

        ids.extend(header_ids)
        fallback = fallback_stream_id(protocol, body_json)
        if fallback:
            ids.append({"id": fallback, "source": "fallback:system+first_user.sha256", "confidence": "low"})
        primary = next((i for i in ids if i["confidence"] == "high"), None) or (ids[0] if ids else None)
        record["classification"] = {
            "ids": ids,
            "primary_id": primary["id"] if primary else None,
            "primary_source": primary["source"] if primary else None,
            "confidence": primary["confidence"] if primary else "none",
        }

        # ---- ChatML 派生视图（有损，不替代原始格式） ----
        chatml = derive_chatml_request(protocol, body_json)
        if is_sse or record["response"].get("body"):
            chatml.append(assistant_message)
        record["chatml"] = {"derived": True, "lossy": True, "messages": chatml}

    def _chatml_from_json(self, protocol: str, obj: dict, tap: SseTap) -> None:
        """非流式 JSON 响应 -> 填充 tap 以复用 chatml_assistant。"""
        if protocol == "anthropic-messages":
            for block in obj.get("content") or []:
                if block.get("type") == "thinking":
                    tap.thinking += block.get("thinking", "")
                elif block.get("type") == "text":
                    tap.text += block.get("text", "")
                elif block.get("type") == "tool_use":
                    tap.tool_calls.append({"id": block.get("id"), "name": block.get("name"),
                                           "arguments": json.dumps(block.get("input"), ensure_ascii=False)})
            tap.stop_reason = obj.get("stop_reason")
            tap.usage = obj.get("usage")
        elif protocol == "openai-responses":
            for item in obj.get("output") or []:
                if not isinstance(item, dict):
                    continue
                if item.get("type") == "function_call":
                    tap._extract("response.output_item.done", {"type": "response.output_item.done", "item": item})
                elif item.get("type") == "message":
                    tap.text += _text_of(item.get("content")) or ""
                elif item.get("type") == "reasoning":
                    tap.thinking += _text_of(item.get("summary")) or ""
            tap.stop_reason = obj.get("status")
            tap.usage = obj.get("usage")
        else:
            for ch in obj.get("choices") or []:
                m = ch.get("message") or {}
                if m.get("reasoning_content"):
                    tap.thinking += m["reasoning_content"]
                if m.get("content"):
                    tap.text += m["content"]
                for tc in m.get("tool_calls") or []:
                    fn = tc.get("function") or {}
                    tap.tool_calls.append({"id": tc.get("id"), "name": fn.get("name"),
                                           "arguments": fn.get("arguments", "")})
                tap.stop_reason = ch.get("finish_reason")
            tap.usage = obj.get("usage")

    do_POST = _handle
    do_GET = _handle
    do_PUT = _handle
    do_DELETE = _handle


class AuditHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def handle_error(self, request, client_address):
        # 客户端 keep-alive 断开/中途 RST 是常态（omp abort 流即如此），不打栈
        exc = sys.exc_info()[1]
        if isinstance(exc, (ConnectionResetError, ConnectionAbortedError, BrokenPipeError)):
            return
        super().handle_error(request, client_address)



# --------------------------------------------------------------------------
# main
# --------------------------------------------------------------------------


def launch_omp(server, handler, executable: str, omp_args: list[str]) -> int:
    """Run OMP with a disposable extension and child-only environment changes."""
    with tempfile.TemporaryDirectory(prefix="omp-audit-launch-") as directory:
        extension = os.path.join(directory, "audit-route.ts")
        with open(extension, "w", encoding="utf-8") as fh:
            fh.write(LAUNCHER_EXTENSION)
        try:
            os.chmod(extension, 0o600)
        except OSError:
            pass
        environment = os.environ.copy()
        environment["OMP_AUDIT_CONTROL_URL"] = f"http://127.0.0.1:{server.server_port}"
        environment["OMP_AUDIT_CONTROL_TOKEN"] = handler.control_token
        # Only this child gets the compression setting; the user's environment
        # and other OMP processes remain untouched.
        environment["PI_CODEX_ZSTD"] = "0"
        server.audit_ready = threading.Event()
        worker = threading.Thread(target=server.serve_forever, daemon=True)
        worker.start()
        child = None
        try:
            # Inherit the terminal: this remains an ordinary interactive OMP.
            child = subprocess.Popen([executable, "--extension", extension, *omp_args], env=environment)
            while True:
                try:
                    status = child.wait()
                    break
                except KeyboardInterrupt:
                    # Ctrl+C also reaches OMP, where it may mean "cancel turn".
                    # Keep the proxy alive until OMP actually exits.
                    continue
            if not server.audit_ready.is_set():
                print("[audit] OMP exited before temporary routing was ready; original configuration was not changed.", file=sys.stderr)
                return status or 1
            return status
        finally:
            if child is not None and child.poll() is None:
                child.terminate()
                try:
                    child.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait()
            server.shutdown()
            worker.join(timeout=5)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="OMP 本地模型审计代理（127.0.0.1 only）")
    ap.add_argument("--host", default="127.0.0.1", help="绑定地址（默认 127.0.0.1；不要改成 0.0.0.0）")
    ap.add_argument("--port", type=int, help="代理模式默认 8787；一键启动默认自动选择空闲端口")
    ap.add_argument("--log", help="审计 JSONL；一键启动默认按次分目录")
    ap.add_argument("--log-max-bytes", type=int, default=64 * 1024 * 1024)
    ap.add_argument("--log-backups", type=int, default=5)
    ap.add_argument("--tools-log", help="独立工具轨迹 JSONL，默认与 --log 同目录下的 tools.jsonl")
    ap.add_argument("--route", action="append", default=[],
                    help="上游路由 prefix=upstream 或 prefix=upstream=hint，可重复；仅代理模式至少配置一个")
    ap.add_argument("--upstream-connect-timeout", type=float, default=30.0)
    ap.add_argument("--verbose", action="store_true")
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--launch-omp", action="store_true", help="启动 OMP 并自动临时路由；没有 --route 时默认启用")
    mode.add_argument("--proxy-only", action="store_true", help="仅运行代理，需要 --route")
    ap.add_argument("--omp-executable", default="omp", help="OMP 可执行文件名或完整路径")
    arguments = list(sys.argv[1:] if argv is None else argv)
    split = arguments.index("--") if "--" in arguments else len(arguments)
    args = ap.parse_args(arguments[:split])
    omp_args = arguments[split + 1:]
    launch = args.launch_omp or (not args.route and not args.proxy_only)
    if omp_args and not launch:
        ap.error("OMP 参数需要一键启动模式；使用 --launch-omp")
    executable = shutil.which(args.omp_executable) if launch else None
    if launch and not executable:
        ap.error("未找到 OMP；请安装 OMP 或用 --omp-executable 指定完整路径")

    if args.host != "127.0.0.1":
        print("[audit] 拒绝绑定非 127.0.0.1 地址（审计日志含敏感内容）", file=sys.stderr)
        return 2

    routes = [Route(p, u, h) for p, u, h in DEFAULT_ROUTES]
    for spec in args.route:
        parts = spec.split("=")
        if len(parts) == 2:
            routes.append(Route(parts[0], parts[1], "unknown"))
        elif len(parts) == 3:
            routes.append(Route(parts[0], parts[1], parts[2]))
        else:
            print(f"[audit] 无效路由: {spec}", file=sys.stderr)
            return 2

    if not routes and not launch:
        ap.error("请使用 --route /api/=https://upstream.example/v1=openai-completions 配置自己的上游")

    if args.log is None:
        folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), "audit-logs")
        if launch:
            folder = os.path.join(folder, time.strftime("%Y%m%d-%H%M%S", time.gmtime()) + "-" + uuid.uuid4().hex[:8])
        args.log = os.path.join(folder, "audit.jsonl")
    tools_path = args.tools_log or os.path.join(os.path.dirname(os.path.abspath(args.log)), "tools.jsonl")
    if os.path.normcase(os.path.abspath(tools_path)) == os.path.normcase(os.path.abspath(args.log)):
        ap.error("--tools-log 不能与 --log 使用同一个文件")
    handler = type("ConfiguredAuditHandler", (AuditHandler,), {})
    handler.routes = routes
    handler.logger = JsonlLogger(args.log, args.log_max_bytes, args.log_backups)
    handler.tool_logger = JsonlLogger(tools_path, args.log_max_bytes, args.log_backups)
    handler.upstream_connect_timeout = args.upstream_connect_timeout
    handler.verbose = args.verbose
    handler.control_token = secrets.token_urlsafe(32) if launch else None

    port = args.port if args.port is not None else (0 if launch else 8787)
    try:
        server = AuditHTTPServer((args.host, port), handler)
    except OSError:
        handler.logger.close()
        handler.tool_logger.close()
        raise
    server.daemon_threads = True
    print(f"[audit] listening on http://{args.host}:{server.server_address[1]}", file=sys.stderr)
    print(f"[audit] log: {args.log}", file=sys.stderr)
    print(f"[audit] tools: {tools_path}", file=sys.stderr)
    for r in routes:
        print(f"[audit] route {r.prefix} -> {r.upstream} ({r.hint})", file=sys.stderr)
    try:
        if launch:
            return launch_omp(server, handler, executable, omp_args)
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        handler.logger.close()
        handler.tool_logger.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
