#!/usr/bin/env python3
"""
omp_audit_proxy.py — 本地（127.0.0.1-only）LLM API 流式审计反向代理，单文件，纯标准库。

用途：作为 OMP（或任何 OpenAI/Anthropic 兼容客户端）与真实上游之间的透明中转，
把新对话、完整 thinking、工具参数/结果增量追加到可直接读取的 JSON；一键模式按 OMP session/agent 隔离，
无可信身份时关联真实 response ID，最后按 system+首条 user 的 CRC32 分组并用 SHA-256 校验。

它能看到什么（边界声明）：
  - 经过本代理的 HTTP 请求体、响应头、解析后的 SSE 事件。
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

输出：session-<稳定分组ID>-rl.json，每个会话身份、响应链或 CRC32 组只追加新轮次。
OMP 自动标题统一增量追加到 auxiliary/titles.json，避免辅助请求堆积在主目录。
.audit-state 保存内部增量恢复证据；audit.jsonl 和 tools.jsonl 索引默认不生成。
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
import copy
from collections import OrderedDict
from contextlib import contextmanager, ExitStack
import glob
import hashlib
import heapq
import hmac
import http.client
import json
import logging
import logging.handlers
import os
import re
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
import zlib
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
# Redirect fetch at the HTTP boundary. Never put ephemeral URLs into OMP's
# registry: background discovery can persist those URLs into models.db.
LAUNCHER_EXTENSION = r'''
import { createHmac } from "node:crypto";

export default function (pi) {
  const root = process.env.OMP_AUDIT_CONTROL_URL;
  const token = process.env.OMP_AUDIT_CONTROL_TOKEN;
  const supported = new Set(["anthropic-messages", "openai-completions", "openai-responses"]);
  const key = Symbol.for("omp.audit.launcher.routing.v1");
  const state = globalThis[key] || (globalThis[key] = {
    originalFetch: globalThis.fetch,
    destinations: new Map(),
    pending: new Map(),
  });
  function fail(error) {
    process.stderr.write("[audit] routing failed; stopping this OMP session: " + String(error) + "\n");
    process.exit(1);
    throw error;
  }
  function routeKey(entry) {
    return JSON.stringify([entry.provider, entry.api, entry.upstream]);
  }
  function endpoint(model) {
    if (!model || !supported.has(model.api) || typeof model.baseUrl !== "string" || !model.baseUrl.trim()) {
      fail("Unsupported or missing endpoint for selected model " +
        (model ? model.provider + "/" + model.id + " (" + model.api + ")" : "<none>"));
    }
    if (/^http:\/\/(127\.0\.0\.1|localhost|\[::1\]):\d+\/_audit\//.test(model.baseUrl)) {
      fail("Stale audit URL for " + model.provider + "/" + model.id);
    }
    return {provider: model.provider, api: model.api,
      upstream: model.baseUrl.trim().replace(/\/+$/, ""), exact_path: true};
  }
  async function register(entries) {
    const response = await state.originalFetch(root + "/__audit/routes", {
      method: "POST",
      headers: {"Content-Type": "application/json", "Authorization": "Bearer " + token},
      body: JSON.stringify({routes: entries}),
      signal: AbortSignal.timeout(15000),
    });
    if (!response.ok) throw new Error("Audit route registration failed: " + response.status);
    const payload = await response.json();
    if (!Array.isArray(payload.routes) || payload.routes.length !== entries.length) {
      throw new Error("Incomplete audit route registration");
    }
    for (const route of payload.routes) {
      const entry = entries.find(e => e.provider === route.provider);
      if (!entry) throw new Error("Unexpected audit route registration");
      const upstream = new URL(entry.upstream);
      state.destinations.set(routeKey(entry), {
        origin: upstream.origin,
        path: upstream.pathname.replace(/\/+$/, ""),
        local: route.baseUrl,
      });
    }
  }
  async function ensureModel(model) {
    const entry = endpoint(model);
    const id = routeKey(entry);
    if (state.destinations.has(id)) return;
    let pending = state.pending.get(id);
    if (!pending) {
      pending = register([entry]);
      state.pending.set(id, pending);
    }
    try {
      await pending;
    } catch (error) {
      fail(error);
    } finally {
      state.pending.delete(id);
    }
  }
  if (!state.proxyFetch) {
    state.proxyFetch = new Proxy(state.originalFetch, {
      apply(target, receiver, args) {
        const [input, init] = args;
        const url = new URL(input instanceof Request ? input.url : String(input));
        const method = String(init?.method || (input instanceof Request ? input.method : "GET")).toUpperCase();
        const routes = [...state.destinations.values()].sort((a, b) => b.path.length - a.path.length);
        const route = routes.find(r => url.origin === r.origin &&
          (url.pathname === r.path || url.pathname.startsWith(r.path + "/")));
        if (!route) {
          // The hook can fail or be absent for a provider. Never send a model
          // inference request directly to an unregistered HTTP endpoint.
          if (method === "POST" && /\/(?:v1\/)?(?:messages|chat\/completions|responses)\/*$/.test(url.pathname)) {
            throw new Error("Uncovered model request blocked by audit launcher: " + url.origin + url.pathname);
          }
          return Reflect.apply(target, receiver, args);
        }
        const local = route.local + "/" + url.pathname.slice(route.path.length) + url.search;
        const forwarded = input instanceof Request ? new Request(local, input) : local;
        return Reflect.apply(target, receiver, [forwarded, init]);
      },
    });
    globalThis.fetch = state.proxyFetch;
  }
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
        const stale = models.some(m => /^http:\/\/(127\.0\.0\.1|localhost|\[::1\]):\d+\/_audit\//.test(m.baseUrl || ""));
        if (stale) {
          skipped++;
          continue;
        }
        if (endpoints.size !== 1 || !supported.has(model.api) || !model.baseUrl) {
          skipped++;
          continue;
        }
        entries.push(endpoint(model));
      }
      if (ctx.model) endpoint(ctx.model);
      await register(entries);
      if (ctx.model) await ensureModel(ctx.model);
      const ready = await state.originalFetch(root + "/__audit/routes", {
        method: "POST",
        headers: {"Content-Type": "application/json", "Authorization": "Bearer " + token},
        body: JSON.stringify({routes: [], ready: true}),
        signal: AbortSignal.timeout(15000),
      });
      if (!ready.ok) throw new Error("Audit readiness acknowledgement failed");
      process.stderr.write("[audit] temporary routing ready: " + entries.length +
        " provider(s); " + skipped + " provider(s) deferred to selected-model validation.\n");
    } catch (error) {
      fail(error);
    }
  });
  // Some providers have their own transport and do not emit the payload hook.
  // Validate before every turn as well as before each supported HTTP request.
  pi.on("context", async (_event, ctx) => {
    try {
      await ensureModel(ctx.model);
    } catch (error) {
      fail(error);
    }
  });
  pi.on("before_provider_request", async (event, ctx) => {
    try {
      if (!root || !token) fail("Missing private launcher connection");
      await ensureModel(ctx.model);
      if (!event.payload || typeof event.payload !== "object" || Array.isArray(event.payload)) {
        fail("Cannot tag a non-object provider payload");
      }
      const sessionId = ctx.sessionManager.getSessionId();
      const agentId = ctx.agent.id;
      if ([sessionId, agentId].some(value => typeof value !== "string" || !value.length ||
          value.length > 512 || value.includes("\0"))) {
        fail("Missing or invalid session identity");
      }
      const signature = createHmac("sha256", token)
        .update(sessionId + "\0" + agentId).digest("hex");
      return {...event.payload, _omp_audit_v1: {session_id: sessionId, agent_id: agentId, signature}};
    } catch (error) {
      fail(error);
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
# Append-only incremental journal, independent of the rotating JSONL indexes.
# --------------------------------------------------------------------------


def classify_ids(ids: list[dict], fallback: str | None) -> dict:
    candidates = [dict(item) for item in ids if isinstance(item, dict)
                  and isinstance(item.get("id"), str) and item["id"]]
    primary = next((item for item in candidates if str(item.get("source", "")).startswith("body:")
                    or item.get("source") in ("header:response-id", "header:x-response-id")), None)
    if fallback:
        hint = {"id": fallback, "source": "fallback:system+first_user.sha256", "confidence": "low"}
        if not any(item.get("id") == fallback and item.get("source") == hint["source"] for item in candidates):
            candidates.append(hint)
        primary = primary or hint
    return {"ids": candidates, "primary_id": primary["id"] if primary else None,
            "primary_source": primary.get("source") if primary else None,
            "confidence": primary.get("confidence", "high") if primary else "none"}


@contextmanager
def _session_file_lock(path: str):
    # Persistent lock files avoid unlink/recreate races between proxy processes.
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a+b") as handle:
        deadline = time.monotonic() + 10
        while True:
            try:
                handle.seek(0)
                if os.name == "nt":
                    import msvcrt
                    msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except OSError:
                if time.monotonic() >= deadline:
                    raise TimeoutError("Session archive lock timed out")
                time.sleep(0.05)
        try:
            yield
        finally:
            if os.name == "nt":
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _json_delta(before, after, path=()):
    """Lossless JSON changes; unchanged history never appears in an append."""
    if _canon(before) == _canon(after):
        return []
    if isinstance(before, dict) and isinstance(after, dict):
        changes = [{"op": "remove", "path": [*path, key]} for key in before if key not in after]
        for key, value in after.items():
            if key in before:
                changes.extend(_json_delta(before[key], value, (*path, key)))
            else:
                changes.append({"op": "set", "path": [*path, key], "value": value})
        return changes
    if isinstance(before, list) and isinstance(after, list):
        prefix = 0
        while prefix < min(len(before), len(after)) and _canon(before[prefix]) == _canon(after[prefix]):
            prefix += 1
        suffix = 0
        while (suffix < min(len(before), len(after)) - prefix
               and _canon(before[len(before)-suffix-1]) == _canon(after[len(after)-suffix-1])):
            suffix += 1
        old_end, new_end = len(before)-suffix, len(after)-suffix
        splice = [{"op": "splice", "path": list(path), "index": prefix,
                   "delete": old_end-prefix, "values": after[prefix:new_end]}]
        nested = []
        for index in range(prefix, min(old_end, new_end)):
            nested.extend(_json_delta(before[index], after[index], (*path, index)))
        if old_end != new_end:
            start = min(old_end, new_end)
            nested.append({"op": "splice", "path": list(path), "index": start,
                           "delete": max(0, old_end-new_end), "values": after[start:new_end]})
        return nested if len(_canon(nested)) < len(_canon(splice)) else splice
    if isinstance(before, str) and isinstance(after, str) and before and after.startswith(before):
        return [{"op": "append", "path": list(path), "value": after[len(before):]}]
    return [{"op": "set", "path": list(path), "value": after}]


def apply_json_delta(before, changes):
    """Replay a journal frame without modifying its previous state."""
    value = copy.deepcopy(before)
    for change in changes:
        path = change["path"]
        if not path:
            raise ValueError("Root snapshot replacement is not an incremental frame")
        parent = value
        for part in path[:-1]:
            parent = parent[part]
        key = path[-1]
        operation = change["op"]
        if operation == "set":
            parent[key] = copy.deepcopy(change["value"])
        elif operation == "remove":
            del parent[key]
        elif operation == "append":
            parent[key] += change["value"]
        elif operation == "splice":
            start = change["index"]
            parent[key][start:start+change["delete"]] = copy.deepcopy(change["values"])
        else:
            raise ValueError("Unknown journal operation: " + str(operation))
    return value


def session_filename(stream_id):
    """Stable, Windows-safe filename; response changes do not change a stream's name."""
    value = stream_id.removeprefix("response:")
    if value.startswith("crc32:") or value.startswith("unclassified:"):
        value = value.replace(":", "-", 1)
    safe = re.sub(r"[^A-Za-z0-9_.-]", "-", value)[:120]
    if safe != value or not safe or value != value.lower():
        safe = (safe or "id") + "-" + hashlib.sha256(stream_id.encode()).hexdigest()[:16]
    return "session-" + safe + "-rl.jsonl"


def _journal_frames(directory, offsets=None):
    """Merge named journals in commit order, buffering at most one frame per file."""
    offsets = offsets or {}
    paths = set(glob.glob(os.path.join(directory, "session-*-rl.jsonl")))
    if set(offsets) - paths:
        raise ValueError("A journal was removed while the writer is running")
    with ExitStack() as stack:
        pending = []

        def advance(path, handle):
            line = handle.readline()
            if not line:
                return
            if not line.endswith(b"\n"):
                raise ValueError("Incomplete journal tail; refusing to overwrite evidence")
            frame = json.loads(line)
            if not isinstance(frame, dict) or not isinstance(frame.get("sequence"), int):
                raise ValueError("Named journal is missing its commit sequence")
            heapq.heappush(pending, (frame["sequence"], path, frame, len(line), handle))

        for path in sorted(paths):
            offset = offsets.get(path, 0)
            size = os.path.getsize(path)
            if size < offset:
                raise ValueError("Journal truncated while writer is running")
            if size == offset:
                continue
            handle = stack.enter_context(open(path, "rb"))
            handle.seek(offset)
            advance(path, handle)
        while pending:
            sequence, path, frame, length, handle = heapq.heappop(pending)
            if os.path.basename(path) != session_filename(frame.get("stream_id", "")):
                raise ValueError("Journal filename does not match its stream ID")
            yield frame, path, length
            advance(path, handle)


def _single_journal_frames(path):
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            if not line.endswith("\n"):
                raise ValueError("Incomplete journal tail; refusing silent truncation")
            yield json.loads(line)


def iter_session_records(path):
    """Reconstruct exchanges one at a time; the journal itself stays incremental."""
    if os.path.isdir(path) and os.path.isdir(os.path.join(path, ".audit-state")):
        path = os.path.join(path, ".audit-state")
    states, heads = {}, {}
    frames = ((frame for frame, _, _ in _journal_frames(path)) if os.path.isdir(path)
              else _single_journal_frames(path))
    for frame in frames:
        stream = frame["stream_id"]
        if frame.get("schema_version") != 2 or frame.get("base_exchange_id") != heads.get(stream):
            raise ValueError("Incompatible or broken journal chain")
        state = apply_json_delta(states.get(stream, {}), frame["changes"])
        if hashlib.sha256(_canon(state).encode()).hexdigest() != frame["state_sha256"]:
            raise ValueError("Journal state checksum mismatch")
        states[stream], heads[stream] = state, frame["exchange_id"]
        yield copy.deepcopy(state)


class DeltaJournalWriter:
    """One append-only journal per session identity, response chain or CRC32 group."""

    def __init__(self, directory: str):
        self.directory = os.path.abspath(directory)
        self.path = None
        self.lock = threading.Lock()
        self.states, self.heads, self.responses, self.contexts, self.seen = {}, {}, {}, {}, {}
        self.offsets = {}
        self.sequence = 0
        self.frames = 0
        os.makedirs(self.directory, exist_ok=True)

    def flush(self):
        with self.lock:
            pass  # Every committed frame is already flushed and fsynced.

    def _accept(self, frame, state, path):
        stream = frame["stream_id"]
        self.states[stream] = state
        self.heads[stream] = frame["exchange_id"]
        self.seen[frame["exchange_id"]] = path
        self.sequence = frame["sequence"]
        # A request without a session identity must never attach to an explicitly
        # identified session just because the provider reused a response ID.
        explicit_session = state.get("audit_session_id")
        if frame.get("response_id") and not explicit_session:
            self.responses[(frame["scope"], frame["response_id"])] = stream
        if frame.get("context_sha256") and not explicit_session:
            self.contexts[(frame["scope"], frame["context_crc32"], frame["context_sha256"])] = stream
            # Older journals included transient cache hints in their identity.
            # Restore a normalized alias without renaming or rewriting evidence.
            anchor = initial_context(state["protocol"], state["request"].get("body"))
            if anchor is not None:
                encoded = _canon(anchor).encode("utf-8")
                normalized = (frame["scope"], f"{zlib.crc32(encoded):08x}",
                              hashlib.sha256(encoded).hexdigest())
                self.contexts.setdefault(normalized, stream)
        self.frames += 1

    def _sync(self):
        for frame, path, length in _journal_frames(self.directory, self.offsets):
            stream = frame["stream_id"]
            if (frame.get("schema_version") != 2 or frame["sequence"] <= self.sequence
                    or frame.get("base_exchange_id") != self.heads.get(stream)):
                raise ValueError("Incompatible or broken journal chain")
            state = apply_json_delta(self.states.get(stream, {}), frame["changes"])
            if hashlib.sha256(_canon(state).encode()).hexdigest() != frame["state_sha256"]:
                raise ValueError("Journal state checksum mismatch")
            self._accept(frame, state, path)
            self.offsets[path] = self.offsets.get(path, 0) + length

    def write(self, record):
        if not record.get("request") or not record.get("protocol"):
            return None
        body = record["request"].get("body")
        anchor = initial_context(record["protocol"], body)
        encoded = _canon(anchor).encode("utf-8") if anchor is not None else None
        crc = f"{zlib.crc32(encoded):08x}" if encoded is not None else None
        fingerprint = hashlib.sha256(encoded).hexdigest() if encoded is not None else None
        grouping = classify_ids((record.get("classification") or {}).get("ids", []), None)
        response_id = grouping["primary_id"]
        previous_id = body.get("previous_response_id") if isinstance(body, dict) else None
        scope = str(record.get("upstream", "")) + "|" + record["protocol"]
        session_id = record.get("audit_session_id")
        agent_id = record.get("audit_agent_id")
        if session_id is not None and (not isinstance(session_id, str) or not session_id):
            raise ValueError("audit_session_id must be a nonempty string")
        if agent_id is not None and (not isinstance(agent_id, str) or not agent_id):
            raise ValueError("audit_agent_id must be a nonempty string")
        lock_path = os.path.join(self.directory, ".session-locks", "sessions-rl.lock")
        with self.lock, _session_file_lock(lock_path):
            self._sync()
            if record["exchange_id"] in self.seen:
                self.path = self.seen[record["exchange_id"]]
                return self.path
            if session_id:
                identity = [scope, session_id] + ([agent_id] if agent_id else [])
                digest = hashlib.sha256(_canon(identity).encode("utf-8")).hexdigest()
                stream, reason = "audit-session-" + digest[:32], "audit_session_id"
            else:
                stream = self.responses.get((scope, response_id)) if response_id else None
                reason = "response_id" if stream else None
                if not stream and previous_id:
                    stream = self.responses.get((scope, previous_id))
                    if stream:
                        reason = "previous_response_id"
                if not stream and crc:
                    stream = self.contexts.get((scope, crc, fingerprint))
                    if not stream:
                        collision = any(key[1] == crc for key in self.contexts)
                        suffix = hashlib.sha256(_canon([scope, fingerprint]).encode()).hexdigest()[:16]
                        stream = "crc32:" + crc + ("-" + suffix if collision else "")
                    reason = "system+first_user.crc32"
                if not stream:
                    stream = ("response:" + response_id + "@" + hashlib.sha256(scope.encode()).hexdigest()[:12]
                              if response_id else "unclassified:" + record["exchange_id"])
                    reason = "response_id" if response_id else "unclassified"
            record["session_file"] = session_filename(stream)
            self.path = os.path.join(self.directory, record["session_file"])
            record["stream_id"] = stream
            state = copy.deepcopy(record)
            frame = {"schema_version": 2, "event": "exchange_delta", "stream_id": stream,
                     "sequence": self.sequence + 1,
                     "grouped_by": reason, "scope": scope, "context_crc32": crc,
                     "context_sha256": fingerprint, "response_id": response_id,
                     "exchange_id": record["exchange_id"], "base_exchange_id": self.heads.get(stream),
                     "thinking": thinking_summary(state),
                     "changes": _json_delta(self.states.get(stream, {}), state),
                     "state_sha256": hashlib.sha256(_canon(state).encode()).hexdigest()}
            payload = (_canon(frame) + "\n").encode("utf-8")
            with open(self.path, "ab") as handle:
                try:
                    os.chmod(self.path, 0o600)
                except OSError:
                    pass
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            self.offsets[self.path] = self.offsets.get(self.path, 0) + len(payload)
            self._accept(frame, state, self.path)
        return self.path


def _message_view(message):
    """Readable ChatML content, without transport-specific marker strings."""
    result = {key: copy.deepcopy(message[key]) for key in
              ("role", "content", "thinking", "tool_calls", "tool_results", "tool_call_id") if key in message}
    if result.get("tool_calls"):
        calls = []
        for call in result["tool_calls"]:
            function = call.get("function") or call
            calls.append({"id": call.get("id"), "name": function.get("name"),
                          "arguments": _arguments(function.get("arguments"))})
        result["tool_calls"] = calls
        content = result.get("content")
        if isinstance(content, str):
            for call in calls:
                content = content.replace(f"[tool_use {call.get('name', '')}({call.get('id', '')})]", "")
            result["content"] = content or None
    return result


def _tool_identity(event):
    return _canon({key: event.get(key) for key in
                   ("tool_call_id", "tool_name", "arguments", "content", "is_error")})


class SessionJsonWriter:
    """Human-readable JSON, adding new turns by replacing only the closing footer."""

    FOOTER = b"\n  ]\n}\n"
    TITLE_STREAM = "auxiliary:titles"

    def __init__(self, directory):
        self.directory = os.path.abspath(directory)
        self.journal = DeltaJournalWriter(os.path.join(self.directory, ".audit-state"))
        self.lock = threading.Lock()
        self.offsets, self.raw_states, self.heads = {}, {}, {}
        self.documents, self.messages_seen, self.calls_seen, self.results_seen = {}, {}, {}, {}
        self.tool_counts, self.delta_tool_seen = {}, {}
        self.dirty = set()
        self.path = None

    @staticmethod
    def filename(stream):
        if stream == SessionJsonWriter.TITLE_STREAM:
            return os.path.join("auxiliary", "titles.json")
        return session_filename(stream)[:-1]  # .json, not .jsonl

    @staticmethod
    def document_stream(record, stream):
        body = (record.get("request") or {}).get("body")
        if not isinstance(body, dict) or body.get("tools") or record.get("tool_trace"):
            return stream
        messages = derive_chatml_request(record["protocol"], body)
        system = "\n".join(_text_of(message.get("content")) or "" for message in messages
                           if message.get("role") in ("system", "developer"))
        title_prefix = ("Write a ~5 word title using only the task described in the next user message.\n"
                        "- You MUST ONLY answer with the title, inside the <title> tag.")
        if system.replace("\r\n", "\n").startswith(title_prefix):
            return SessionJsonWriter.TITLE_STREAM
        return stream

    def _tool_events(self, record, assistant, stream):
        """Deduplicate echoed history, never distinct response occurrences."""
        counts = self.tool_counts.setdefault(stream, {"calls": {}, "results": {}, "results_by_call": {}})
        body = record["request"].get("body") or {}
        previous = body.get("previous_response_id") if record["protocol"] == "openai-responses" else None
        delta_seen = self.delta_tool_seen.setdefault(stream, set())
        context_counts, result_counts, response_counts = {}, {}, {}
        calls, results = [], []
        request_result_counts, result_call_keys = {}, {}
        for event in record.get("tool_trace", []):
            if event.get("event") == "tool_result":
                key = _tool_identity(event)
                request_result_counts[key] = request_result_counts.get(key, 0) + 1
                result_call_keys[key] = _tool_identity({field: event.get(field) for field in
                                                       ("tool_call_id", "tool_name", "arguments")})
        # Reserve outstanding calls for results that are definitely new before
        # considering an identical result from a shortened history window.
        # Otherwise old X can consume the slot belonging to new Y, and inflate
        # result counts enough to suppress a later genuine execution of X.
        definite_by_call, replay_candidates = {}, {}
        for key, number in request_result_counts.items():
            call_key = result_call_keys[key]
            old_count = counts["results"].get(key, 0)
            definite_by_call[call_key] = definite_by_call.get(call_key, 0) + max(0, number - old_count)
            replay_candidates[call_key] = replay_candidates.get(call_key, 0) + min(number, old_count)
        uncertain_budget = {
            key: min(number, max(0, counts["calls"].get(key, 0)
                                 - counts["results_by_call"].get(key, 0)
                                 - definite_by_call.get(key, 0)))
            for key, number in replay_candidates.items()
        }
        emitted_results = {}

        def emit(target, event, kind):
            item = copy.deepcopy(event)
            item["audit_event_id"] = f"{record['exchange_id']}:{kind}:{len(target)}"
            target.append(item)

        for event in record.get("tool_trace", []):
            kind = event.get("event")
            key = _tool_identity(event)
            if kind == "tool_call":
                emit(calls, event, "call")
                response_counts[key] = response_counts.get(key, 0) + 1
            elif kind in ("tool_call_context", "tool_result"):
                is_call = kind == "tool_call_context"
                uncertain = False
                local = context_counts if is_call else result_counts
                total = counts["calls" if is_call else "results"]
                local[key] = local.get(key, 0) + 1
                call_key = _tool_identity({field: event.get(field) for field in
                                           ("tool_call_id", "tool_name", "arguments")})
                if previous:
                    # Responses input is a delta relative to this response, not
                    # the same absolute history position in every request.
                    occurrence = (previous, kind, key, local[key])
                    fresh = occurrence not in delta_seen
                    delta_seen.add(occurrence)
                    if is_call and total.get(key, 0) >= local[key]:
                        fresh = False  # Already captured in the prior response.
                else:
                    fresh = local[key] > total.get(key, 0)
                    if not is_call and not fresh:
                        # Prefer the last possible occurrences in a shortened
                        # history while keeping their original output order.
                        replay_candidates[call_key] -= 1
                        fresh = replay_candidates[call_key] < uncertain_budget[call_key]
                        uncertain = fresh
                        if fresh:
                            uncertain_budget[call_key] -= 1
                if fresh:
                    emit(calls if is_call else results, event, "context-call" if is_call else "result")
                    if not is_call:
                        emitted_results[key] = emitted_results.get(key, 0) + 1
                        if uncertain:
                            results[-1]["provenance_uncertain"] = True
                        by_call = counts["results_by_call"]
                        by_call[call_key] = by_call.get(call_key, 0) + 1
        for key, number in context_counts.items():
            counts["calls"][key] = max(counts["calls"].get(key, 0), number)
        # Count exported occurrences, not only the longest observed history.
        # Delta inputs and shortened windows can reveal more occurrences than
        # any one request contains; a later full history must not repeat them.
        for key, number in emitted_results.items():
            counts["results"][key] = counts["results"].get(key, 0) + number
        # Imported records may lack tool_trace. Match by occurrence within this
        # response so that two identical calls are preserved, without exporting
        # the same call once from trace and again from the assistant view.
        assistant_counts = {}
        for call in assistant.get("tool_calls", []):
            event = {"event": "tool_call", "tool_call_id": call.get("id"), "tool_name": call.get("name"),
                     "arguments": call.get("arguments"), "source": "assistant"}
            key = _tool_identity(event)
            assistant_counts[key] = assistant_counts.get(key, 0) + 1
            if assistant_counts[key] > response_counts.get(key, 0):
                emit(calls, event, "call")
        for key in response_counts.keys() | assistant_counts.keys():
            counts["calls"][key] = counts["calls"].get(key, 0) + max(
                response_counts.get(key, 0), assistant_counts.get(key, 0))
        return calls, results

    def _make_turn(self, record, frame, document_stream):
        messages = (record.get("chatml") or {}).get("messages", [])
        if (record.get("method") in ("GET", "HEAD", "OPTIONS") and not record.get("tool_trace")
                and not any(message.get("content") or message.get("thinking") or message.get("tool_calls")
                            or message.get("tool_results") for message in messages)):
            return None  # Usage/model metadata remains in evidence, not an empty conversation JSON.
        stream = document_stream
        message_keys = self.messages_seen.setdefault(stream, set())
        call_keys = self.calls_seen.setdefault(stream, set())
        result_keys = self.results_seen.setdefault(stream, set())
        request_messages = derive_chatml_request(record["protocol"], record["request"].get("body"))
        new_inputs, recovered = [], []
        for index, message in enumerate(request_messages):
            message = _message_view(message)
            key = (index, _canon(message))
            body = record["request"].get("body") or {}
            if (record.get("readable_format_version", 1) >= 2 and record["protocol"] == "openai-responses"
                    and body.get("previous_response_id")
                    and message.get("role") not in ("system", "developer")):
                key = (body["previous_response_id"], *key)
            if key in message_keys:
                continue
            message_keys.add(key)
            item = {"context_index": index, **message}
            (new_inputs if message.get("role") in ("system", "developer", "user") else recovered).append(item)
        all_messages = (record.get("chatml") or {}).get("messages", [])
        assistant = (_message_view(all_messages[-1]) if len(all_messages) > len(request_messages)
                     and all_messages[-1].get("role") == "assistant" else {})
        if assistant:
            message_keys.add((len(request_messages), _canon(assistant)))
        calls, results = [], []
        legacy = record.get("readable_format_version", 1) < 2
        for event in record.get("tool_trace", []) if legacy else []:
            if event.get("event") in ("tool_call", "tool_call_context"):
                key = _tool_identity(event)
                if key not in call_keys:
                    call_keys.add(key)
                    calls.append(copy.deepcopy(event))
            elif event.get("event") == "tool_result":
                key = _tool_identity(event)
                if key not in result_keys:
                    result_keys.add(key)
                    results.append(copy.deepcopy(event))
        # Some imported records have assistant tool calls but no derived tool_trace.
        for call in assistant.get("tool_calls", []) if legacy else []:
            event = {"event": "tool_call", "tool_call_id": call.get("id"), "tool_name": call.get("name"),
                     "arguments": call.get("arguments"), "source": "assistant"}
            key = _tool_identity(event)
            if key not in call_keys:
                call_keys.add(key)
                calls.append(event)
        if legacy:
            # Rebuild occurrence state from evidence, not the legacy view
            # which may already have collapsed separate identical calls.
            self._tool_events(record, assistant, stream)
        else:
            calls, results = self._tool_events(record, assistant, stream)
        response = record.get("response") or {}
        turn = {"sequence": frame["sequence"], "exchange_id": record["exchange_id"],
                "response_id": frame.get("response_id"), "timestamp": record.get("ts"),
                "protocol": record.get("protocol"), "http_status": response.get("status"),
                "input_messages": new_inputs, "recovered_messages": recovered,
                "thinking": assistant.get("thinking"), "content": assistant.get("content"),
                "tool_calls": calls, "tool_results": results}
        if document_stream == self.TITLE_STREAM:
            turn["request_purpose"] = "title_generation"
            turn["source_stream_id"] = frame["stream_id"]
        source_assistant = all_messages[-1] if assistant else {}
        if source_assistant.get("stop_reason"):
            turn["stop_reason"] = source_assistant["stop_reason"]
        if response.get("usage"):
            turn["usage"] = response["usage"]
        if "sse_complete" in response:
            turn["sse_complete"] = response["sse_complete"]
        for key in ("duration_ms", "error", "client_disconnect", "response_truncated_by"):
            if key in record:
                turn[key] = record[key]
        return turn

    def _write_document(self, stream):
        document = self.documents[stream]
        path = os.path.join(self.directory, self.filename(stream))
        os.makedirs(os.path.dirname(path), exist_ok=True)
        existing = None
        if os.path.exists(path):
            try:
                with open(path, encoding="utf-8") as handle:
                    existing = json.load(handle)
            except json.JSONDecodeError:
                pass  # The durable internal journal can repair an interrupted footer append.
        if existing is not None:
            if (existing.get("schema_version") != 3 or existing.get("stream_id") != stream
                    or not isinstance(existing.get("turns"), list)):
                raise ValueError("Refusing to replace an incompatible readable JSON")
            count = len(existing["turns"])
            if existing["turns"] != document["turns"][:count]:
                raise ValueError("Readable JSON disagrees with durable evidence; refusing overwrite")
            additions = document["turns"][count:]
            if not additions:
                return
            rendered = ["\n".join("    " + line for line in json.dumps(turn, ensure_ascii=False, indent=2).splitlines())
                        for turn in additions]
            payload = ((",\n" if count else "") + ",\n".join(rendered)).encode("utf-8") + self.FOOTER
            with open(path, "r+b") as handle:
                handle.seek(-len(self.FOOTER), os.SEEK_END)
                if handle.read() != self.FOOTER:
                    raise ValueError("Readable JSON has an unexpected footer")
                handle.seek(-len(self.FOOTER), os.SEEK_END)
                handle.write(payload)
                handle.truncate()
                handle.flush()
                os.fsync(handle.fileno())
            return
        # Initial creation or crash recovery, never the normal per-response write path.
        metadata = {key: value for key, value in document.items() if key != "turns"}
        header = json.dumps(metadata, ensure_ascii=False, indent=2)[:-2] + ',\n  "turns": [\n'
        turns = ["\n".join("    " + line for line in json.dumps(turn, ensure_ascii=False, indent=2).splitlines())
                 for turn in document["turns"]]
        temporary = path + ".tmp"
        with open(temporary, "wb") as handle:
            try:
                os.chmod(temporary, 0o600)
            except OSError:
                pass
            handle.write(header.encode("utf-8") + ",\n".join(turns).encode("utf-8") + self.FOOTER)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)

    def _sync(self):
        with _session_file_lock(os.path.join(self.journal.directory, ".session-locks", "sessions-rl.lock")):
            for frame, path, length in _journal_frames(self.journal.directory, self.offsets):
                stream = frame["stream_id"]
                if frame.get("base_exchange_id") != self.heads.get(stream):
                    raise ValueError("Broken internal evidence chain")
                state = apply_json_delta(self.raw_states.get(stream, {}), frame["changes"])
                if hashlib.sha256(_canon(state).encode()).hexdigest() != frame["state_sha256"]:
                    raise ValueError("Internal evidence checksum mismatch")
                document_stream = self.document_stream(state, stream)
                turn = self._make_turn(state, frame, document_stream)
                if turn is not None:
                    auxiliary = document_stream == self.TITLE_STREAM
                    document = self.documents.setdefault(document_stream, {"schema_version": 3, "stream_id": document_stream,
                        "grouped_by": "request_purpose" if auxiliary else frame["grouped_by"],
                        "context_crc32": None if auxiliary else frame.get("context_crc32"),
                        "description": "New conversation content only; thinking and tool payloads are directly readable.",
                        "turns": []})
                    document["turns"].append(turn)
                    self.dirty.add(document_stream)
                self.raw_states[stream], self.heads[stream] = state, frame["exchange_id"]
                self.offsets[path] = self.offsets.get(path, 0) + length
        for stream in list(self.dirty):
            self._write_document(stream)
            self.dirty.remove(stream)

    def write(self, record):
        if not record.get("request") or not record.get("protocol"):
            return None
        with self.lock, _session_file_lock(os.path.join(self.journal.directory, "readable-json.lock")):
            record.setdefault("readable_format_version", 2)
            internal = self.journal.write(record)
            self._sync()
            document_stream = self.document_stream(record, record["stream_id"])
            if document_stream not in self.documents:
                self.path = internal
                record["session_file"] = os.path.relpath(internal, self.directory)
                return self.path
            name = self.filename(document_stream)
            self.path = os.path.join(self.directory, name)
            record["session_file"] = name
            return self.path

    def flush(self):
        with self.lock, _session_file_lock(os.path.join(self.journal.directory, "readable-json.lock")):
            self._sync()


def thinking_summary(record):
    messages = (record.get("chatml") or {}).get("messages", [])
    assistant = messages[-1] if messages and messages[-1].get("role") == "assistant" else {}
    text = assistant.get("thinking") or ""
    return {"present": bool(text), "characters": len(text), "field": "chatml.messages[-1].thinking"}


def audit_index(record):
    """No prompt, tool result, or response payloads duplicated in the index."""
    names = ("ts", "ts_epoch", "exchange_id", "protocol", "stream_id", "session_file", "duration_ms",
             "error", "health", "client_disconnect", "response_truncated_by", "session_export_error")
    return {**{key: record[key] for key in names if key in record},
            "status": (record.get("response") or {}).get("status"),
            "response_id": classify_ids((record.get("classification") or {}).get("ids", []), None)["primary_id"],
            "thinking": thinking_summary(record)}


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


def _context_content(content):
    """Ignore provider cache hints on content blocks, preserving actual payloads."""
    if isinstance(content, list):
        return [{key: value for key, value in block.items() if key != "cache_control"}
                if isinstance(block, dict) else block for block in content]
    return content


def initial_context(protocol: str, body):
    """Anchor on system and first-user content, excluding transient cache hints."""
    if not isinstance(body, dict):
        return None
    system = None
    first_user = None
    if protocol == "anthropic-messages":
        system = _context_content(body.get("system"))
        for m in body.get("messages") or []:
            if isinstance(m, dict) and m.get("role") == "user":
                first_user = _context_content(m.get("content"))
                break
    else:  # openai-completions / openai-responses / unknown 按 openai 形尝试
        msgs = body.get("messages") or body.get("input") or []
        if isinstance(msgs, str):
            first_user = msgs
            msgs = []
        sys_parts = []
        if body.get("instructions"):
            sys_parts.append(_context_content(body["instructions"]))
        for m in msgs:
            if not isinstance(m, dict):
                continue
            if m.get("role") in ("system", "developer"):
                sys_parts.append(_context_content(m.get("content")))
            if m.get("role") == "user":
                first_user = _context_content(m.get("content"))
                break
        system = sys_parts if sys_parts else None
    if system is None and first_user is None:
        return None
    return {"system": system, "first_user": first_user}


def fallback_stream_id(protocol: str, body) -> str | None:
    # Legacy grouping hint only; authenticated session identities take priority.
    anchor = initial_context(protocol, body)
    if anchor is None:
        return None
    digest = hashlib.sha256(_canon(anchor).encode("utf-8")).hexdigest()
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
                thinking = "".join(c.get("thinking", "") for c in m["content"]
                                   if isinstance(c, dict) and c.get("type") == "thinking")
                if thinking:
                    entry["thinking"] = thinking
                    entry["content"] = _text_of([c for c in m["content"]
                                                  if not isinstance(c, dict) or c.get("type") != "thinking"])
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
            if m.get("reasoning_content") or m.get("reasoning") or m.get("thinking"):
                entry["thinking"] = m.get("reasoning_content") or m.get("reasoning") or m.get("thinking")
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
                candidate_items = [(key, c) for key, c in self._calls.items()
                                   if call_id and key[0] == scope and key[1] == call_id]
                if context is not None:
                    candidate_items = [(key, c) for key, c in candidate_items
                                       if self._signature(c) == self._signature(context)]
                elif previous_response_id:
                    candidate_items = [(key, c) for key, c in candidate_items
                                       if c.get("response_id") == previous_response_id]
                candidates = [c for _, c in candidate_items]
                candidate_keys = frozenset(key for key, _ in candidate_items)
                ambiguous = len(candidates) > 1
                observed = candidates[0] if len(candidates) == 1 else None
                call = context or observed or {}
                fingerprint = hashlib.sha256(_canon([scope, call_id, self._signature(call),
                                                     result.get("content"), result.get("is_error"),
                                                     (context or {}).get("source"), result.get("source"),
                                                     previous_response_id]).encode()).hexdigest()
                first = self._seen_results.get(fingerprint)
                if first is None:
                    self._seen_results[fingerprint] = (exchange_id, candidate_keys)
                    self._trim(self._seen_results)
                else:
                    self._seen_results.move_to_end(fingerprint)
                replay_uncertain = first is not None and first[1] != candidate_keys
                events.append({
                    "event": "tool_result",
                    "tool_call_id": call_id,
                    "tool_name": call.get("name") or result.get("name"),
                    "tool_kind_hint": tool_kind_hint(call.get("name") or result.get("name")),
                    "arguments": _arguments(call.get("arguments")),
                    "call_exchange_id": (observed or {}).get("exchange_id"),
                    "call_response_id": (observed or {}).get("response_id"),
                    "call_ordinal": (observed or {}).get("ordinal"),
                    "result_exchange_id": exchange_id,
                    "correlated": bool(context or observed),
                    "correlation_source": "request_context" if context else ("response_cache" if observed else "unmatched"),
                    "call_exchange_ambiguous": ambiguous,
                    "replayed_context": None if replay_uncertain else first is not None,
                    "result_origin_ambiguous": replay_uncertain or ambiguous,
                    "first_result_exchange_id": first[0] if first else exchange_id,
                    "is_error": result.get("is_error"),
                    "content": result.get("content"),
                    "source": result.get("source"),
                })
        return events

    def observe_calls(self, exchange_id: str, calls: list[dict], scope: str | None = None, response_id=None) -> list[dict]:
        events = []
        with self._lock:
            for ordinal, call in enumerate(calls):
                call_id = call.get("id")
                event = {
                    "event": "tool_call",
                    "tool_call_id": call_id,
                    "tool_name": call.get("name"),
                    "tool_kind_hint": tool_kind_hint(call.get("name")),
                    "call_exchange_id": exchange_id,
                    "call_response_id": response_id,
                    "call_ordinal": ordinal,
                    "arguments": _arguments(call.get("arguments")),
                    "source": "response",
                    "correlated": False,
                }
                if call_id:
                    key = (scope, call_id, exchange_id, ordinal)
                    self._calls[key] = {**call, "exchange_id": exchange_id, "response_id": response_id,
                                        "ordinal": ordinal}
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
        self.complete = False  # A protocol terminal event was captured, independent of socket closure.
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
            self.complete = True
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
            if (obj.get("type") or event) == "message_stop":
                self.complete = True
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
                self.complete = True
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
    def __init__(self, prefix: str, upstream: str, hint: str, *, automatic: bool = False,
                 exact_path: bool = False):
        self.exact_path = exact_path
        self.prefix = prefix
        self.upstream = upstream.rstrip("/")
        self.hint = hint
        self.automatic = automatic
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
    session_writer: SessionJsonWriter | None = None
    raw_log = False
    control_token: str | None = None
    route_lock = threading.Lock()
    upstream_connect_timeout = 30.0
    verbose = False

    def log_message(self, fmt, *args):  # 静音默认访问日志；审计写入结构化文件。
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
            record.setdefault("error", "socket_timeout")
            # A timed-out upstream must finish the client exchange too. Leaving
            # HTTP/1.1 open here makes OMP wait for its own watchdog indefinitely.
            try:
                if not self.wfile.closed and not self._response_started:
                    body = json.dumps({"error": record["error"]}).encode()
                    self._response_started = True
                    self.send_response(504)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(body)))
                    self.send_header("Connection", "close")
                    self.end_headers()
                    self.wfile.write(body)
                    self.wfile.flush()
            except OSError:
                record["client_disconnect"] = True
            self.close_connection = True
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
            if self.session_writer:
                try:
                    self.session_writer.write(record)
                except (OSError, ValueError, TypeError, KeyError, IndexError) as exc:
                    record["session_export_error"] = type(exc).__name__
                    print(f"[audit] incremental journal export failed: {exc}", file=sys.stderr)
                    if not self.logger:
                        try:
                            recovery = os.path.join(self.session_writer.directory,
                                                    "failed-" + record["exchange_id"] + ".json")
                            with open(recovery, "w", encoding="utf-8") as handle:
                                json.dump(record, handle, ensure_ascii=False, indent=2)
                        except OSError as recovery_error:
                            print(f"[audit] recovery JSON write failed: {recovery_error}", file=sys.stderr)
            if self.logger:
                # Preserve a failed export as explicit recovery evidence, never silently lose it.
                self.logger.write(record if self.raw_log or record.get("session_export_error") else audit_index(record))
            if self.tool_logger:
                grouping = record.get("classification") or {}
                for index, event in enumerate(record.get("tool_trace", [])):
                    if not self.raw_log:
                        if event.get("replayed_context"):
                            continue
                        self.tool_logger.write({"ts": record["ts"], "exchange_id": record["exchange_id"],
                                                "stream_id": record.get("stream_id"),
                                                "session_file": record.get("session_file"), "tool_trace_index": index,
                                                "event": event.get("event"), "tool_name": event.get("tool_name"),
                                                "tool_call_id": event.get("tool_call_id")})
                        continue
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
                exact_path = entry.get("exact_path", False)
                if not isinstance(exact_path, bool):
                    raise ValueError("Invalid path mode")
                if not isinstance(provider, str) or not provider or provider in providers:
                    raise ValueError("Missing or duplicate provider")
                if api not in SUPPORTED_APIS or not isinstance(upstream, str):
                    raise ValueError("Unsupported route protocol")
                # Mirror OMP's normalizeAnthropicBaseUrl before replacing the
                # model URL: its client appends /v1/messages to the local URL.
                # This is launcher-only; explicit --route paths stay literal.
                upstream = upstream.strip().rstrip("/")
                if not exact_path and api == "anthropic-messages" and upstream.endswith("/v1"):
                    upstream = upstream[:-3]
                parsed = urllib.parse.urlsplit(upstream)
                if (parsed.scheme not in ("http", "https") or not parsed.hostname
                        or parsed.username or parsed.password or parsed.query or parsed.fragment):
                    raise ValueError("Upstream must be an HTTP(S) base URL without credentials, query or fragment")
                if parsed.hostname in ("localhost", "127.0.0.1", "::1") and parsed.port == self.server.server_port:
                    raise ValueError("Refusing a proxy routing loop")
                if parsed.hostname in ("localhost", "127.0.0.1", "::1") and parsed.path.startswith("/_audit/"):
                    raise ValueError("Refusing a stale audit endpoint")
                providers.add(provider)
                identity = [provider, api, upstream] + (["fetch"] if exact_path else [])
                key = hashlib.sha256(_canon(identity).encode()).hexdigest()[:20]
                prefix = "/_audit/" + key + "/"
                routes.append(Route(prefix, upstream, api, automatic=True, exact_path=exact_path))
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
        if isinstance(body_json, dict) and "_omp_audit_v1" in body_json:
            marker = body_json.pop("_omp_audit_v1")
            session_id = marker.get("session_id") if isinstance(marker, dict) else None
            agent_id = marker.get("agent_id") if isinstance(marker, dict) else None
            signature = marker.get("signature") if isinstance(marker, dict) else None
            valid_identity = all(isinstance(value, str) and 0 < len(value) <= 512 and "\0" not in value
                                 for value in (session_id, agent_id))
            valid_signature = (isinstance(signature, str) and len(signature) == 64
                               and all(character in "0123456789abcdef" for character in signature))
            expected = None
            if self.control_token and valid_identity and valid_signature:
                expected = hmac.new(self.control_token.encode("utf-8"),
                                    (session_id + "\0" + agent_id).encode("utf-8"), hashlib.sha256).hexdigest()
            if expected is None or not hmac.compare_digest(signature, expected):
                # This is a private launcher field. Never forward a malformed
                # marker or let an unauthenticated value control grouping.
                record["error"] = "invalid_audit_identity"
                self._control_reply(403, {"error": "invalid_audit_identity"})
                return
            record["audit_session_id"] = session_id
            record["audit_agent_id"] = agent_id
            body = json.dumps(body_json, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        conversation_key = fallback_stream_id(protocol, body_json) or "unkeyed"
        # Prompt hashes are grouping hints, not reliable unique session IDs.
        # Correlate by call identity + context; ambiguous origins stay unknown.
        trace_scope = route.prefix
        if record.get("audit_session_id"):
            identity_scope = _canon([record["audit_session_id"], record["audit_agent_id"]])
            trace_scope += "session-" + hashlib.sha256(identity_scope.encode("utf-8")).hexdigest()
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
        base_path = route.base_path
        # A provider shim may advertise OpenAI but send Anthropic messages.
        # Normalize using the actual wire protocol, not only the catalog hint.
        if route.automatic and not route.exact_path and protocol == "anthropic-messages" and base_path.endswith("/v1"):
            base_path = base_path[:-3]
        if route.exact_path:
            upstream_path = base_path + sub_path
        else:
            upstream_path = base_path + "/" + sub_path if not sub_path.startswith("/") else base_path + sub_path
        if not upstream_path.startswith("/"):
            upstream_path = "/" + upstream_path

        record.update({
            "route": route.prefix,
            "protocol": protocol,
            "routing": {"mode": "automatic" if route.automatic else "explicit",
                        "catalog_api": route.hint, "wire_api": protocol,
                        "base_path_normalized": base_path != route.base_path},
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

        try:
            conn.request(self.command, upstream_path, body=body, headers=headers)
            # getresponse() detaches conn.sock for close-delimited responses;
            # HTTPResponse still owns a file wrapper around this same socket.
            upstream_socket = conn.sock
            resp = conn.getresponse()
        except socket.timeout:
            record["error"] = "upstream_connect_timeout"
            conn.close()
            raise
        # 响应开始后取消读超时（SSE 长流依赖 omp 端自己的 watchdog/abort）
        if upstream_socket is not None:
            upstream_socket.settimeout(None)

        record["response"] = {
            "status": resp.status,
            "headers": redact_headers(resp.getheaders()),
        }
        # 响应头里的 request-id 类 ID
        header_ids = []
        for name in ("response-id", "x-response-id", "request-id", "x-request-id",
                     "anthropic-request-id", "openai-request-id", "x-completion-id"):
            v = resp.getheader(name)
            if v:
                header_ids.append({"id": v, "source": f"header:{name}", "confidence": "high"})

        if route.automatic and resp.status in (301, 302, 303, 307, 308):
            # Native fetch follows redirects internally without re-entering the
            # launcher wrapper. A 307/308 would also replay its original body,
            # including the private identity marker, to an unaudited endpoint.
            # Retain upstream status/headers as evidence, but never expose its
            # Location to the automatic client. Explicit routes retain their
            # existing redirect semantics for manually managed clients.
            record["error"] = "upstream_redirect_blocked"
            record["response"]["body_not_captured"] = "redirect_blocked"
            payload = {"error": "upstream_redirect_blocked", "upstream_status": resp.status}
            record["proxy_response"] = {"status": 502, "body": payload}
            record["classification"] = classify_ids(header_ids, fallback_stream_id(protocol, body_json))
            record["chatml"] = {"messages": derive_chatml_request(protocol, body_json)}
            try:
                resp.close()
            finally:
                conn.close()
            self._response_started = True
            self._control_reply(502, payload)
            return

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
        if is_sse:
            record["response"]["sse_complete"] = tap.complete
        if stream_error:
            record["error"] = "upstream_stream_error"
            if not (is_sse and tap.complete):
                record["response_truncated_by"] = stream_error
        if response_over_limit:
            record["response"]["body_truncated"] = True
        if client_gone:
            record["client_disconnect"] = True
            if not (is_sse and tap.complete):
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
        record["classification"] = classify_ids(ids, fallback)

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
    ap.add_argument("--log", help="可选审计索引路径（--indexes 启用）；未指定输出目录时沿用该文件的父目录")
    ap.add_argument("--indexes", action="store_true", help="可选：额外生成 audit.jsonl / tools.jsonl 小型索引")
    ap.add_argument("--raw-log", action="store_true", help="显式调试：额外保存全量原始审计和工具日志（占用大）")
    ap.add_argument("--log-max-bytes", type=int, default=64 * 1024 * 1024)
    ap.add_argument("--log-backups", type=int, default=5)
    ap.add_argument("--tools-log", help="可选工具索引路径（--indexes 启用），默认与 --log 同目录下的 tools.jsonl")
    ap.add_argument("--sessions-dir", "--output-dir", dest="sessions_dir",
                    help="可读 JSON 输出目录，文件按稳定会话分组命名为 session-*-rl.json")
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
        folder = args.sessions_dir or os.path.join(os.path.dirname(os.path.abspath(__file__)), "audit-logs")
        if launch and not args.sessions_dir:
            folder = os.path.join(folder, time.strftime("%Y%m%d-%H%M%S", time.gmtime()) + "-" + uuid.uuid4().hex[:8])
        args.log = os.path.join(folder, "audit.jsonl")
    tools_path = args.tools_log or os.path.join(os.path.dirname(os.path.abspath(args.log)), "tools.jsonl")
    if os.path.normcase(os.path.abspath(tools_path)) == os.path.normcase(os.path.abspath(args.log)):
        ap.error("--tools-log 不能与 --log 使用同一个文件")
    sessions_dir = args.sessions_dir or os.path.dirname(os.path.abspath(args.log))
    journal_directory = os.path.normcase(os.path.abspath(sessions_dir))
    title_path = os.path.normcase(os.path.abspath(os.path.join(sessions_dir,
                                  SessionJsonWriter.filename(SessionJsonWriter.TITLE_STREAM))))
    for index_path in (args.log, tools_path):
        normalized = os.path.normcase(os.path.abspath(index_path))
        name = os.path.basename(normalized)
        if normalized == title_path:
            ap.error("审计/工具索引不能覆盖 auxiliary/titles.json 辅助正文")
        if (os.path.dirname(normalized) == journal_directory and
                (name == "sessions-rl.jsonl" or re.fullmatch(r"session-.*-rl[.]jsonl?", name))):
            ap.error("审计/工具索引不能使用保留的 session-*-rl.json 正文文件名")
    handler = type("ConfiguredAuditHandler", (AuditHandler,), {})
    handler.routes = routes
    handler.logger = JsonlLogger(args.log, args.log_max_bytes, args.log_backups) if args.indexes or args.raw_log else None
    handler.tool_logger = JsonlLogger(tools_path, args.log_max_bytes, args.log_backups) if args.indexes or args.raw_log else None
    handler.session_writer = SessionJsonWriter(sessions_dir)
    handler.upstream_connect_timeout = args.upstream_connect_timeout
    handler.verbose = args.verbose
    handler.raw_log = args.raw_log
    handler.control_token = secrets.token_urlsafe(32) if launch else None

    port = args.port if args.port is not None else (0 if launch else 8787)
    try:
        server = AuditHTTPServer((args.host, port), handler)
    except OSError:
        if handler.logger:
            handler.logger.close()
        if handler.tool_logger:
            handler.tool_logger.close()
        raise
    server.daemon_threads = True
    print(f"[audit] listening on http://{args.host}:{server.server_address[1]}", file=sys.stderr)
    if handler.logger:
        print(f"[audit] log: {args.log}", file=sys.stderr)
        print(f"[audit] tools: {tools_path}", file=sys.stderr)
    print(f"[audit] sessions: {handler.session_writer.directory}", file=sys.stderr)
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
        handler.session_writer.flush()
        if handler.logger:
            handler.logger.close()
        if handler.tool_logger:
            handler.tool_logger.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
