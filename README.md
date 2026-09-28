# omp-agent-audit

本地 LLM API 审计代理：记录 OMP 内置工具、MCP 工具和模型上下文，不修改 agent 源码。

A local LLM API audit proxy for OMP built-in tools, MCP tools, and model context. No agent source changes required.

[中文](#中文) · [English](#english)

## 中文

### 功能

- 单文件 Python，纯标准库，仅监听 `127.0.0.1`。
- 支持 Anthropic Messages、OpenAI-compatible Chat Completions 和 Responses 的 JSON / SSE 报文。
- 记录请求、响应、API 返回的 thinking/reasoning、工具调用参数及后续上下文回传的结果。
- 输出派生 ChatML 视图和独立工具轨迹，记录上游响应/请求 ID；无 ID 时使用 system + 首条 user 的哈希作为分组提示。
- 从请求历史恢复 OMP/MCP 工具名、参数和结果；重复历史会标记，无法确定的原始调用来源保留为空。
- 不内置个人模型、上游地址、API Key 或 agent 配置。

### 快速开始

需要 Python 3.10+ 和已安装、可在命令行运行的 OMP。Python 无需第三方依赖。

```bash
git clone https://github.com/wlndysky/omp-agent-audit.git
cd omp-agent-audit
python omp_audit_proxy.py
```

这一条命令会自动选择空闲端口、启动代理、生成临时扩展，再启动 OMP。扩展读取 OMP 当前有效的模型目录，仅在此次进程内覆盖上游地址，复用原来的模型和凭据；不修改 `models.yml`、不安装持久扩展、不修改父进程环境。其他已经打开的 OMP 窗口以及之后直接运行的 `omp` 不会自动使用该代理。

正常退出 OMP 后，启动器会关闭代理并删除临时扩展。OMP 自身仍按平常方式保存会话、使用记录等；这不是一个禁写沙箱。进程被强制结束时，临时文件的清理不保证执行。

需要给 OMP 传入参数，在 `--` 后照常填写：

```bash
python omp_audit_proxy.py -- --continue
python omp_audit_proxy.py -- --model "provider/model-id"
```

`--omp-executable` 可指定 OMP 可执行文件完整路径。脚本可以放在 NAS 共享目录，但它在哪台电脑执行，就在哪台电脑启动代理和 OMP。

### 仅代理模式（可选）

如果希望自行管理客户端连接，原来的显式路由方式仍然可用：

```bash
python omp_audit_proxy.py --proxy-only --route "/api/=https://upstream.example/v1=openai-completions"
```

`upstream.example` 是占位地址，需要替换成真实上游。只有这种仅代理方式才需要手动调整客户端地址。

在 OMP 的对应 provider 配置中，**只将 `baseUrl` 换成 `http://127.0.0.1:8787/api`**；保留原来的模型 ID、API 类型和认证设置。此例中，请求 `/api/chat/completions` 会转发到上游 `/v1/chat/completions`。

OMP 仍负责执行内置工具和调用 MCP；代理记录它们在模型请求/响应中的表示，不执行工具。只有经过代理的流量才会被记录。

### 路由

格式：`--route /本地前缀/=上游基础地址=协议`。可重复传入，路径前缀最长匹配优先。

| 协议 | 路由示例 | 客户端 baseUrl |
| --- | --- | --- |
| Chat Completions | `/chat/=https://upstream.example/v1=openai-completions` | `http://127.0.0.1:8787/chat` |
| Anthropic Messages | `/messages/=https://upstream.example=anthropic-messages` | `http://127.0.0.1:8787/messages` |
| Responses | `/responses/=https://upstream.example/v1=openai-responses` | `http://127.0.0.1:8787/responses` |

手动 `--route` 只替换前缀，不会自动去重上游路径中的 `/v1`。请按原 provider 的实际请求路径设置上游基础地址。不要把凭据放进路由 URL。

一键启动会复用 OMP 的 Anthropic 地址规则：先移除基础地址末尾的 `/v1`，再转发客户端追加的 `/v1/messages`，避免重复拼接。Chat Completions 和 Responses 的 `/v1` 基础路径保持不变。

### 日志与操作

一键启动默认输出到脚本旁的 `audit-logs/<本次运行ID>/`，各次运行隔离；仅代理模式默认使用 `audit-logs/`：

- `audit.jsonl`：每次 HTTP 交换的请求/响应、解析后的 SSE 事件、ID 分类、ChatML 和工具轨迹。
- `tools.jsonl`：每条工具事件单独一行，便于检索工具名、参数、结果和跨轮关联。

工具事件的 `event` 为 `tool_call`（当前响应中的调用）、`tool_call_context`（历史调用）或 `tool_result`（上下文回传的结果）。调用记录不等于执行成功。`replayed_context` 标记重复结果；`call_exchange_ambiguous` 表示原始交换来源有歧义。`tool_kind_hint` 仅按名称分类，不是身份认证；错误状态未报告时 `is_error` 为 `null`，不擅自判定成功。

日志保持追加写入，不回写旧记录，也不为每个 response ID 单独建文件。跨请求来源缓存有界且仅在内存中；代理重启后仍可从请求历史恢复工具内容，但不会虚构原始 exchange ID。

```bash
python omp_audit_proxy.py --help
python -B -X utf8 test_audit_proxy.py
```

- `--port`：一键启动默认选择空闲端口，仅代理模式默认 `8787`。
- 一键模式下按 OMP 自身方式退出，代理随后停止；`Ctrl+C` 保留 OMP 的取消当前轮次语义。仅代理模式用 `Ctrl+C` 停止。
- `--log` / `--tools-log`：分别指定完整审计和工具轨迹文件。
- `--log-max-bytes` / `--log-backups`：默认每个日志约 64 MiB 后轮转，保留 5 个备份；不是永久归档。
- `--upstream-connect-timeout`：默认 30 秒；响应开始后取消该连接的 socket 读超时。

默认测试使用本机临时端口和模拟上游，不需要 API Key，不调用真实模型或真实 MCP 服务。加 `--real-omp` 会额外启动已安装的真实 OMP，使用临时独立配置和本机假上游，验证一次真实 read 工具调用、配置未被重写，以及不安全路由在推理前停止。不代表已验证所有 provider、Vibe 子代理路径或用户的完整 agent 环境。

### 隐私与限制

- 自动路由只覆盖上述三类 API，且要求同一 provider 的聊天模型使用同一上游地址和 API。其他或混合端点 provider 会明确提示未覆盖；启动时所选模型无法安全路由则停止，不会悄悄直接调用。运行中切换到未覆盖的 provider 不会自动获得审计覆盖。
- 只对指定认证头、Cookie 和敏感查询参数脱敏。**正文中的密码、源码、Flag、文件路径、工具输出等仍可能原样出现，分享前必须审查。**
- `.gitignore` 排除日志、缓存和常见本地配置，但不能清除已经提交的文件或 Git 历史。
- 使用正常的 TLS 证书校验，仅允许回环绑定；回环绑定不能阻止其他本机进程访问。
- 日志尝试设置 POSIX `0600` 权限；Windows 需另外配置文件系统 ACL。
- 工具结果要等客户端在后续模型请求中回传后才能记录；未回传的本地执行细节不在 HTTP 审计范围内。只记录 API 实际返回的推理内容。
- ChatML 是有损派生视图；SSE 事件经过 JSON 解析，不是逐字节取证副本。
- 不支持 WebSocket 审计或压缩请求体解码。压缩请求以 base64 保留；需要结构化审计时，在客户端关闭请求压缩。向上游发送 `Accept-Encoding: identity`。
- 正文和 SSE 事件日志有截断阈值；检查截断、断连标记，不应视为无限量、不可丢失的归档。

## English

### Features

- One Python file, standard library only, bound to `127.0.0.1`.
- JSON and SSE handling for Anthropic Messages, OpenAI-compatible Chat Completions, and Responses.
- Captures requests, responses, API-exposed thinking/reasoning, tool arguments, and results returned in subsequent model context.
- Produces a derived ChatML view and a separate tool-event log. Records carry response/request IDs; a system + first-user hash is a low-confidence grouping hint when no upstream ID exists.
- Recovers OMP/MCP tool names, arguments, and results from request history, including after a proxy restart. Repeated context is marked; ambiguous origins are not guessed.
- No personal models, upstream endpoints, API keys, or agent configuration are bundled.

### Quick start

Python 3.10+ and an installed OMP executable on PATH are required. No third-party Python dependencies.

```bash
git clone https://github.com/wlndysky/omp-agent-audit.git
cd omp-agent-audit
python omp_audit_proxy.py
```

This selects a free local port, starts the proxy, creates a temporary extension, and launches OMP. The extension reads OMP's effective model catalog and overrides provider URLs only in this process, keeping the existing models and credentials. It does not edit `models.yml`, install a persistent extension, or change the parent environment. Other OMP windows and later plain `omp` launches do not inherit the proxy.

When OMP exits normally, the launcher stops the proxy and removes the temporary extension. OMP itself still saves sessions and usage information as usual; this is not a no-write sandbox. Cleanup is not guaranteed if the process is forcibly killed.

Pass OMP arguments after `--`:

```bash
python omp_audit_proxy.py -- --continue
python omp_audit_proxy.py -- --model "provider/model-id"
```

Use `--omp-executable` for an explicit executable path. The script may live on a NAS share; the proxy and OMP run on the machine executing it.

### Proxy-only mode (optional)

Explicit routes remain available if you prefer managing the client connection yourself:

```bash
python omp_audit_proxy.py --proxy-only --route "/api/=https://upstream.example/v1=openai-completions"
```

Replace `upstream.example` with your actual upstream. Only this proxy-only mode requires manually changing the client endpoint.

For the corresponding OMP provider, change **only `baseUrl` to `http://127.0.0.1:8787/api`**. Keep its model ID, API type, and authentication settings unchanged. In this example, `/api/chat/completions` is forwarded to `/v1/chat/completions` upstream.

OMP still executes built-in tools and calls MCP servers. This proxy observes their representation in model traffic; it does not execute tools. Traffic that bypasses the proxy is not captured.

### Routes

Syntax: `--route /local-prefix/=upstream-base=protocol`. Repeat the option for multiple routes; the longest matching prefix wins.

| Protocol | Example route | Client baseUrl |
| --- | --- | --- |
| Chat Completions | `/chat/=https://upstream.example/v1=openai-completions` | `http://127.0.0.1:8787/chat` |
| Anthropic Messages | `/messages/=https://upstream.example=anthropic-messages` | `http://127.0.0.1:8787/messages` |
| Responses | `/responses/=https://upstream.example/v1=openai-responses` | `http://127.0.0.1:8787/responses` |

Explicit `--route` entries only replace the local prefix and do not deduplicate `/v1`; match your provider's actual request path when choosing the upstream base. Do not put credentials in route URLs.

One-command launches mirror OMP's Anthropic URL normalization: remove a trailing `/v1` from the base before forwarding the client's `/v1/messages` suffix. Chat Completions and Responses retain their `/v1` base paths.

### Logs and operation

One-command launches write to `audit-logs/<run-id>/` beside the script, isolating each run. Proxy-only mode defaults to `audit-logs/`:

- `audit.jsonl`: HTTP exchanges, parsed SSE events, ID classification, derived ChatML, and tool traces.
- `tools.jsonl`: one tool event per line, including names, arguments, results, and correlation fields.

Event types are `tool_call` (current response), `tool_call_context` (historical call), and `tool_result` (result echoed in context). An observed call is not proof of successful execution. `replayed_context` marks repeated results; `call_exchange_ambiguous` flags uncertain origins. `tool_kind_hint` is name-based, not an authenticated identity. An unreported error status is `null`, not assumed success.

Logs are append-only. Classification fields do not create separate files for each response ID. The cross-request cache is bounded and in-memory; after restart, context can recover tool contents but not missing original exchange IDs.

```bash
python omp_audit_proxy.py --help
python -B -X utf8 test_audit_proxy.py
```

- `--port` selects a free port in launcher mode and defaults to `8787` in proxy-only mode.
- Exit OMP normally to stop its proxy. In launcher mode, `Ctrl+C` retains OMP's cancel-turn behavior; in proxy-only mode it stops the proxy.
- `--log` and `--tools-log` select the two output files.
- `--log-max-bytes` and `--log-backups` default to rotation after approximately 64 MiB per file and five backups. Logs are not permanent archives.
- `--upstream-connect-timeout` defaults to 30 seconds; the socket read timeout is removed once the response starts.

Default tests use temporary loopback ports and a mock upstream, with no API key or real model/MCP server calls. Add `--real-omp` to exercise the installed OMP against an isolated temporary configuration and a local fake provider: it performs a real read tool call, checks that model configuration is unchanged, and verifies unsafe routing stops before inference. Passing does not certify every provider, Vibe subagent path, or a user's full deployment.

### Privacy and limitations

- Automatic routing covers the three APIs above and requires a uniform chat-model endpoint and API within each provider. Unsupported or mixed-endpoint providers are reported as uncovered. An unsupported selection at startup stops the child instead of silently calling it directly. Switching to an uncovered provider later does not automatically add audit coverage.
- Selected authentication headers, cookies, and sensitive query parameters are redacted. **Bodies may still contain passwords, source code, flags, paths, and tool output. Review logs before sharing.**
- `.gitignore` excludes logs, caches, and common local configuration; it cannot erase tracked files or existing Git history.
- Normal TLS certificate verification is used and non-loopback binding is refused. Loopback binding is not access control against other local processes.
- Log files attempt POSIX mode `0600`; configure filesystem ACLs separately on Windows.
- Tool results are visible only after the client sends them back in model context. Local-only execution details are outside HTTP auditing. Only reasoning actually exposed by the API is recorded.
- ChatML is a lossy view; SSE events are parsed, not byte-for-byte forensic copies.
- WebSocket auditing and compressed-request decoding are unsupported. Compressed requests are retained as base64; disable client request compression for structured capture. Upstream requests use `Accept-Encoding: identity`.
- Body and SSE event logs have truncation thresholds. Check truncation/disconnect markers; do not assume unlimited or lossless archival.
