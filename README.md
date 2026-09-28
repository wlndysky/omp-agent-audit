# omp-agent-audit

本地 LLM API 审计代理：记录 OMP 内置工具、MCP 工具和模型上下文，不修改 agent 源码。

A local LLM API audit proxy for OMP built-in tools, MCP tools, and model context. No agent source changes required.

[中文](#中文) · [English](#english)

## 中文

### 功能

- 单文件 Python，纯标准库，仅监听 `127.0.0.1`。
- 支持 Anthropic Messages、OpenAI-compatible Chat Completions 和 Responses 的 JSON / SSE 报文。
- 记录请求、响应、API 返回的 thinking/reasoning、工具调用参数及后续上下文回传的结果。
- 默认只写一个追加式 `sessions-rl.jsonl`：优先按真实 response ID / previous_response_id 关联，ID 每轮变化且无法关联时按 system → 首条 user 的 CRC32 分组；SHA-256 校验锚点，避免 CRC32 碰撞误合并。请求 ID 不冒充响应 ID。
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

Windows 也可从项目终端调用 `start-audit.cmd` 的完整路径。它与 Python 文件放在同一目录，不切换当前项目目录，并把 `audit.jsonl`、`tools.jsonl` 写入脚本目录的上一级。参数原样传递，例如 `start-audit.cmd -- --continue`；可用环境变量 `OMP_AUDIT_LOG_DIR` 指定其他日志目录。要保留项目上下文，请在项目终端调用，不要直接双击。

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

部分 provider 会登记为 OpenAI、实际通过适配层发送 Anthropic 请求。因此自动路由还会按每次请求的实际协议检查基础路径，而不只依赖模型目录中的 API 类型。日志的 `routing` 字段记录目录类型、实际协议及路径是否调整；手动路由不受此规则影响。

### 增量日志与操作

一键启动默认输出到脚本旁的 `audit-logs/<本次运行ID>/`。Windows 启动脚本默认输出到脚本目录的上一级。

- `sessions-rl.jsonl`：唯一的正文日志，每次交换追加一行 `schema_version: 2` 增量。包含请求、解析后的响应/SSE、ChatML、独立 `thinking` 字段及工具参数/结果。
- `audit.jsonl`：交换 ID、流 ID、状态、耗时和 THINK 字符数等小型索引，不重复保存完整上下文。
- `tools.jsonl`：工具事件索引，通过 `exchange_id` 和 `tool_trace_index` 定位正文，不重复写大段工具结果；已标记的历史重播不再写索引。

每个流首次保存基线，后续只追加相对上一条的 `set/remove/append/splice` 变化，不反复写全量对话，也不整体覆盖累计 JSON。保留原始结构，文件原有字节不改写；`state_sha256` 校验重建结果。每行有 `base_exchange_id`、`response_id` 和明确的 `thinking.present/characters`。无 THINK 的响应如实标为 false，不生成思考内容。

真实 response ID 是响应关联键，不一定是会话 ID。Anthropic/Chat Completions 经常每轮换 ID，因此无法通过响应链关联时用初始上下文 CRC32 分流；所有流都在同一个 JSONL 中。相同 CRC32 但不同 SHA-256 锚点会分开；完全相同的初始上下文仍只是分组线索，不是独立运行或分支的严格身份。

线程和进程共享文件锁。重启按日志重放恢复状态，并按 exchange ID 防止重复导出；后续只读其他写入者新增的尾部。损坏或未写完的末行拒绝覆盖。异常中断的响应保留已捕获内容和断连标记。日志写入发生在响应结束/中断时，不是每个 token 立刻落盘。启动时会重放现有日志，内存保留每个流最后的状态及关联索引。

需要读取完整交换时按需重放，不要重新落盘每轮全量快照：

```python
from omp_audit_proxy import iter_session_records

for exchange in iter_session_records("sessions-rl.jsonl"):
    messages = exchange.get("chatml", {}).get("messages", [])
    # exchange["tool_trace"] contains arguments and results.
    # Assistant messages keep API-visible reasoning in message["thinking"].
```

旧版文件不会自动转换或删除；旧程序不会热更新，部署后须正常重新启动。工具记录不等于执行成功；工具结果必须由客户端回传才可被代理记录。原始交换结构经重放保留，ChatML 仍是有损派生视图。

```bash
python omp_audit_proxy.py --help
python -B -X utf8 test_audit_proxy.py
python -B -X utf8 test_audit_proxy.py --real-omp
```

- `--log` / `--tools-log`：两个索引的位置；`--sessions-dir`：增量正文的位置。
- `--raw-log`：仅显式调试启用，额外保留全量审计及工具 JSONL，文件会明显增大。默认关闭。
- 增量导出失败时，`audit.jsonl` 会带 `session_export_error` 保存该次完整记录作为恢复证据，不会静默丢弃正文。
- `--log-max-bytes` / `--log-backups`：仅轮转两个索引，默认约 64 MiB、5 个备份；增量正文不轮转，以免丢失重建基线。
- `--port`：一键模式自动选择空闲端口，仅代理模式默认 `8787`；`--upstream-connect-timeout` 默认 30 秒。
- 正常按 OMP 自身方式退出，代理随后停止；一键模式的 `Ctrl+C` 保留 OMP 取消当前轮次的语义。

测试全部使用本机合成数据。`--real-omp` 启动真实 OMP 但使用独立临时配置和本机模拟上游，验证 read 工具、路由、配置保留和默认增量输出，不调用付费模型或真实 MCP。工具索引只是检索入口；完整参数和回传结果在增量正文中。

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
- Writes one append-only `sessions-rl.jsonl`. Link by actual response ID / previous_response_id; when IDs change without a link, group by CRC32 of system through first user, checking SHA-256 to separate CRC32 collisions. Request IDs are never used as response IDs.
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

On Windows, invoke the full path to `start-audit.cmd` from your project terminal. Keep it beside the Python file. It preserves the working directory and writes `audit.jsonl` and `tools.jsonl` to the parent of the script directory. Arguments pass through unchanged, e.g. `start-audit.cmd -- --continue`. Set `OMP_AUDIT_LOG_DIR` to override the log directory. Invoke it from the project terminal rather than double-clicking to retain project context.

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

Some provider shims advertise an OpenAI API while sending Anthropic requests. Automatic routes therefore check each request's wire protocol, not only the model catalog hint. The `routing` log field records the catalog API, wire API, and whether the base path changed. Explicit manual routes remain untouched.

### Incremental logs and operation

Launcher output defaults to `audit-logs/<run-id>/` beside the script. The Windows launcher defaults to the script directory's parent.

- `sessions-rl.jsonl`: one append-only payload journal. Each exchange is a schema-v2 delta retaining request/response structures, parsed SSE, ChatML, separate `thinking`, and tool arguments/results.
- `audit.jsonl`: a small exchange index with status, duration, stream ID and THINK presence/character counts; no repeated conversation body.
- `tools.jsonl`: a small tool index. Resolve `exchange_id` and `tool_trace_index` against the journal for full payloads; already-marked context replays do not repeat the index entry.

The first exchange of each stream supplies its baseline. Later lines contain only `set/remove/append/splice` changes from `base_exchange_id`, with a `state_sha256` replay checksum. Existing journal bytes are never rewritten. THINK content remains explicit in ChatML; `thinking.present/characters` describe each captured assistant response without inventing missing reasoning.

Response IDs identify responses, not necessarily conversations. Known current or previous response IDs link exchanges; otherwise CRC32 of the system and first user anchors the stream. SHA-256 disambiguates CRC32 collisions. Identical initial prompts are grouping hints, not strict independent-session or branch identities. All streams share one physical JSONL file.

A file lock serializes threads/processes. Restart replays the journal and restores exchange-ID deduplication. Subsequent writes read only newly appended frames. Corrupt or incomplete tails are rejected without overwriting evidence. Interrupted streams retain captured payloads and interruption markers. A frame is committed at response end/interruption, not after each token. Memory holds the last state per stream and correlation indexes; startup replays the existing journal.

Read complete exchanges on demand without writing full snapshots back to disk:

```python
from omp_audit_proxy import iter_session_records

for exchange in iter_session_records("sessions-rl.jsonl"):
    messages = exchange.get("chatml", {}).get("messages", [])
    # Separate assistant thinking and full tool_trace payloads are preserved.
```

Legacy files are not automatically converted or deleted. A running old proxy does not hot-reload deployed code. Tool calls are not proof of success; results must be returned in client context before the proxy can capture them. Replay retains captured original exchange structures; ChatML remains a lossy derived view.

```bash
python -B -X utf8 test_audit_proxy.py
python -B -X utf8 test_audit_proxy.py --real-omp
```

- `--log` / `--tools-log` locate the small indexes; `--sessions-dir` locates the incremental payload journal.
- `--raw-log` explicitly enables additional full audit/tool dumps for debugging. Off by default.
- On journal export failure, the audit index retains the failed full record with `session_export_error` as recovery evidence.
- `--log-max-bytes` / `--log-backups` rotate only indexes (64 MiB / 5 backups by default). The journal is not rotated: later changes depend on earlier baselines.
- `--port`: automatically selected in launcher mode, `8787` in proxy-only mode; upstream connect timeout defaults to 30 seconds.

Tests use local synthetic fixtures. `--real-omp` additionally drives installed OMP against loopback mock upstreams with temporary isolated configuration, checking read tools, routing, unchanged configuration and incremental output. No paid models or real MCP services are invoked. Tool indexes are pointers; payloads remain in the journal.

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
