# omp-agent-audit

本地 LLM API 审计代理：记录 OMP 内置工具、MCP 工具和模型上下文，不修改 agent 源码。

A local LLM API audit proxy for OMP built-in tools, MCP tools, and model context. No agent source changes required.

[中文](#中文) · [English](#english)

## 中文

### 功能

- 单文件 Python，纯标准库，仅监听 `127.0.0.1`。
- 支持 Anthropic Messages、OpenAI-compatible Chat Completions 和 Responses 的 JSON / SSE 报文。
- 记录请求、响应、API 返回的 thinking/reasoning、工具调用参数及后续上下文回传的结果。
- 按稳定分组写可直接打开的 `session-<分组ID>-rl.json`，逐轮追加新对话、思考全文、工具参数和结果：优先按真实 response ID / previous_response_id 关联，ID 每轮变化且无法关联时按 system → 首条 user 的 CRC32 分组；SHA-256 校验锚点，避免 CRC32 碰撞误合并。请求 ID 不冒充响应 ID。
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

Windows 也可从项目终端调用 `start-audit.cmd` 的完整路径。它与 Python 文件放在同一目录，不切换当前项目目录，并把可读的 `session-<分组ID>-rl.json` 写入脚本目录的上一级，默认不生成两个 JSONL 索引。参数原样传递，例如 `start-audit.cmd -- --continue`；可用环境变量 `OMP_AUDIT_LOG_DIR` 指定其他日志目录。要保留项目上下文，请在项目终端调用，不要直接双击。

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

### 可读 JSON 与增量写入

一键启动默认输出到脚本旁的 `audit-logs/<本次运行ID>/`。Windows 启动脚本默认输出到脚本目录的上一级。

- `session-<分组ID>-rl.json`：真正的、格式化的 JSON 文档，直接打开或 `json.load()` 即可读，不需要重放补丁。
- `turns[].thinking`：该次响应中 API 实际返回的完整思考文本；没有返回时为 `null`，不会编造内容。
- `turns[].tool_calls`：工具调用 ID、名称和完整 `arguments`；`turns[].tool_results`：工具名、参数、完整 `content` 及错误状态。
- `turns[].input_messages`：新增的 system/developer/user 消息；`recovered_messages` 保留尚未记录的历史助手消息和工具结果。
- `.audit-state/`：内部增量证据和文件锁，用于恢复原始交换、重启去重及中断恢复。内部仍使用 JSONL 差分，不是要求用户查看的输出，也不每轮另存全量对话。

每个稳定分组一份 JSON，每轮只追加新内容，已记录的请求历史和工具事件不反复写入。正常追加只替换文件结尾的数组/对象闭合符，保留此前轮次的字节，不整体覆盖累计文档。首次创建和中断修复使用临时文件原子替换。响应结束或中断后落盘，不是每个 token 立即写入；追加的短暂窗口内，外部读取者可能读到未闭合尾部，稍后重读即可。

CRC32 只取 system 与首条 user 的实际内容，忽略内容块上的 `cache_control` 缓存元数据；缓存标记增加、移除或移动不会创建新分组，原始请求仍完整保留。OMP 自动生成标题等独立提示词请求使用自己的分组，因此一次 OMP 运行可能包含主对话 JSON 和标题 JSON。GET/HEAD/OPTIONS 等请求若不含对话、思考或工具事件（例如 `GET /usages` 额度查询），只保存在内部审计证据中，不再生成空会话 JSON。

优先通过真实 response ID / previous_response_id 关联已有分组；无法关联时使用 system + 首条 user 的 CRC32，例如 `session-crc32-1a2b3c4d-rl.json`。已有响应链沿用原文件，无初始上下文时使用带 response ID 的文件名。每轮新 response ID 不会强制新建文件。SHA-256 校验锚点，防止 CRC32 碰撞误合并；相同初始上下文仍只是分组线索，不是独立运行/分支的严格身份。请求 ID 不冒充响应 ID。

直接读取用户可见 JSON：

```python
import json

with open("session-crc32-1a2b3c4d-rl.json", encoding="utf-8") as f:
    document = json.load(f)
for turn in document["turns"]:
    print(turn["thinking"])
    for call in turn["tool_calls"]:
        print(call["tool_name"], call["arguments"])
    for result in turn["tool_results"]:
        print(result["tool_name"], result["content"])
```

内部先持久化增量证据，再追加可读 JSON。重启和跨进程写入使用文件锁并按 exchange ID 去重；缺失/中断的 JSON 可从证据恢复，合法但与证据冲突的人工修改会被拒绝覆盖。不要单独删除或修改 `.audit-state` 的部分内容；要清空一次采集，应在停止代理后一起移走该输出目录里的 JSON 和对应内部状态。启动时重放内部证据，内存保留分组状态；追加前校验已有 JSON，因此读取成本仍随文件增大。

默认不生成 `audit.jsonl` / `tools.jsonl`。这两份是可选的交换/工具事件索引，包含状态、耗时、THINK 字符数及正文定位信息，不是思考或工具结果的正文。仅需直接阅读 JSON 时不必启用；排查问题可显式加 `--indexes`。`--raw-log` 额外保存全量调试日志，默认关闭，开启后会增加重复数据。

`iter_session_records("/path/to/logs")` 可选地重建内部原始交换（包括请求/响应、解析后的 SSE 和 ChatML）；阅读普通 JSON 不需要它。旧版文件不会自动转换或删除。部署后正常重启才会使用新版；旧进程不会热更新。工具结果必须由客户端回传才能被代理记录，工具调用本身不代表执行成功。

```bash
python omp_audit_proxy.py --help
python -B -X utf8 test_audit_proxy.py
python -B -X utf8 test_audit_proxy.py --real-omp
python -B -X utf8 test_readable_json.py
```

- `--sessions-dir` / `--output-dir`：可读 JSON 及内部状态的输出目录。
- `--indexes`：显式启用小型索引；`--log` / `--tools-log` 指定索引路径。仅给 `--log` 时仍兼容使用其父目录作为默认 JSON 输出目录，不自动启用索引。
- `--raw-log`：显式启用全量审计及工具调试日志，默认关闭。
- 导出失败时，启用的审计索引保存带 `session_export_error` 的完整恢复记录；没有索引时写入 `failed-<exchange_id>.json`，并报告错误。
- `--log-max-bytes` / `--log-backups`：仅轮转可选索引，默认约 64 MiB、5 个备份；JSON 正文及内部增量证据不自动轮转。
- `--port`：一键模式自动选择空闲端口，仅代理模式默认 `8787`；`--upstream-connect-timeout` 默认 30 秒。
- 正常按 OMP 自身方式退出，代理随后停止；一键模式的 `Ctrl+C` 保留 OMP 取消当前轮次的语义。

测试使用本机合成数据；`--real-omp` 额外驱动已安装 OMP，使用临时独立配置和回环模拟上游，验证工具、路由、配置保留及可读 JSON，不调用付费模型或真实 MCP。JSON 测试覆盖思考全文、工具参数/结果、历史去重、追加字节保留、中断恢复和跨进程写入。

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
- Writes one directly readable `session-<group-id>-rl.json` per stable group, appending new turns with full thinking and tool payloads. Link by actual response ID / previous_response_id; when IDs change without a link, group by CRC32 of system through first user, checking SHA-256 to separate CRC32 collisions. Request IDs are never used as response IDs.
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

On Windows, invoke the full path to `start-audit.cmd` from your project terminal. Keep it beside the Python file. It preserves the working directory and writes readable `session-<group-id>-rl.json` files to the parent of the script directory; JSONL indexes are disabled by default. Arguments pass through unchanged, e.g. `start-audit.cmd -- --continue`. Set `OMP_AUDIT_LOG_DIR` to override the log directory. Invoke it from the project terminal rather than double-clicking to retain project context.

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

### Readable JSON and incremental writes

Launcher output defaults to `audit-logs/<run-id>/` beside the script. The Windows launcher defaults to the script directory's parent.

- `session-<group-id>-rl.json` is real, formatted JSON. Open it directly or use `json.load()`; no patch replay is needed.
- `turns[].thinking` contains the complete reasoning text actually exposed by that response; absent reasoning is `null` and is never invented.
- `turns[].tool_calls` contains call IDs, tool names and complete `arguments`. `turns[].tool_results` contains names, arguments, full result `content` and error status.
- `turns[].input_messages` contains new system/developer/user messages; `recovered_messages` retains previously unseen assistant history and tool results.
- `.audit-state/` holds internal incremental evidence and locks for exact exchange recovery, restart deduplication and crash repair. It uses JSONL deltas internally, without storing a full conversation snapshot on every turn.

Each group has one JSON document. Only new content and unique tool events are appended. Normal appends replace the closing array/object footer and preserve earlier turn bytes; they do not rewrite the entire accumulated document. First creation and interrupted-write repair use atomic temporary-file replacement. Writes occur at response completion/interruption, not after each token. External readers may briefly observe an incomplete footer during append and should retry.

CRC32 uses system and first-user content, ignoring `cache_control` metadata on content blocks. Adding, removing or moving cache hints does not create a new group; raw requests remain intact. Independent OMP prompts such as automatic title generation have their own groups, so one OMP run may produce a main-conversation JSON and a title JSON. GET/HEAD/OPTIONS requests without conversation content, reasoning or tool events (such as `GET /usages` quota checks) remain in internal evidence without generating empty conversation JSON files.

Known response IDs or previous_response_id link existing groups; otherwise the system and first user form a CRC32 anchor, e.g. `session-crc32-1a2b3c4d-rl.json`. Linked responses keep their original group file; without initial context the filename contains the response ID. A fresh response ID alone does not force a new file. SHA-256 disambiguates CRC32 collisions. Identical initial prompts remain grouping hints, not strict independent-session or branch identities. Request IDs are never treated as response IDs.

Read the public output directly:

```python
import json

with open("session-crc32-1a2b3c4d-rl.json", encoding="utf-8") as f:
    document = json.load(f)
for turn in document["turns"]:
    print(turn["thinking"])
    for call in turn["tool_calls"]:
        print(call["tool_name"], call["arguments"])
    for result in turn["tool_results"]:
        print(result["tool_name"], result["content"])
```

The internal evidence is persisted before the readable JSON. File locks serialize processes; exchange IDs prevent duplicate exports after restart. Missing/interrupted JSON can be repaired from evidence, while valid manual edits that conflict with evidence are refused. Do not partially delete or edit `.audit-state`. To reset a capture, stop the proxy and move the public JSON and matching internal state together. Startup replays internal evidence and retains group state in memory; append validates the existing JSON, so read costs still grow with document size.

`audit.jsonl` and `tools.jsonl` are disabled by default. These optional exchange/tool indexes hold status, duration, THINK counts and payload locations, not the conversation body. They are unnecessary for reading the JSON; enable with `--indexes` for diagnostics. `--raw-log` enables additional full debugging dumps and therefore duplicates data; it is off by default.

`iter_session_records("/path/to/logs")` optionally reconstructs original exchanges, including requests, responses, parsed SSE and ChatML. It is not needed to read public JSON. Legacy files are not automatically converted or deleted. Restart normally after deployment; running old processes do not hot-reload. Tool results must be returned by the client before they are visible to the proxy; a call alone does not prove success.

```bash
python -B -X utf8 test_audit_proxy.py
python -B -X utf8 test_audit_proxy.py --real-omp
python -B -X utf8 test_readable_json.py
```

- `--sessions-dir` / `--output-dir` select the public JSON and internal-state directory.
- `--indexes` enables small indexes; `--log` / `--tools-log` choose their paths. For compatibility, `--log` alone supplies the default output parent directory without enabling indexes.
- `--raw-log` explicitly enables full audit/tool debugging dumps. Off by default.
- On export failure, an enabled audit index retains the failed full record with `session_export_error`; without indexes, `failed-<exchange_id>.json` preserves it and an error is reported.
- `--log-max-bytes` / `--log-backups` rotate only optional indexes (64 MiB / 5 backups by default). Public JSON and internal evidence are not automatically rotated.
- `--port` is automatically selected in launcher mode and defaults to `8787` in proxy-only mode; upstream connect timeout defaults to 30 seconds.

Tests use local synthetic fixtures. `--real-omp` additionally drives installed OMP against loopback mock upstreams with isolated temporary configuration, checking tools, routing, unchanged configuration and readable JSON. No paid models or real MCP services are invoked. JSON regressions cover full thinking, complete tool payloads, deduplication, preservation of earlier bytes, interrupted-write recovery and concurrent processes.

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
