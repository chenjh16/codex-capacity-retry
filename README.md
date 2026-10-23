<div align="center">

# codex-capacity-retry

Process-first automatic recovery for local ChatGPT and Codex Desktop sessions.

[![Platform](https://img.shields.io/badge/platform-macOS-111827?style=flat-square)](https://www.apple.com/macos/)
[![Python](https://img.shields.io/badge/python-3.9%2B-3776AB?style=flat-square)](https://www.python.org/)
[![Dependencies](https://img.shields.io/badge/dependencies-stdlib%20only-2F855A?style=flat-square)](https://docs.python.org/3/library/)
[![License](https://img.shields.io/github/license/chenjh16/codex-capacity-retry?style=flat-square)](LICENSE)
[![Last commit](https://img.shields.io/github/last-commit/chenjh16/codex-capacity-retry?style=flat-square)](https://github.com/chenjh16/codex-capacity-retry/commits/main)

[中文说明](#中文说明) · [English](#english)

</div>

---

## 中文说明

### 快速开始

`codex_capacity_retry.py` 是一个单文件、零第三方依赖的 macOS 工具，用于监控
本机 ChatGPT/Codex Desktop session，并自动恢复模型容量错误。

直接运行：

```bash
./codex_capacity_retry.py
```

默认行为等同于 `--interval 2 --all-sessions`：每 2 秒扫描一次，覆盖当前发现的
全部 session。完整参数执行：

```bash
./codex_capacity_retry.py --help
```

建议先执行限定时长的 dry-run：

```bash
./codex_capacity_retry.py --duration 30 --dry-run --process-report
```

需要机器读取日志时使用 JSON Lines：

```bash
./codex_capacity_retry.py --json
```

运行条件：macOS、Python 3.9+、ChatGPT/Codex Desktop 正在运行，并且工具与 App
使用同一个 macOS 用户。一次只能运行一个 monitor；工具使用本地 lock 和 SQLite
ledger 防止重复提交。

### 技术说明

#### 发现与扫描

- 从正在运行的 ChatGPT/Codex 进程出发，通过 `lsof` 找到当前打开的
  `state_*.sqlite` 和 IPC socket，不依赖硬编码的 `CODEX_HOME` 列表。
- 每轮刷新进程、home、session 索引；新实例、重启实例、活跃 session 和候选
  session 优先扫描，冷 session 进行周期性全量轮询。
- 持久化 thread 和打开的 ephemeral side-chat 使用相同的 owner、IPC stream、
  ledger 和 retry 路径。侧边聊天通过 Desktop 本地 DevTools 的
  `data-tab-id="sidechat:<UUID>"` 发现。
- 首次同步后只保留 turn 状态、错误和重试所需设置，并使用带 revision 校验的
  IPC 增量补丁；不读取或上传认证文件、rollout 正文、消息正文或工具输出。
- 多代理 v2 的 `subagent` thread 由父 thread 控制，App-server 不允许外部直接
  注入输入；工具会跳过这类 route，避免把协议拒绝误报成重复 retry 失败。

#### 自动 Retry 类型

| 情况 | 处理方式 |
| --- | --- |
| 普通 turn：`Selected model is at capacity. Please try a different model.` | 发送 Desktop 原生 `capacity_retry_automatic` 空 turn，继承线程设置。 |
| Compact：`Error running remote compact task: ...` | 调用专用 `thread-follower-compact-thread`，不创建普通空 turn。 |
| 新版协议：`policyCapacityUnavailable` | 按 capacity 错误处理。 |
| `Reconnecting... N/5` 且 `willRetry=true`，附带 capacity 错误 | 立即执行原生 stop + continue 恢复。 |
| `stream disconnected before completion: Our servers are currently overloaded...` | 识别为服务器过载 stream 错误；重连中立即 stop + continue，终态失败时执行普通 turn retry。 |
| App-server 入口队列暂时过载、请求尚未被接受 | 对控制面请求做退避重试，不把它误判为新的 session turn。 |
| `direct app-server input is not allowed for multi-agent v2 sub-agents` | 不直接处理；sub-agent 必须由父 thread 控制，工具跳过该 route。 |

重连中的 stop + continue 流程是：

1. 使用 `thread-follower-interrupt-turn`，携带 `expectedTurnId` 和 `mode=system`，
   只停止检测到的当前 turn；
2. 等待该 turn 确认离开 `inProgress`；
3. 使用空输入和 `turnTrigger="reconnect"` 继续；
4. 通过 ledger 保证同一个 turn 只执行一次 stop + continue。

这样可以绕过 App 内部可能持续数分钟的重连等待。由于当前 turn 可能已经执行过
shell 命令、文件修改或其他工具调用，中断后继续仍可能重复部分副作用；工具因此
使用 expected turn ID 和单飞 ledger，避免并发抢占和错停新 turn。

#### 不会盲目重试的情况

额度耗尽、上下文超限、模型不存在、认证失败、普通参数错误、内容或安全策略、
权限、sandbox，以及 `direct app-server input is not allowed for multi-agent v2 sub-agents`
不会被当作 capacity retry。网络流错误由 Codex/App-server
自己的 retry budget 处理；只有明确落入 capacity 恢复路径时才会执行外部 stop +
continue。

#### 日志与状态

- `WAIT app-reconnect`：发现 App 正在进行 capacity 重连。
- `RETRY ... operation=turn mode=stop_continue`：普通 turn 快速恢复。
- `RETRY ... operation=compact`：compact 恢复。
- `unknown_delivery`：IPC 结果不确定时停止继续提交，避免重复任务。

### 开发说明

仓库刻意只保留可执行 Python 文件、README 和 MIT LICENSE，没有第三方运行时依赖、
打包步骤、测试目录或 workflow。

提交前运行：

```bash
python3 -m py_compile codex_capacity_retry.py
git diff --check
./codex_capacity_retry.py --duration 1 --dry-run --process-report
```

## English

### Usage

`codex_capacity_retry.py` is a standalone, dependency-free macOS monitor for local
ChatGPT and Codex Desktop sessions. It discovers live App processes and IPC homes,
then recovers model-capacity failures.

Run it directly:

```bash
./codex_capacity_retry.py
```

The default is equivalent to `--interval 2 --all-sessions`: scan every two seconds
and include all sessions discovered from running processes. Run the following for the
complete option list:

```bash
./codex_capacity_retry.py --help
```

Use a bounded dry-run first:

```bash
./codex_capacity_retry.py --duration 30 --dry-run --process-report
```

Use JSON Lines for another local process:

```bash
./codex_capacity_retry.py --json
```

Requirements: macOS, Python 3.9+, a running ChatGPT/Codex Desktop, and the same macOS
user that owns those processes. One monitor instance is allowed at a time.

### Technical notes

The monitor starts from live ChatGPT/Codex processes and uses `lsof` to discover opened
`state_*.sqlite` files and IPC sockets. It does not use a hardcoded home registry. New
or restarted instances, active sessions, persisted threads, and open ephemeral
side-chats use the same owner, IPC stream, retry ledger, and mutation paths.

It supports ordinary capacity failures, remote compact capacity failures, the newer
`policyCapacityUnavailable` variant, and the stream form
`stream disconnected before completion: Our servers are currently overloaded...`.

When the App publishes a transient `willRetry=true` error with capacity details, the
monitor follows the App's native recovery shape: interrupt the expected turn, wait for
it to stop, and start an empty `turnTrigger="reconnect"` continuation. Terminal ordinary
failures use `capacity_retry_automatic`; compact failures use
`thread-follower-compact-thread`.

Revision-checked IPC patches retain only turn status, errors, and retry settings. The
SQLite ledger deduplicates each `(home, thread, turn)` and records unknown mutation
delivery as a stop condition. An interrupted turn may repeat side effects that already
ran, so the fast path uses an expected turn ID and single-flight ledger but cannot make
external commands inherently idempotent.

Multi-agent v2 sub-agent threads are controlled by their parent thread and reject direct
app-server input. The monitor excludes those routes from retry mutations. This error is
therefore an unsupported direct-retry case; the parent thread remains the only controller.

The monitor deliberately does not retry quota exhaustion, context overflow, missing
models, authentication, policy, permission, sandbox, ordinary invalid-request errors, or
direct input to multi-agent v2 sub-agents.

### Development

The repository contains only the executable Python file, this README, and the MIT
license. It has no third-party runtime dependency or package build step.

```bash
python3 -m py_compile codex_capacity_retry.py
git diff --check
./codex_capacity_retry.py --duration 1 --dry-run --process-report
```

See [LICENSE](LICENSE) for the MIT license.
