# codex-capacity-retry

[English](#english) · [中文](#中文说明)

## English

- [Usage](#usage)
- [Approach](#approach)
- [Development](#development)
- [中文说明](#中文说明)

### Usage

`codex_capacity_retry.py` is a standalone, dependency-free macOS utility for
local ChatGPT/Codex Desktop sessions. It watches for the exact failure:

```text
Selected model is at capacity. Please try a different model.
```

```bash
./codex_capacity_retry.py --interval 2 --all-sessions
```

Run a bounded dry run before enabling submissions:

```bash
./codex_capacity_retry.py --duration 30 --dry-run --process-report
```

Use JSON Lines when another local process will consume the log:

```bash
./codex_capacity_retry.py --interval 2 --all-sessions --json
```

Requirements:

- macOS with ChatGPT Desktop or Codex Desktop running;
- Python 3.9 or newer;
- the same macOS user that owns those processes.

The runtime uses only Python's standard library and the macOS `ps` and `lsof`
commands. It does not read authentication files, session JSONL, or app-server
stdio. Useful controls include `--max-retries`, `--retry-base`,
`--retry-max-backoff`, `--max-sessions-per-cycle`, `--active-priority-seconds`,
`--verbose-unavailable`, `--ledger`, and `--lock`. Run
`./codex_capacity_retry.py --help` for the complete list.

Each `--interval` cycle refreshes live processes and scans sessions, so a
restart or newly opened instance is picked up by the same loop.

The session index is cached by its SQLite/WAL/SHM file signature. New or
changed routes, active sessions, and retry candidates are processed first;
unchanged cold routes are deferred. Each background sweep attempts every
eligible session once, including unavailable sessions, then waits 30 seconds
before the next sweep. The index is also re-read at least every 30 seconds.

Live sessions keep their IPC subscriptions between scans. After the initial
snapshot, the tool applies revision-checked patches and retains only turn
status, errors, and retry settings, discarding message and tool-output bodies.
It resynchronizes after a disconnect, owner change, or missing patch revision.
Up to 64 session connections are retained; additional sessions use one-shot
probes. The App may also send a replacement snapshot during history changes.

`--timeout` defaults to 8 seconds for initial synchronization and retry
submissions. `--interval` is the wait after each scan. Stream failures include
the session ID and stage, with repeated warnings limited to once per 30 seconds.

### Approach

The tool starts from live processes. It uses `lsof` to find `state_*.sqlite`
files and IPC sockets currently opened by ChatGPT/Codex processes, then reads
the session index and connects to the corresponding per-home Unix socket.

For each candidate session it confirms the live owner and snapshot, and only
acts when the newest turn is terminal, contains the exact capacity message,
and the session is not busy. The retry submits an empty input with the native
Desktop capacity-retry trigger, inherits thread settings, and never resends
the previous user prompt.

A small SQLite retry ledger under `~/.codex-capacity-retry/` prevents the same
failed turn from being submitted repeatedly while the App is still updating.
It also applies backoff and stops automatic replay when the transport result
is unknown. The ledger is local retry state; the tool does not edit persisted
App state, thread indexes, or rollout files.

The IPC endpoint and request versions are internal Desktop behavior. Protocol
or ownership checks fail closed, so an App update is reported instead of
blindly sending an uncertain mutation.

### Development

There is no package build step and no runtime dependency. The repository is
deliberately limited to this Python file, this README, and the MIT license.

Before submitting a change, run:

```bash
python3 -m py_compile codex_capacity_retry.py
./codex_capacity_retry.py --duration 1 --dry-run --process-report
```

Keep protocol changes in the single source file, retain process-first
discovery, and do not add machine-specific paths, credentials, or generated
artifacts.

## 中文说明

- [使用说明](#使用说明)
- [方案](#方案)
- [开发说明](#开发说明)
- [English](#english)

### 使用说明

`codex_capacity_retry.py` 是一个单文件、零第三方依赖的 macOS 工具，监控
本机 ChatGPT/Codex Desktop session 是否出现：

```text
Selected model is at capacity. Please try a different model.
```

直接运行单文件：

```bash
./codex_capacity_retry.py --interval 2 --all-sessions
```

建议先用限定时长的 dry-run 检查发现结果：

```bash
./codex_capacity_retry.py --duration 30 --dry-run --process-report
```

需要机器读取日志时使用 JSON Lines：

```bash
./codex_capacity_retry.py --interval 2 --all-sessions --json
```

运行条件：

- macOS 上的 ChatGPT Desktop 或 Codex Desktop 正在运行；
- Python 3.9 或更高版本；
- 当前用户与这些 App 进程的所属用户相同。

运行时只使用 Python 标准库以及 macOS 自带的 `ps`、`lsof`。它不会读取
认证文件、session JSONL 或 app-server 标准输出。常用参数包括
`--max-retries`、`--retry-base`、`--retry-max-backoff`、
`--max-sessions-per-cycle`、`--active-priority-seconds`、
`--verbose-unavailable`、`--ledger` 和 `--lock`；完整参数可执行
`./codex_capacity_retry.py --help` 查看。

每个 `--interval` 周期都会刷新运行中的进程并扫描 session，因此重启或新打开
的实例会在同一个循环中自动接入。

工具根据 SQLite/WAL/SHM 文件签名缓存 session 索引，优先处理新增或变更的
route、活跃 session 和待重试候选；没有变化的冷 session 会延后处理。每 30 秒
重新读取一次索引。后台扫描对范围内的 session 各尝试一次，不可用的 session
也计为本轮已尝试，完成后等待 30 秒再开始下一轮。

活跃 session 在轮次之间保留 IPC 订阅。首次 snapshot 后，通过校验 revision 的
增量补丁更新状态；缓存仅保留 turn 状态、错误和重试设置，丢弃消息正文与工具
输出。断线、owner 变化或补丁版本不连续时重新同步。最多保留 64 个 session
连接，超出部分使用单次探测；App 在历史发生变化时也可能主动发送新 snapshot。

`--timeout` 默认给首次同步和重试提交 8 秒。`--interval` 是每轮扫描完成后的
等待时间。流读取失败会显示 session ID 和出错阶段，相同警告每 30 秒最多输出一次。

### 方案

工具从正在运行的进程出发，通过 `lsof` 寻找 ChatGPT/Codex 进程当前打开的
`state_*.sqlite` 和 IPC socket，再读取 session 索引并连接对应 home 的 Unix
socket。

对每个候选 session，工具会确认实时 owner 和 snapshot；只有最新 turn 已
结束、包含完整的 capacity 错误、且 session 当前不忙时才会执行重试。重试
发送 Desktop 使用的空输入和 capacity-retry trigger，继承原线程设置，不会
重新发送上一条用户输入。

工具在 `~/.codex-capacity-retry/` 下保存很小的 SQLite 重试状态，用于避免
App 尚未更新期间重复提交同一个失败 turn，并记录退避和未知传输结果。它不
修改 App 持久化数据、thread 索引或 rollout 文件。

IPC endpoint 和请求版本属于 Desktop 内部行为，协议或权限检查失败时会停止
发送并输出错误，避免在结果不确定时盲目变更 session。

### 开发说明

项目没有打包步骤，也没有运行时依赖，仓库刻意只保留 Python 单文件、README
和 MIT LICENSE。

提交修改前运行：

```bash
python3 -m py_compile codex_capacity_retry.py
./codex_capacity_retry.py --duration 1 --dry-run --process-report
```

协议修改集中在这个单文件中；请保持从运行中进程发现 session 的逻辑，不要加入
机器特定路径、凭据或生成物。

MIT 许可见 [LICENSE](LICENSE)。
