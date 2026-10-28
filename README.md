# ResourceLock

[![CI / CD](https://github.com/jamesburton/ResourceLock/actions/workflows/publish.yml/badge.svg)](https://github.com/jamesburton/ResourceLock/actions/workflows/publish.yml)
[![NuGet Version](https://img.shields.io/nuget/v/ResourceLock.svg)](https://www.nuget.org/packages/ResourceLock)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Target Framework: net10.0](https://img.shields.io/badge/.NET-10.0-purple.svg)](https://dotnet.microsoft.com/)

Distributed and local hardware resource locking [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) server for AI coding agents (Claude Desktop, Cursor, Antigravity, etc.).

`ResourceLock` coordinates mutually exclusive access to shared local hardware—such as `"CPU"`, `"GPU"`, `"iGPU"`, `"NPU"`—and remote federated machines like `"Strix:CPU"`. It prevents parallel agent swarms from thrashing system resources, competing for GPU VRAM, or colliding during heavy compilations and benchmark runs.

---

## Key Features

- **Zero-Config Agent Execution**: Invoked directly via `dnx ResourceLock` over standard I/O (stdio) MCP transport without prior manual installation.
- **Auto-Spawning Shared Coordinator**: MCP server instances automatically connect to a shared local coordinator daemon via fast OS-native IPC (Named Pipes on Windows, Unix Domain Sockets on Linux/macOS). If the coordinator is not running, it auto-spawns as a detached background daemon.
- **Multi-Tiered Liveness & Orphan Prevention**:
  - **Connection-Drop Cleanup**: Stream disconnection immediately revokes locks and handovers to queued waiters.
  - **PID Liveness Watchdog**: OS-level verification (`Process.GetProcessById`) detects and evicts stale locks from dead processes.
  - **Graceful On-Exit Hooks**: Registers `AppDomain.CurrentDomain.ProcessExit`, `Console.CancelKeyPress`, and `IHostApplicationLifetime` handlers.
  - **Automatic Expiration**: Optional `timeout_seconds` ensures locks are released even if an agent hangs indefinitely.
- **Non-Blocking & Queued Waiting**: Supports immediate return (`wait_seconds: 0`) or asynchronous FIFO waiting queue (`wait_seconds > 0`).
- **Cross-Machine Federation**: Supports remote resources (e.g., `Strix:CPU`, `Server1:GPU`) by forwarding lock requests over HTTP to remote coordinators.
- **NuGet Trusted Publishing**: Packaged as a .NET Tool published via OIDC without persistent API keys, protected by NuGet package prefix reservation `ResourceLock*`.

---

## Why ResourceLock? Empirical Benchmark Evidence

AI coding agents running in parallel swarms frequently collide when competing for shared local compute—such as compiling large codebases, running inference on local GPU/NPU hardware, or running intensive test suites.

Historically, agents coordinate via ad-hoc file locks (e.g., writing `.gpu.lock` and polling in bash loops). We conducted side-by-side empirical benchmarks measuring **agent token consumption**, **turn counts**, and **contention resolution latency** between file-lock polling and `ResourceLock`:

| Metric | Ad-Hoc File Locking (Bash / Polling) | ResourceLock (MCP + Coordinator) | Improvement |
|---|---|---|---|
| **Turn Count** | 5 agent turns | **1 single atomic turn** | **80.0% reduction** |
| **Token Consumption** | 4,181 tokens | **776 tokens** | **81.4% saved** |
| **Concurrency & Accuracy** | High TOCTOU race condition risk | **100% serialized FIFO queue** | Deterministic order |
| **Orphan Recovery** | Dangerous: Crashed agent leaves stale file | **Instant**: Connection drop & PID watchdog auto-release | Zero manual cleanup |
| **Acquisition Mechanism** | Shell sleep loops (`while test -f ...`) | Asynchronous non-blocking queue (`wait_seconds: N`) | Zero compute/IO thrash |

> **Key Takeaway**: File-lock polling burns thousands of context tokens per contention event because the agent must re-evaluate prompt context, history, and bash tool outputs on every loop iteration. `ResourceLock` offloads waiting to the local coordinator daemon via `acquire_lock(wait_seconds: ...)`, returning control to the agent only once the lock is cleanly acquired.

---

## Deployment: `dnx` vs Global Tool

`ResourceLock` can be executed either on-demand via .NET 10 `dnx` or as a pre-installed global .NET tool:

### 1. Zero-Install via `dnx` (Best for Single Sessions & Subagents)
When running a single agent session or a session with occasional subagents, `dnx ResourceLock` is ideal:
- **Zero setup**: Fetched and cached automatically from NuGet.org on first run.
- **Auto-updating**: Always pulls latest published package version without manual maintenance.

```bash
# Launch stdio MCP server on demand
dnx ResourceLock

# Check version
dnx ResourceLock -- -v

# View CLI options
dnx ResourceLock -- --help

# Run standalone coordinator daemon explicitly in a terminal
dnx ResourceLock -- coordinator
```

### 2. Global Tool Installation (Recommended for Heavy Agent Swarms)

> [!WARNING]
> **Preventing Process Storms**:
> When running multiple concurrent agent sessions or swarms (e.g., Claude Desktop, Codex, Copilot, and Antigravity operating simultaneously), each instance launched via `dnx` will independently evaluate NuGet package caches. Under heavy agent concurrency, this creates a **process storm** with CPU and disk I/O contention.
> 
> For frequent local use or multi-agent swarms, install `ResourceLock` globally. The pre-installed binary starts instantaneously with zero package restore overhead.

```bash
# Global installation from NuGet
dotnet tool install -g ResourceLock

# Run directly from system PATH
ResourceLock

# Update to latest release
dotnet tool update -g ResourceLock
```

---

## Quickstart & MCP Configurations

`ResourceLock` runs as an MCP server communicating over standard I/O (`stdio`). Configure your agent environments using the globally installed `ResourceLock` (recommended) or `dnx`:

### Claude Desktop
Add to `%APPDATA%\Claude\claude_desktop_config.json` (Windows) or `~/Library/Application Support/Claude/claude_desktop_config.json` (macOS):

```json
{
  "mcpServers": {
    "ResourceLock": {
      "command": "ResourceLock"
    }
  }
}
```
*(Or `"command": "dnx", "args": ["ResourceLock"]` for zero-install mode).*

### OpenAI Codex
Add to `~/.codex/config.toml`:

```toml
[mcp_servers.ResourceLock]
command = "ResourceLock"
```

### VS Code & GitHub Copilot
Add to `%APPDATA%\Code\User\mcp.json`:

```json
{
  "servers": {
    "ResourceLock": {
      "command": "ResourceLock",
      "type": "stdio"
    }
  }
}
```

### Cursor
Add to `~/.cursor/mcp.json` or `.cursor/mcp.json`:

```json
{
  "mcpServers": {
    "ResourceLock": {
      "command": "ResourceLock"
    }
  }
}
```

### Antigravity (`agy`)
Add to `~/.gemini/config/mcp_config.json`:

```json
{
  "mcpServers": {
    "ResourceLock": {
      "command": "ResourceLock"
    }
  }
}
```

---

## MCP Tools Reference

`ResourceLock` exposes four MCP tools via `Microsoft.Extensions.AI`:

### 1. `acquire_lock`

Acquires an exclusive lock on a specified hardware or compute resource.

**Parameters:**

| Parameter | Type | Required | Default | Description |
|---|---|---|---|---|
| `resource` | `string` | **Yes** | — | Resource name (e.g., `"CPU"`, `"GPU"`, `"NPU"`, or `"Strix:CPU"`). Case-insensitive. |
| `wait_seconds` | `int` | No | `0` | Seconds to wait if contended. `0` returns immediately with contention status; `>0` queues the agent. |
| `timeout_seconds` | `int?` | No | `null` | Maximum seconds to hold the lock before automatic eviction. |
| `reason` | `string?` | No | `null` | Optional description of the task (e.g., `"Compiling LLVM"`, `"Whisper transcription"`). |
| `owner` | `string?` | No | `null` | Sub-owner label for parallel subagents sharing one MCP session. The effective session id becomes `{sessionId}:{owner}`. |

> **Parallel subagents:** subagents sharing one `ResourceLock` process must each pass a unique `owner` (e.g. their lane/agent name) to `acquire_lock`, `release_lock` and `release_all_locks`. Without it they share the same session id.
>
> **Same-session re-acquire:** acquiring a resource your session already holds (same effective session id) no longer silently succeeds. It fails immediately with `held_by_session` populated and a diagnostic advising a unique `owner`. Explicit TTL renewal is a `renew` flag on the engine-level `LockRequest` (default `false`); it is not currently exposed as an MCP tool parameter.

```json
{ "resource": "GPU", "wait_seconds": 30, "owner": "lane-a" }
```

**Success Response (JSON):**

```json
{
  "success": true,
  "resource": "GPU",
  "session_id": "8fa1f46d-318a-44e2-b92c-6334f5d564cf",
  "expires_at": "2026-10-23T21:45:00Z",
  "message": "Lock acquired successfully."
}
```

**Contention Response (JSON):**

```json
{
  "success": false,
  "resource": "GPU",
  "message": "Resource is held by another process.",
  "held_by_session": "a984df12-5811-4770-9883-7182903abdef",
  "held_by_pid": 4821,
  "held_reason": "Training diffusion model",
  "expires_at": "2026-10-23T21:40:00Z"
}
```

---

### 2. `release_lock`

Releases an active lock held by the calling session.

**Parameters:**

| Parameter | Type | Required | Description |
|---|---|---|---|
| `resource` | `string` | **Yes** | Identifier of the resource to release. |
| `owner` | `string?` | No | Must match the `owner` used to acquire. If omitted while a sub-owner holds the lock, the response is a hint naming that sub-owner. |

**Response (JSON):**

```json
{
  "success": true,
  "resource": "GPU",
  "message": "Lock on 'GPU' released."
}
```

---

### 3. `list_locks`

Queries the coordinator for all currently active locks across the system.

**Parameters:** None.

**Response (JSON):**

```json
[
  {
    "resource": "CPU",
    "session_id": "8fa1f46d-318a-44e2-b92c-6334f5d564cf",
    "owner_pid": 10420,
    "acquired_at": "2026-10-23T20:00:00Z",
    "expires_at": null,
    "reason": "Compiling Roslyn solution"
  },
  {
    "resource": "GPU",
    "session_id": "a984df12-5811-4770-9883-7182903abdef",
    "owner_pid": 4821,
    "acquired_at": "2026-10-23T20:15:00Z",
    "expires_at": "2026-10-23T20:45:00Z",
    "reason": "Training diffusion model"
  }
]
```

---

### 4. `release_all_locks`

Releases all locks currently owned by the calling session. Recommended as a cleanup step before an agent terminates.

**Parameters:**

| Parameter | Type | Required | Description |
|---|---|---|---|
| `owner` | `string?` | No | Releases only that sub-owner's locks; omit for the base session. |

On process exit, the base session and all sub-owner locks are released.

**Response (JSON):**

```json
{
  "success": true,
  "released_count": 2,
  "resources": ["CPU", "GPU"]
}
```

---

### Concurrency

Requests on one MCP connection are multiplexed (by `MessageId` over IPC) rather than handled head-of-line. A waiting `acquire_lock` (`wait_seconds > 0`) does not block a concurrent `release_lock` on the same pipe.

---

## Coordinator Architecture & Federation

```
┌────────────────────────┐      ┌────────────────────────┐
│ Claude Desktop Agent   │      │ Cursor / Antigravity   │
│ (ResourceLock stdio)   │      │ (ResourceLock stdio)   │
└───────────┬────────────┘      └───────────┬────────────┘
            │ Local IPC                     │ Local IPC
            │ (Named Pipe / UDS)            │ (Named Pipe / UDS)
            ▼                               ▼
┌────────────────────────────────────────────────────────┐
│             Local Coordinator Daemon                   │
│   - FIFO Waiter Queues & Non-blocking Granting         │
│   - Process Watchdog & Disconnect Cleanup              │
│   - Auto-Spawn Mutex Protection                        │
└───────────────────────────┬────────────────────────────┘
                            │ HTTP (Port 51234)
                            │ Remote Resource: "Strix:CPU"
                            ▼
┌────────────────────────────────────────────────────────┐
│           Remote Host Coordinator (Strix)              │
│   - ASP.NET Core Kestrel Endpoint                      │
│   - Manages Strix local hardware resources             │
└────────────────────────────────────────────────────────┘
```

### Auto-Spawning Coordinator Daemon

When `ResourceLock` starts:
1. It attempts to connect to the active coordinator via the platform's native IPC:
   - **Windows**: Named Pipe `\\.\pipe\ResourceLock-Coordinator-{SanitizedUsername}`.
   - **Linux / macOS**: Unix Domain Socket `~/.resourcelock/coordinator.sock`.
2. If no coordinator is running, it acquires an OS-wide single-instance spawn lock (`~/.resourcelock/spawn.lock`) to prevent race conditions during concurrent agent starts.
3. It spawns `ResourceLock coordinator --daemon` as a detached, windowless background process, then connects with backoff retries.

### Remote Machine Federation

To access resources on remote hardware across the LAN, prefix the resource identifier with the remote hostname or IP address:

```json
{
  "resource": "Strix:CPU",
  "wait_seconds": 10,
  "reason": "Remote LLM inference benchmark"
}
```

The local coordinator parses the `Strix:` prefix and forwards the acquire/release request to `http://strix:51234/api/locks/acquire`. When the local session releases the lock or disconnects, the remote lock is automatically released.

---

## NuGet Trusted Publishing & Prefix Reservation

`ResourceLock` is published to [NuGet.org](https://www.nuget.org/) using passwordless **NuGet Trusted Publishing (OpenID Connect / OIDC)**:

### 1. Package Prefix Reservation
- The package identifier pattern **`ResourceLock*`** is reserved on NuGet.org under package prefix reservation.
- Prefix reservation grants the verified checkmark badge on NuGet and ensures only authorized owners can upload packages with IDs starting with `ResourceLock`.

### 2. GitHub Actions OIDC Trusted Publishing
- The publishing pipeline (`.github/workflows/publish.yml`) uses GitHub Actions OIDC authentication with `id-token: write` permissions.
- In NuGet.org Account Settings > **Trusted Publishing**, a publisher policy links directly to:
  - **Repository**: `ResourceLock`
  - **Workflow**: `publish.yml`
  - **Package Prefix**: `ResourceLock*`
- Releases are triggered automatically by pushing Git tags (e.g., `git tag v0.1.0 && git push origin v0.1.0`) or manually via GitHub `workflow_dispatch`.

---

## Building and Testing

### Prerequisites
- [.NET 10 SDK](https://dotnet.microsoft.com/)

### Build
```bash
dotnet build ResourceLock.sln
```

### Test
```bash
dotnet test ResourceLock.sln
```

### Pack
```bash
dotnet pack ResourceLock/ResourceLock.csproj -c Release
```

---

## License

This project is licensed under the [MIT License](LICENSE).
