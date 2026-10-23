# Architectural Specification: ResourceLock

**Date:** 2026-10-23  
**Status:** Approved  
**Target Framework:** .NET 10.0  
**Package / Tool Name:** `ResourceLock`  
**Execution Command:** `dnx ResourceLock` / `dotnet tool run ResourceLock`  

---

## 1. Overview & Goals

`ResourceLock` is a .NET 10 global tool and library providing Model Context Protocol (MCP) tools for AI agents (Claude Desktop, Cursor, Antigravity, etc.) to coordinate access to shared hardware resources—such as `"CPU"`, `"GPU"`, `"iGPU"`, `"NPU"`, and remote machines like `"Strix:CPU"`.

### Key Capabilities
1. **Zero-Config Agent Execution**: Invoked directly via `dnx ResourceLock` over standard I/O (stdio) MCP transport.
2. **Auto-Spawning Shared Coordinator**: When the MCP tool starts, it connects to a shared local coordinator daemon via fast local IPC (Named Pipes on Windows, Unix Domain Sockets on Linux/macOS). If the coordinator is not running, it auto-spawns it as a detached background daemon.
3. **Robust Dead-Process & Orphan Cleanup**:
   - Immediate release on MCP IPC connection drops.
   - PID tracking and OS-level liveness verification (`Process.GetProcessById`).
   - Clean on-exit disposal hooks (`AppDomain.ProcessExit`, `Console.CancelKeyPress`, `IHostApplicationLifetime`).
   - Optional configurable lock hold timeouts (`timeout_seconds`).
4. **Non-Blocking & Queued Waiting**: Supports immediate return or blocking until acquired with a configurable `wait_seconds` parameter.
5. **Cross-Machine Federation**: Supports remote resources (e.g. `Strix:CPU`) where the local coordinator forwards lock acquisition/release over HTTP to a coordinator on the remote machine.
6. **Deferred Phase**: mDNS coordinator auto-discovery across local network subnets.
7. **NuGet Distribution**: Packaged as a .NET tool with NuGet Trusted Publishing configured for package prefix `ResourceLock*`.

---

## 2. Solution Structure & Repository Layout

Per project constraints, all project folders are situated directly in the solution root without any `src/` directory.

```text
ResourceLock/
├── .gitignore
├── ResourceLock.sln
├── ResourceLock.Core/             # Class library: domain models, lock engine, IPC contracts
│   ├── Enums/
│   ├── Models/                    # LockRecord, LockRequest, LockResult, SessionInfo
│   ├── Engine/                    # ILockEngine, InMemoryLockEngine
│   ├── Ipc/                       # IIpcProtocol, ProtocolConstants, FrameCodec
│   └── ResourceLock.Core.csproj
├── ResourceLock.Coordinator/      # Class library: Coordinator daemon & transport endpoints
│   ├── Background/                # Daemon process runner & single-instance mutex
│   ├── Transports/
│   │   ├── NamedPipes/            # Windows Named Pipe server stream handler
│   │   ├── UnixSockets/           # Linux/macOS Unix Domain Socket handler
│   │   └── Http/                  # ASP.NET Core Kestrel minimal API for remote federation
│   ├── Federation/                # Remote coordinator client (e.g. Strix:CPU)
│   └── ResourceLock.Coordinator.csproj
├── ResourceLock/                  # Executable (.NET Tool): MCP Server & CLI entry point
│   ├── Commands/                  # 'mcp' (default stdio) and 'coordinator --daemon' commands
│   ├── Mcp/                       # MCP Server setup using Microsoft Agent Framework / Extensions
│   ├── Tools/                     # acquire_lock, release_lock, list_locks, release_all_locks
│   ├── Hooks/                     # ProcessExit & CancelKeyPress cleanup handlers
│   ├── Program.cs
│   └── ResourceLock.csproj        # <PackAsTool>true</PackAsTool>
└── ResourceLock.Tests/            # xUnit tests for engine, IPC, cleanup, and MCP tools
    ├── EngineTests.cs
    ├── LivenessTests.cs
    ├── ConcurrencyTests.cs
    ├── SerializationTests.cs
    └── ResourceLock.Tests.csproj
```

---

## 3. Coordinator Lifecycle & IPC Protocol

### 3.1 IPC Endpoints
* **Windows**: Named Pipe `\\.\pipe\ResourceLock-Coordinator-{SanitizedUsername}`.
* **Linux / macOS**: Unix Domain Socket at `~/.resourcelock/coordinator.sock` (with permissions `0600`).

### 3.2 Auto-Spawn Daemon Mechanism
When `dnx ResourceLock` starts without arguments:
1. It attempts to connect to the local IPC endpoint.
2. If the connection fails (endpoint does not exist or server not listening):
   - Acquires a local file/mutex lock (`~/.resourcelock/spawn.lock`) to avoid spawning races when multiple agents launch simultaneously.
   - Launches a detached background process:
     `Environment.ProcessPath coordinator --daemon`
     - Uses `ProcessStartInfo` with `UseShellExecute = false`, `CreateNoWindow = true`, standard handles redirected/discarded.
   - Releases the spawn lock.
   - Retries IPC connection with exponential backoff (up to 3 seconds).
3. Once connected, registers the current MCP session and transmits the agent process PID (`Environment.ProcessId`).

### 3.3 Remote Network Federation & Cross-Machine Coordination
* Coordinator runs an optional HTTP listener via ASP.NET Core Kestrel on port `51234` (configurable).
* Resources follow the format `[<host>:]<resource-name>`:
  - Local resource: `"CPU"`, `"GPU"`, `"NPU"`.
  - Remote resource: `"Strix:CPU"`, `"Server1:GPU"`.
* When a local agent requests `"Strix:CPU"`:
  1. The local coordinator parses host `"Strix"` and resource `"CPU"`.
  2. The local coordinator issues an HTTP POST request to `http://strix:51234/api/locks/acquire` with the request payload.
  3. Returns the remote coordinator's response back to the local agent.
  4. Releases or disconnects on the local agent automatically propagate a release call to the remote coordinator.

### 3.4 Deferred Task: mDNS Discovery
* **Phase 2 (Deferred)**: Implement Zeroconf / mDNS service announcement (`_resourcelock._tcp`) so local coordinators automatically broadcast their presence and resolve machine names across local networks without hardcoded DNS/IPs.

---

## 4. Locking Domain Engine & Liveness

### 4.1 State Models
```csharp
public sealed record LockRecord(
    string Resource,
    string SessionId,
    int OwnerPid,
    DateTimeOffset AcquiredAt,
    DateTimeOffset? ExpiresAt,
    string? Reason);

public sealed record LockRequest(
    string Resource,
    string SessionId,
    int OwnerPid,
    int WaitSeconds = 0,
    int? TimeoutSeconds = null,
    string? Reason = null);

public sealed record LockResult(
    bool Success,
    string Resource,
    string? SessionId,
    int? OwnerPid,
    string? Message,
    DateTimeOffset? ExpiresAt,
    LockRecord? CurrentHolder = null);
```

### 4.2 Wait Queue & Asynchronous Granting
* An in-memory queue (`PriorityQueue` or FIFO `Channel<WaiterRegistration>`) is maintained per resource.
* When `wait_seconds > 0` is requested:
  - An asynchronous waiter is placed in the resource's wait queue backed by a `TaskCompletionSource<LockResult>`.
  - A timeout cancellation token is bound to `wait_seconds`.
  - If the lock is freed or expired before the timeout, the waiter is granted the lock immediately without polling.
  - If the timeout fires first, the waiter is evicted from the queue and returns `Success: false` with current holder metadata.

### 4.3 Multi-Tiered Liveness & Orphan Prevention
1. **Graceful On-Exit Hook**:
   - `ResourceLock` registers handlers on:
     - `AppDomain.CurrentDomain.ProcessExit`
     - `Console.CancelKeyPress`
     - `IHostApplicationLifetime.ApplicationStopping`
   - Invokes `release_all_locks` synchronously via IPC before the client process terminates.
2. **IPC Connection Drop (Primary Watchdog)**:
   - The coordinator maintains an open bidirectional stream with each active MCP client.
   - If the client terminates, crashes, or is killed (`SIGKILL` / `Taskkill`), the stream breaks (`EndOfStreamException`).
   - The coordinator immediately cleans up all locks associated with that connection stream and signals the next waiters in the queue.
3. **PID Validation (Secondary Watchdog)**:
   - When contention occurs or during periodic coordinator sweeps (every 10s):
   - The coordinator queries `Process.GetProcessById(ownerPid)`.
   - If the process is dead (`ArgumentException` or `HasExited == true`), the stale lock is evicted immediately.
4. **Configurable Lock Timeout (`TimeoutSeconds`)**:
   - If set, the coordinator starts an internal timer for the lock.
   - Upon timer expiry, the lock is automatically revoked and granted to the next queued session.

---

## 5. MCP Server & Microsoft Agent Framework Tool Surface

### 5.1 Protocol Transport
* The MCP server runs over standard I/O (`stdin`/`stdout`) using standard JSON-RPC formatting compliant with the Model Context Protocol specification.
* Integrates using Microsoft Agent Framework / `Microsoft.Extensions.AI` tool definitions.

### 5.2 Exposed MCP Tools

#### 1. `acquire_lock`
* **Description**: Acquires an exclusive lock on a specified resource (e.g., `"CPU"`, `"GPU"`, `"NPU"`, `"Strix:CPU"`).
* **Parameters**:
  - `resource` *(string, required)*: Identifier of the resource.
  - `wait_seconds` *(int, optional, default: 0)*: Maximum seconds to block waiting if the resource is currently locked. Set to 0 for non-blocking.
  - `timeout_seconds` *(int, optional, default: null)*: Max duration to hold the lock before automatic release.
  - `reason` *(string, optional)*: Short explanation of work being performed.
* **Returns**:
  ```json
  {
    "success": true,
    "resource": "GPU",
    "session_id": "c1f7b09e-...",
    "expires_at": "2026-10-23T20:30:00Z",
    "message": "Lock acquired successfully."
  }
  ```
  Or on contention:
  ```json
  {
    "success": false,
    "resource": "GPU",
    "message": "Resource is held by another process.",
    "held_by_session": "a984df...",
    "held_by_pid": 4821,
    "held_reason": "Training model",
    "expires_at": "2026-10-23T20:25:00Z"
  }
  ```

#### 2. `release_lock`
* **Description**: Releases a previously acquired lock on a resource held by the current session.
* **Parameters**:
  - `resource` *(string, required)*: Identifier of the resource.
* **Returns**:
  ```json
  {
    "success": true,
    "resource": "GPU",
    "message": "Lock on 'GPU' released."
  }
  ```

#### 3. `list_locks`
* **Description**: Lists all currently held locks across the system with their owner session, PID, acquisition time, and reason.
* **Parameters**: *(none)*
* **Returns**:
  ```json
  [
    {
      "resource": "CPU",
      "session_id": "c1f7b09e-...",
      "owner_pid": 10420,
      "acquired_at": "2026-10-23T20:00:00Z",
      "expires_at": null,
      "reason": "Compiling LLVM"
    }
  ]
  ```

#### 4. `release_all_locks`
* **Description**: Releases all locks currently held by this session. Recommended before finishing work.
* **Parameters**: *(none)*
* **Returns**:
  ```json
  {
    "success": true,
    "released_count": 2,
    "resources": ["CPU", "GPU"]
  }
  ```

---

## 6. Packaging & NuGet Trusted Publishing

### 6.1 Tool Package Configuration (`ResourceLock.csproj`)
```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <TargetFramework>net10.0</TargetFramework>
    <ImplicitUsings>enable</ImplicitUsings>
    <Nullable>enable</Nullable>
    <PackAsTool>true</PackAsTool>
    <ToolCommandName>ResourceLock</ToolCommandName>
    <PackageId>ResourceLock</PackageId>
    <Authors>YourName</Authors>
    <Description>Distributed and local resource locking MCP server for AI agents.</Description>
    <PackageLicenseExpression>MIT</PackageLicenseExpression>
  </PropertyGroup>
  <ItemGroup>
    <ProjectReference Include="..\ResourceLock.Core\ResourceLock.Core.csproj" />
    <ProjectReference Include="..\ResourceLock.Coordinator\ResourceLock.Coordinator.csproj" />
  </ItemGroup>
</Project>
```

### 6.2 NuGet Trusted Publishing Instructions
1. **Prefix Reservation**:
   - In your NuGet.org account, request prefix reservation for **`ResourceLock*`**.
2. **Trusted Publishing (OIDC)**:
   - On NuGet.org, create a new Trusted Publisher linked to your GitHub Repository:
     - Owner: `<GitHub-Org-or-Username>`
     - Repository: `ResourceLock`
     - Workflow filename: `publish.yml`
     - Package pattern: `ResourceLock*`
3. **GitHub Actions Workflow (`.github/workflows/publish.yml`)**:
   - Uses `dotnet pack` and `dotnet nuget push` without static API keys, relying on GitHub Actions OIDC environment tokens.

---

## 7. Testing Strategy

1. **Unit Tests (`ResourceLock.Tests`)**:
   - `InMemoryLockEngine`: Verify exclusive access, conflict rejections, timeout auto-expirations, FIFO waiter queue ordering, and `ReleaseAll`.
   - `ProtocolCodec`: Serialize/deserialize requests and responses over simulated streams.
2. **Integration Tests**:
   - `NamedPipe / Socket IPC`: Start coordinator in-process, connect multiple clients simultaneously, test lock contention and wait queue handover.
   - `Connection-Drop Cleanup`: Connect a client, acquire locks, forcibly close the client stream, and assert that the coordinator frees the locks and grants them to waiting sessions.
   - `PID Dead Process Eviction`: Acquire lock with a simulated non-existent PID, attempt acquire from another session, verify coordinator evicts stale lock and succeeds.
3. **MCP Tool Invocation Tests**:
   - Test `acquire_lock`, `release_lock`, `list_locks`, `release_all_locks` against MCP tool handler definitions.
