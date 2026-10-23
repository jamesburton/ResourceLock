# ResourceLock Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build and publish a .NET 10 global tool (`ResourceLock`, executable directly via `dnx ResourceLock`) providing MCP tools for agents to acquire and release exclusive locks on shared resources (`CPU`, `GPU`, `iGPU`, `NPU`, `Strix:CPU`) via a shared background coordinator daemon.

**Architecture:** A modular solution in the repository root containing `ResourceLock.Core` (locking state engine, IPC contracts), `ResourceLock.Coordinator` (Named Pipe / Unix Domain Socket server + Kestrel HTTP for remote federation), `ResourceLock` (CLI entry point, auto-spawning orchestrator, MCP stdio server with exit hooks), and `ResourceLock.Tests`.

**Tech Stack:** .NET 10.0, C# 13/14, Microsoft Agent Framework / `Microsoft.Extensions.AI`, ASP.NET Core Kestrel, System.IO.Pipes, System.Net.Sockets, xUnit.

**Spec:** [docs/superpowers/specs/2026-10-23-resourcelock-design.md](file:///c:/Development/ResourceLock/docs/superpowers/specs/2026-10-23-resourcelock-design.md)

## Global Constraints

- Target framework is `net10.0`.
- All projects placed directly in the solution root without a `src/` directory.
- `ResourceLock` configured with `<PackAsTool>true</PackAsTool>` and executable via `dnx ResourceLock`.
- NuGet prefix reservation pattern and Trusted Publishing workflow configured for `ResourceLock*`.
- Lock tracking must record session ID, owner PID, UTC acquisition time, optional expiration timestamp, and optional reason.
- On client disconnection or PID death, held locks must be automatically evicted and granted to waiting sessions.
- Process exit hooks (`AppDomain.ProcessExit`, `Console.CancelKeyPress`) must invoke `release_all_locks` on disposal.

## Review Focus

1. **Dead process with no disconnect**: A process holding a lock terminates abruptly without closing the pipe; when a second agent requests the lock, the coordinator checks PID liveness, evicts the dead lock, and grants it to the second agent.
2. **Concurrent daemon spawn race**: Two agent instances start at the exact same millisecond when no coordinator is running; local file lock ensures only one daemon starts and both agents connect successfully.
3. **Queue timeout expiration under contention**: An agent requests a locked resource with `wait_seconds = 2`; if not freed within 2 seconds, the request aborts with `success: false` and the holder's details without remaining stuck in the queue.
4. **Remote host failure**: Requesting `Strix:CPU` when host `Strix` is unreachable returns a clean error payload rather than crashing the local MCP session.
5. **Case-insensitive resource normalization**: Requesting `"cpu"`, `"CPU"`, and `"Cpu"` resolves to the same resource lock.

---

### Task 1: Solution Scaffolding & Core Models

**Files:**
- Create: `ResourceLock.sln`
- Create: `ResourceLock.Core/ResourceLock.Core.csproj`
- Create: `ResourceLock.Core/Models/LockRecord.cs`
- Create: `ResourceLock.Core/Models/LockRequest.cs`
- Create: `ResourceLock.Core/Models/LockResult.cs`
- Create: `ResourceLock.Core/Models/SessionInfo.cs`
- Create: `ResourceLock.Tests/ResourceLock.Tests.csproj`
- Create: `ResourceLock.Tests/Models/ModelSerializationTests.cs`

**Interfaces:**
- Produces:
  ```csharp
  namespace ResourceLock.Core.Models;
  public sealed record LockRecord(string Resource, string SessionId, int OwnerPid, DateTimeOffset AcquiredAt, DateTimeOffset? ExpiresAt, string? Reason);
  public sealed record LockRequest(string Resource, string SessionId, int OwnerPid, int WaitSeconds = 0, int? TimeoutSeconds = null, string? Reason = null);
  public sealed record LockResult(bool Success, string Resource, string? SessionId, int? OwnerPid, string? Message, DateTimeOffset? ExpiresAt, LockRecord? CurrentHolder = null);
  public sealed record SessionInfo(string SessionId, int OwnerPid, DateTimeOffset ConnectedAt);
  ```

- [ ] **Step 1: Write the failing model serialization tests**

```csharp
namespace ResourceLock.Tests.Models;

using System.Text.Json;
using ResourceLock.Core.Models;
using Xunit;

public class ModelSerializationTests
{
    [Fact]
    public void LockResult_SerializesAndDeserializes_Correctly()
    {
        var record = new LockRecord("CPU", "sess-1", 1234, DateTimeOffset.UtcNow, null, "test");
        var result = new LockResult(true, "CPU", "sess-1", 1234, "Acquired", null, record);
        var json = JsonSerializer.Serialize(result);
        var deserialized = JsonSerializer.Deserialize<LockResult>(json);

        Assert.NotNull(deserialized);
        Assert.True(deserialized.Success);
        Assert.Equal("CPU", deserialized.Resource);
        Assert.Equal(1234, deserialized.OwnerPid);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `dotnet test ResourceLock.Tests/ResourceLock.Tests.csproj`  
Expected: Build or execution failure due to missing projects/classes.

- [ ] **Step 3: Scaffold solution, projects, and implement models**

Run commands to create projects in root:
```bash
dotnet new sln -n ResourceLock
dotnet new classlib -n ResourceLock.Core -f net10.0
dotnet new xunit -n ResourceLock.Tests -f net10.0
dotnet sln add ResourceLock.Core/ResourceLock.Core.csproj ResourceLock.Tests/ResourceLock.Tests.csproj
dotnet add ResourceLock.Tests/ResourceLock.Tests.csproj reference ResourceLock.Core/ResourceLock.Core.csproj
```
Implement `LockRecord`, `LockRequest`, `LockResult`, and `SessionInfo` in `ResourceLock.Core/Models/`.

- [ ] **Step 4: Run test to verify it passes**

Run: `dotnet test ResourceLock.Tests/ResourceLock.Tests.csproj`  
Expected: PASS (1 test passed).

- [ ] **Step 5: Commit**

```bash
git add ResourceLock.sln ResourceLock.Core/ ResourceLock.Tests/
git commit -m "feat: scaffold solution and core domain models"
```

---

### Task 2: InMemoryLockEngine (Queue, Timeout, Normalization)

**Files:**
- Create: `ResourceLock.Core/Engine/ILockEngine.cs`
- Create: `ResourceLock.Core/Engine/InMemoryLockEngine.cs`
- Create: `ResourceLock.Tests/Engine/InMemoryLockEngineTests.cs`

**Interfaces:**
- Consumes: `LockRecord`, `LockRequest`, `LockResult` from `ResourceLock.Core.Models`.
- Produces:
  ```csharp
  namespace ResourceLock.Core.Engine;
  public interface ILockEngine
  {
      ValueTask<LockResult> AcquireAsync(LockRequest request, CancellationToken cancellationToken = default);
      ValueTask<LockResult> ReleaseAsync(string resource, string sessionId, CancellationToken cancellationToken = default);
      ValueTask<int> ReleaseAllForSessionAsync(string sessionId, CancellationToken cancellationToken = default);
      ValueTask<IReadOnlyList<LockRecord>> ListLocksAsync(CancellationToken cancellationToken = default);
      ValueTask<bool> EvictDeadPidAsync(int pid, CancellationToken cancellationToken = default);
  }
  ```

- [ ] **Step 1: Write the failing tests for lock engine**

```csharp
namespace ResourceLock.Tests.Engine;

using ResourceLock.Core.Engine;
using ResourceLock.Core.Models;
using Xunit;

public class InMemoryLockEngineTests
{
    [Fact]
    public async Task Acquire_NonContended_Succeeds()
    {
        var engine = new InMemoryLockEngine();
        var req = new LockRequest("GPU", "sess-1", 100);
        var result = await engine.AcquireAsync(req);

        Assert.True(result.Success);
        Assert.Equal("gpu", result.Resource); // Normalized to lowercase
    }

    [Fact]
    public async Task Acquire_Contended_Immediate_FailsWithCurrentHolder()
    {
        var engine = new InMemoryLockEngine();
        await engine.AcquireAsync(new LockRequest("GPU", "sess-1", 100, Reason: "Busy"));
        var result = await engine.AcquireAsync(new LockRequest("gpu", "sess-2", 200, WaitSeconds: 0));

        Assert.False(result.Success);
        Assert.NotNull(result.CurrentHolder);
        Assert.Equal("sess-1", result.CurrentHolder.SessionId);
    }

    [Fact]
    public async Task Release_Unblocks_QueuedWaiter()
    {
        var engine = new InMemoryLockEngine();
        await engine.AcquireAsync(new LockRequest("CPU", "sess-1", 100));

        var waiterTask = engine.AcquireAsync(new LockRequest("CPU", "sess-2", 200, WaitSeconds: 5));
        Assert.False(waiterTask.IsCompleted);

        await engine.ReleaseAsync("CPU", "sess-1");
        var waiterResult = await waiterTask;

        Assert.True(waiterResult.Success);
        Assert.Equal("sess-2", waiterResult.SessionId);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `dotnet test ResourceLock.Tests/ResourceLock.Tests.csproj --filter InMemoryLockEngineTests`  
Expected: FAIL (`ILockEngine` / `InMemoryLockEngine` not found).

- [ ] **Step 3: Implement `InMemoryLockEngine`**

Implement resource name normalization (`resource.Trim().ToLowerInvariant()`), thread-safe lock dictionary, FIFO waiter queue per resource with `TaskCompletionSource<LockResult>`, and timeout timers.

- [ ] **Step 4: Run tests to verify they pass**

Run: `dotnet test ResourceLock.Tests/ResourceLock.Tests.csproj --filter InMemoryLockEngineTests`  
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add ResourceLock.Core/Engine/ ResourceLock.Tests/Engine/
git commit -m "feat: implement in-memory lock engine with FIFO waiter queues and timeouts"
```

---

### Task 3: IPC Protocol & Streaming Frame Codec

**Files:**
- Create: `ResourceLock.Core/Ipc/IpcConstants.cs`
- Create: `ResourceLock.Core/Ipc/IpcMessage.cs`
- Create: `ResourceLock.Core/Ipc/IpcFrameCodec.cs`
- Create: `ResourceLock.Tests/Ipc/IpcFrameCodecTests.cs`

**Interfaces:**
- Consumes: Models from `ResourceLock.Core.Models`.
- Produces:
  ```csharp
  namespace ResourceLock.Core.Ipc;
  public enum IpcMessageType { RegisterSession, Acquire, Release, ReleaseAll, List, Response, Ping }
  public sealed record IpcEnvelope(string MessageId, IpcMessageType Type, string? PayloadJson);
  public static class IpcFrameCodec
  {
      public static ValueTask WriteFrameAsync(Stream stream, IpcEnvelope envelope, CancellationToken ct = default);
      public static ValueTask<IpcEnvelope?> ReadFrameAsync(Stream stream, CancellationToken ct = default);
  }
  public static class IpcConstants
  {
      public static string GetNamedPipeName();
      public static string GetUnixSocketPath();
  }
  ```

- [ ] **Step 1: Write the failing tests for frame codec**

```csharp
namespace ResourceLock.Tests.Ipc;

using System.IO;
using ResourceLock.Core.Ipc;
using Xunit;

public class IpcFrameCodecTests
{
    [Fact]
    public async Task WriteAndReadFrame_PreservesEnvelope()
    {
        using var stream = new MemoryStream();
        var envelope = new IpcEnvelope("msg-1", IpcMessageType.Acquire, "{\"resource\":\"CPU\"}");

        await IpcFrameCodec.WriteFrameAsync(stream, envelope);
        stream.Position = 0;
        var read = await IpcFrameCodec.ReadFrameAsync(stream);

        Assert.NotNull(read);
        Assert.Equal("msg-1", read.MessageId);
        Assert.Equal(IpcMessageType.Acquire, read.Type);
        Assert.Equal("{\"resource\":\"CPU\"}", read.PayloadJson);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `dotnet test ResourceLock.Tests/ResourceLock.Tests.csproj --filter IpcFrameCodecTests`  
Expected: FAIL (`IpcFrameCodec` not found).

- [ ] **Step 3: Implement length-prefixed `IpcFrameCodec` and `IpcConstants`**

Write 4-byte big-endian / network order length header followed by UTF-8 JSON bytes. `IpcConstants.GetNamedPipeName()` returns `ResourceLock-Coordinator-{Environment.UserName}`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `dotnet test ResourceLock.Tests/ResourceLock.Tests.csproj --filter IpcFrameCodecTests`  
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add ResourceLock.Core/Ipc/ ResourceLock.Tests/Ipc/
git commit -m "feat: implement length-prefixed IPC frame codec and endpoint paths"
```

---

### Task 4: Coordinator Transports (Named Pipe, Unix Socket, Kestrel HTTP)

**Files:**
- Create: `ResourceLock.Coordinator/ResourceLock.Coordinator.csproj`
- Create: `ResourceLock.Coordinator/CoordinatorServer.cs`
- Create: `ResourceLock.Coordinator/Transports/IpcConnectionHandler.cs`
- Create: `ResourceLock.Coordinator/Transports/HttpCoordinatorEndpoints.cs`
- Create: `ResourceLock.Tests/Coordinator/CoordinatorServerTests.cs`

**Interfaces:**
- Consumes: `ILockEngine`, `IpcFrameCodec`, `IpcEnvelope` from `ResourceLock.Core`.
- Produces:
  ```csharp
  namespace ResourceLock.Coordinator;
  public sealed class CoordinatorServer : IAsyncDisposable
  {
      public CoordinatorServer(ILockEngine lockEngine, int httpPort = 51234);
      public Task StartAsync(CancellationToken cancellationToken = default);
      public Task StopAsync(CancellationToken cancellationToken = default);
  }
  ```

- [ ] **Step 1: Write the failing tests for CoordinatorServer IPC connection handling**

```csharp
namespace ResourceLock.Tests.Coordinator;

using System.IO.Pipes;
using ResourceLock.Coordinator;
using ResourceLock.Core.Engine;
using ResourceLock.Core.Ipc;
using ResourceLock.Core.Models;
using Xunit;

public class CoordinatorServerTests
{
    [Fact]
    public async Task ClientDisconnect_AutomaticallyReleasesHeldLocks()
    {
        var engine = new InMemoryLockEngine();
        var pipeName = "ResourceLock-Test-" + Guid.NewGuid();
        // Server listens on pipeName, client connects, acquires lock, closes stream abruptly.
        // Assert engine.ListLocksAsync() shows resource freed.
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `dotnet test ResourceLock.Tests/ResourceLock.Tests.csproj --filter CoordinatorServerTests`  
Expected: FAIL.

- [ ] **Step 3: Create `ResourceLock.Coordinator` project and implement `CoordinatorServer`**

```bash
dotnet new classlib -n ResourceLock.Coordinator -f net10.0
dotnet sln add ResourceLock.Coordinator/ResourceLock.Coordinator.csproj
dotnet add ResourceLock.Coordinator/ResourceLock.Coordinator.csproj reference ResourceLock.Core/ResourceLock.Core.csproj
dotnet add ResourceLock.Tests/ResourceLock.Tests.csproj reference ResourceLock.Coordinator/ResourceLock.Coordinator.csproj
```
Implement `IpcConnectionHandler` that reads envelopes, executes against `ILockEngine`, and when stream completes/breaks, calls `lockEngine.ReleaseAllForSessionAsync(sessionId)`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `dotnet test ResourceLock.Tests/ResourceLock.Tests.csproj --filter CoordinatorServerTests`  
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add ResourceLock.Coordinator/ ResourceLock.Tests/
git commit -m "feat: implement CoordinatorServer with NamedPipe/UDS and disconnect cleanup"
```

---

### Task 5: Auto-Spawn Daemon & Process Liveness Watchdog

**Files:**
- Create: `ResourceLock/ResourceLock.csproj`
- Create: `ResourceLock/Lifecycle/DaemonSpawner.cs`
- Create: `ResourceLock/Lifecycle/ProcessLivenessWatchdog.cs`
- Create: `ResourceLock/Hooks/ExitCleanupHook.cs`
- Create: `ResourceLock.Tests/Lifecycle/DaemonSpawnerTests.cs`

**Interfaces:**
- Consumes: `IpcConstants` from `ResourceLock.Core`, `CoordinatorServer` from `ResourceLock.Coordinator`.
- Produces:
  ```csharp
  namespace ResourceLock.Lifecycle;
  public static class DaemonSpawner
  {
      public static Task<Stream> EnsureCoordinatorAndConnectAsync(CancellationToken cancellationToken = default);
  }
  public static class ExitCleanupHook
  {
      public static void Register(Func<Task> onExitCleanupAsync);
  }
  ```

- [ ] **Step 1: Write tests for single-instance spawn mutex and connection backoff**

```csharp
namespace ResourceLock.Tests.Lifecycle;

using ResourceLock.Lifecycle;
using Xunit;

public class DaemonSpawnerTests
{
    [Fact]
    public void ProcessLiveness_ReturnsFalse_ForDeadPid()
    {
        var isAlive = ProcessLivenessWatchdog.IsPidAlive(9999999);
        Assert.False(isAlive);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `dotnet test ResourceLock.Tests/ResourceLock.Tests.csproj --filter DaemonSpawnerTests`  
Expected: FAIL.

- [ ] **Step 3: Implement `DaemonSpawner`, `ProcessLivenessWatchdog`, and `ExitCleanupHook`**

Implement PID check with `Process.GetProcessById()`, spawn lock using `FileStream` or named mutex, and process spawn via `Environment.ProcessPath coordinator --daemon`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `dotnet test ResourceLock.Tests/ResourceLock.Tests.csproj --filter DaemonSpawnerTests`  
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add ResourceLock/ ResourceLock.Tests/
git commit -m "feat: implement daemon spawner, liveness watchdog, and process exit hooks"
```

---

### Task 6: MCP Server & Microsoft Agent Framework Tools

**Files:**
- Create: `ResourceLock/Mcp/McpServerHost.cs`
- Create: `ResourceLock/Tools/ResourceLockTools.cs`
- Create: `ResourceLock/Program.cs`
- Create: `ResourceLock.Tests/Mcp/McpToolsTests.cs`

**Interfaces:**
- Consumes: Microsoft Agent Framework / `Microsoft.Extensions.AI` abstractions and `ResourceLock.Core`.
- Produces:
  The 4 exposed tools:
  - `acquire_lock(string resource, int wait_seconds = 0, int? timeout_seconds = null, string? reason = null)`
  - `release_lock(string resource)`
  - `list_locks()`
  - `release_all_locks()`

- [ ] **Step 1: Write tests for `ResourceLockTools`**

```csharp
namespace ResourceLock.Tests.Mcp;

using ResourceLock.Core.Engine;
using ResourceLock.Tools;
using Xunit;

public class McpToolsTests
{
    [Fact]
    public async Task AcquireAndRelease_Tools_ExecuteSuccessfully()
    {
        var engine = new InMemoryLockEngine();
        var tools = new ResourceLockTools(engine, sessionId: "sess-test", ownerPid: 1234);

        var acquireResult = await tools.AcquireLockAsync("GPU", wait_seconds: 0);
        Assert.True(acquireResult.Success);

        var releaseResult = await tools.ReleaseLockAsync("GPU");
        Assert.True(releaseResult.Success);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `dotnet test ResourceLock.Tests/ResourceLock.Tests.csproj --filter McpToolsTests`  
Expected: FAIL.

- [ ] **Step 3: Implement `ResourceLockTools`, `McpServerHost` (stdio), and CLI `Program.cs`**

Configure `ResourceLock/Program.cs` to handle:
- Default / no args: Run MCP stdio server.
- `coordinator --daemon`: Run standalone background coordinator server.

- [ ] **Step 4: Run tests to verify they pass**

Run: `dotnet test ResourceLock.Tests/ResourceLock.Tests.csproj --filter McpToolsTests`  
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add ResourceLock/ ResourceLock.Tests/
git commit -m "feat: implement MCP server host and tools using Agent Framework"
```

---

### Task 7: Cross-Machine Federation Client (`Strix:CPU`)

**Files:**
- Create: `ResourceLock.Coordinator/Federation/RemoteCoordinatorClient.cs`
- Modify: `ResourceLock.Coordinator/Transports/IpcConnectionHandler.cs`
- Create: `ResourceLock.Tests/Federation/RemoteFederationTests.cs`

**Interfaces:**
- Consumes: `HttpClient`, `LockRequest`, `LockResult`.
- Produces:
  ```csharp
  namespace ResourceLock.Coordinator.Federation;
  public interface IRemoteCoordinatorClient
  {
      ValueTask<LockResult> ForwardAcquireAsync(string remoteHost, LockRequest request, CancellationToken ct = default);
      ValueTask<LockResult> ForwardReleaseAsync(string remoteHost, string resource, string sessionId, CancellationToken ct = default);
  }
  ```

- [ ] **Step 1: Write unit tests for remote host parsing and delegation**

```csharp
namespace ResourceLock.Tests.Federation;

using ResourceLock.Coordinator.Federation;
using Xunit;

public class RemoteFederationTests
{
    [Theory]
    [InlineData("Strix:CPU", "strix", "cpu")]
    [InlineData("Server1:GPU", "server1", "gpu")]
    public void ResourceParser_ExtractsHostAndResource(string input, string expectedHost, string expectedResource)
    {
        var parsed = RemoteResourceParser.Parse(input);
        Assert.Equal(expectedHost, parsed.Host);
        Assert.Equal(expectedResource, parsed.Resource);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `dotnet test ResourceLock.Tests/ResourceLock.Tests.csproj --filter RemoteFederationTests`  
Expected: FAIL.

- [ ] **Step 3: Implement `RemoteResourceParser` and `RemoteCoordinatorClient`**

Integrate HTTP forwarding in `IpcConnectionHandler`: when target resource contains `:`, dispatch through `RemoteCoordinatorClient` to `http://{host}:51234/api/locks/acquire`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `dotnet test ResourceLock.Tests/ResourceLock.Tests.csproj --filter RemoteFederationTests`  
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add ResourceLock.Coordinator/ ResourceLock.Tests/
git commit -m "feat: implement cross-machine remote coordinator federation"
```

---

### Task 8: Tool Packaging, Verification & NuGet Trusted Publishing

**Files:**
- Modify: `ResourceLock/ResourceLock.csproj`
- Create: `.github/workflows/publish.yml`
- Create: `README.md`

**Interfaces:**
- Package configuration:
  ```xml
  <PackAsTool>true</PackAsTool>
  <ToolCommandName>ResourceLock</ToolCommandName>
  <PackageId>ResourceLock</PackageId>
  ```
- Workflow:
  GitHub Actions workflow using OIDC trusted publisher for package prefix `ResourceLock*`.

- [ ] **Step 1: Configure `ResourceLock.csproj` for packaging**

Set `PackAsTool`, `ToolCommandName`, package metadata, and bundle dependencies.

- [ ] **Step 2: Create GitHub Actions NuGet Trusted Publishing workflow**

Create `.github/workflows/publish.yml`:
- Trigger on tags `v*` and manual workflow dispatch.
- Run `dotnet pack --configuration Release`.
- Run `dotnet nuget push **/*.nupkg --source https://api.nuget.org/v3/index.json` using GitHub OIDC.
- Explicitly documented in workflow comments: package prefix reservation pattern `ResourceLock*`.

- [ ] **Step 3: Verify packing and run end-to-end `dotnet pack` test**

Run: `dotnet pack ResourceLock/ResourceLock.csproj -c Release`  
Verify: `.nupkg` created successfully under `bin/Release`.

- [ ] **Step 4: Run all unit and integration tests across the solution**

Run: `dotnet test ResourceLock.sln`  
Expected: All tests PASS.

- [ ] **Step 5: Commit**

```bash
git add ResourceLock/ResourceLock.csproj .github/ README.md
git commit -m "ci: add NuGet trusted publishing workflow and configure tool packaging"
```
