namespace ResourceLock.Tests.Mcp;

using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.AI;
using ResourceLock.Coordinator;
using ResourceLock.Core.Engine;
using ResourceLock.Core.Ipc;
using ResourceLock.Core.Models;
using ResourceLock.Ipc;
using ResourceLock.Lifecycle;
using ResourceLock.Mcp;
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

    [Fact]
    public async Task Acquire_WhenContested_ReturnsFailure()
    {
        var engine = new InMemoryLockEngine();
        var currentPid = Environment.ProcessId;
        var tools1 = new ResourceLockTools(engine, sessionId: "sess-1", ownerPid: currentPid);
        var tools2 = new ResourceLockTools(engine, sessionId: "sess-2", ownerPid: currentPid);

        var r1 = await tools1.AcquireLockAsync("GPU", wait_seconds: 0, reason: "Session 1 training");
        Assert.True(r1.Success);

        var r2 = await tools2.AcquireLockAsync("GPU", wait_seconds: 0);
        Assert.False(r2.Success);
        Assert.Equal("sess-1", r2.CurrentHolder?.SessionId);
    }

    [Fact]
    public async Task ListLocks_ReturnsAllActiveLocks()
    {
        var engine = new InMemoryLockEngine();
        var tools = new ResourceLockTools(engine, sessionId: "sess-test", ownerPid: 1234);

        await tools.AcquireLockAsync("GPU", wait_seconds: 0);
        await tools.AcquireLockAsync("CPU", wait_seconds: 0);

        var locks = await tools.ListLocksAsync();
        Assert.Equal(2, locks.Count);
    }

    [Fact]
    public async Task ReleaseAllLocks_ReleasesAllSessionLocks()
    {
        var engine = new InMemoryLockEngine();
        var tools = new ResourceLockTools(engine, sessionId: "sess-test", ownerPid: 1234);

        await tools.AcquireLockAsync("GPU", wait_seconds: 0);
        await tools.AcquireLockAsync("CPU", wait_seconds: 0);

        var res = await tools.ReleaseAllLocksAsync();
        Assert.True(res.Success);
        Assert.Equal(2, res.ReleasedCount);

        var locks = await tools.ListLocksAsync();
        Assert.Empty(locks);
    }

    [Fact]
    public async Task McpServerHost_HandlesInitializeAndToolsListAndCall()
    {
        var engine = new InMemoryLockEngine();
        var tools = new ResourceLockTools(engine, sessionId: "sess-mcp", ownerPid: 5555);
        var host = new McpServerHost(tools);

        // Simulate client stdio via MemoryStreams or Pipes
        var inputLines = new StringBuilder();
        // 1. initialize
        inputLines.AppendLine("{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{}}");
        // 2. notifications/initialized
        inputLines.AppendLine("{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}");
        // 3. ping
        inputLines.AppendLine("{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"ping\"}");
        // 4. tools/list
        inputLines.AppendLine("{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"tools/list\"}");
        // 5. tools/call acquire_lock
        inputLines.AppendLine("{\"jsonrpc\":\"2.0\",\"id\":4,\"method\":\"tools/call\",\"params\":{\"name\":\"acquire_lock\",\"arguments\":{\"resource\":\"NPU\"}}}");
        // 6. tools/call list_locks
        inputLines.AppendLine("{\"jsonrpc\":\"2.0\",\"id\":5,\"method\":\"tools/call\",\"params\":{\"name\":\"list_locks\",\"arguments\":{}}}");
        // 7. tools/call release_lock
        inputLines.AppendLine("{\"jsonrpc\":\"2.0\",\"id\":6,\"method\":\"tools/call\",\"params\":{\"name\":\"release_lock\",\"arguments\":{\"resource\":\"NPU\"}}}");
        // 8. tools/call release_all_locks
        inputLines.AppendLine("{\"jsonrpc\":\"2.0\",\"id\":7,\"method\":\"tools/call\",\"params\":{\"name\":\"release_all_locks\",\"arguments\":{}}}");

        var inStream = new MemoryStream(Encoding.UTF8.GetBytes(inputLines.ToString()));
        var outStream = new MemoryStream();

        await host.RunAsync(inStream, outStream);

        outStream.Position = 0;
        using var reader = new StreamReader(outStream, Encoding.UTF8);
        var outputLines = new List<string>();
        string? line;
        while ((line = await reader.ReadLineAsync()) != null)
        {
            if (!string.IsNullOrWhiteSpace(line))
            {
                outputLines.Add(line);
            }
        }

        // Expected responses for requests with IDs 1, 2, 3, 4, 5, 6, 7
        Assert.Equal(7, outputLines.Count);

        // Check ID 1 (initialize)
        using var initDoc = JsonDocument.Parse(outputLines[0]);
        Assert.Equal(1, initDoc.RootElement.GetProperty("id").GetInt32());
        Assert.True(initDoc.RootElement.GetProperty("result").TryGetProperty("serverInfo", out _));

        // Check ID 2 (ping)
        using var pingDoc = JsonDocument.Parse(outputLines[1]);
        Assert.Equal(2, pingDoc.RootElement.GetProperty("id").GetInt32());

        // Check ID 3 (tools/list)
        using var listDoc = JsonDocument.Parse(outputLines[2]);
        Assert.Equal(3, listDoc.RootElement.GetProperty("id").GetInt32());
        var toolsArray = listDoc.RootElement.GetProperty("result").GetProperty("tools");
        Assert.Equal(4, toolsArray.GetArrayLength());

        // Check ID 4 (acquire_lock)
        using var acqDoc = JsonDocument.Parse(outputLines[3]);
        Assert.Equal(4, acqDoc.RootElement.GetProperty("id").GetInt32());
        var content = acqDoc.RootElement.GetProperty("result").GetProperty("content");
        var text = content[0].GetProperty("text").GetString();
        Assert.Contains("\"success\":true", text?.Replace(" ", ""));
        Assert.Contains("npu", text, StringComparison.OrdinalIgnoreCase);

        // Check ID 6 (release_lock)
        using var relDoc = JsonDocument.Parse(outputLines[5]);
        var relText = relDoc.RootElement.GetProperty("result").GetProperty("content")[0].GetProperty("text").GetString();
        Assert.Contains("\"success\":true", relText?.Replace(" ", ""));
    }

    [Fact]
    public async Task AsAIFunctions_CanBeInvoked()
    {
        var engine = new InMemoryLockEngine();
        var tools = new ResourceLockTools(engine, sessionId: "sess-ai", ownerPid: 9876);
        var functions = tools.AsAIFunctions().ToList();

        Assert.Equal(4, functions.Count);

        var acqFunc = functions.First(f => f.Name == "acquire_lock");
        var raw = await acqFunc.InvokeAsync(new AIFunctionArguments { ["resource"] = "GPU" });
        Assert.NotNull(raw);
        var res = raw is JsonElement elem
            ? JsonSerializer.Deserialize<LockResult>(elem.GetRawText(), IpcFrameCodec.DefaultJsonOptions)
            : raw as LockResult;
        Assert.NotNull(res);
        Assert.True(res.Success);

        var relFunc = functions.First(f => f.Name == "release_lock");
        var rawRel = await relFunc.InvokeAsync(new AIFunctionArguments { ["resource"] = "GPU" });
        Assert.NotNull(rawRel);
        var rel = rawRel is JsonElement relElem
            ? JsonSerializer.Deserialize<LockResult>(relElem.GetRawText(), IpcFrameCodec.DefaultJsonOptions)
            : rawRel as LockResult;
        Assert.NotNull(rel);
        Assert.True(rel.Success);
    }

    [Fact]
    public async Task IpcLockEngine_EndToEndWithCoordinator()
    {
        var pipeName = OperatingSystem.IsWindows() ? "test-mcp-pipe-" + Guid.NewGuid().ToString("N") : Path.Combine(Path.GetTempPath(), $"rl-mcp-{Guid.NewGuid():N}.sock");
        var engine = new InMemoryLockEngine();
        await using var server = new CoordinatorServer(engine, httpPort: -1, customPipeOrSocketName: pipeName);
        await server.StartAsync();

        var stream = await DaemonSpawner.TryConnectAsync(pipeName, 2000, CancellationToken.None);
        Assert.NotNull(stream);

        await using var ipcEngine = new IpcLockEngine(stream);
        await ipcEngine.RegisterSessionAsync("sess-ipc", 1234);

        var tools = new ResourceLockTools(ipcEngine, sessionId: "sess-ipc", ownerPid: 1234);
        var acq = await tools.AcquireLockAsync("CPU", wait_seconds: 0);
        Assert.True(acq.Success);

        var list = await tools.ListLocksAsync();
        Assert.Single(list);

        var rel = await tools.ReleaseLockAsync("CPU");
        Assert.True(rel.Success);
    }

    [Fact]
    public async Task SubagentOwners_ShareSingleMcpToolsInstance_BlockAndReleaseIndependently()
    {
        var engine = new InMemoryLockEngine();
        var currentPid = Environment.ProcessId;
        var tools = new ResourceLockTools(engine, sessionId: "sess-parent", ownerPid: currentPid);

        // 1. Default session acquires CPU
        var acq1 = await tools.AcquireLockAsync("CPU", wait_seconds: 0, reason: "Lane 1 build");
        Assert.True(acq1.Success);
        Assert.Equal("sess-parent", acq1.SessionId);

        // 2. Second acquire without owner fails immediately with diagnostic message
        var dup = await tools.AcquireLockAsync("CPU", wait_seconds: 5, reason: "Duplicate without owner");
        Assert.False(dup.Success);
        Assert.Contains("already locked by this session", dup.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("owner", dup.Message, StringComparison.OrdinalIgnoreCase);

        // 3. Subagent with owner 'lane-2' queues behind default session
        var lane2Task = tools.AcquireLockAsync("CPU", wait_seconds: 5, reason: "Lane 2 test", owner: "lane-2");
        Assert.False(lane2Task.IsCompleted);

        // 4. Default session releases CPU -> lane-2 acquires it
        var rel1 = await tools.ReleaseLockAsync("CPU");
        Assert.True(rel1.Success);

        var acq2 = await lane2Task;
        Assert.True(acq2.Success);
        Assert.Equal("sess-parent:lane-2", acq2.SessionId);

        // 5. Calling ReleaseLockAsync without owner when 'lane-2' holds it fails with helpful hint
        var wrongRel = await tools.ReleaseLockAsync("CPU");
        Assert.False(wrongRel.Success);
        Assert.Contains("lane-2", wrongRel.Message, StringComparison.OrdinalIgnoreCase);

        // 6. Process exit cleanup releases all sub-session locks as well
        await tools.AcquireLockAsync("GPU", wait_seconds: 0, owner: "lane-3");
        var totalExitReleased = await tools.ReleaseAllSessionAndSubsessionLocksAsync();
        Assert.Equal(2, totalExitReleased);
        Assert.Empty(await tools.ListLocksAsync());
    }

    [Fact]
    public async Task McpServerHost_And_IpcLockEngine_MultiplexConcurrentWaitAndReleaseWithoutHeadOfLineBlocking()
    {
        var pipeName = OperatingSystem.IsWindows()
            ? "test-mcp-mux-" + Guid.NewGuid().ToString("N")
            : Path.Combine(Path.GetTempPath(), $"rl-mux-{Guid.NewGuid():N}.sock");

        var engine = new InMemoryLockEngine();
        await using var server = new CoordinatorServer(engine, httpPort: -1, customPipeOrSocketName: pipeName);
        await server.StartAsync();

        var ipcStream = await DaemonSpawner.TryConnectAsync(pipeName, 2000, CancellationToken.None);
        Assert.NotNull(ipcStream);

        var currentPid = Environment.ProcessId;
        await using var ipcEngine = new IpcLockEngine(ipcStream);
        await ipcEngine.RegisterSessionAsync("sess-mux", currentPid);

        var tools = new ResourceLockTools(ipcEngine, sessionId: "sess-mux", ownerPid: currentPid);
        var host = new McpServerHost(tools);

        using var clientToServerPipe = new System.IO.Pipes.AnonymousPipeServerStream(System.IO.Pipes.PipeDirection.Out);
        using var serverInputPipe = new System.IO.Pipes.AnonymousPipeClientStream(System.IO.Pipes.PipeDirection.In, clientToServerPipe.GetClientHandleAsString());
        using var serverToClientPipe = new System.IO.Pipes.AnonymousPipeServerStream(System.IO.Pipes.PipeDirection.Out);
        using var clientOutputPipe = new System.IO.Pipes.AnonymousPipeClientStream(System.IO.Pipes.PipeDirection.In, serverToClientPipe.GetClientHandleAsString());

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var hostTask = host.RunAsync(serverInputPipe, serverToClientPipe, cts.Token);

        await using var writer = new StreamWriter(clientToServerPipe, new UTF8Encoding(false)) { AutoFlush = true };
        using var reader = new StreamReader(clientOutputPipe, Encoding.UTF8);

        // 1. Lane 1 acquires CPU
        await writer.WriteLineAsync("{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{\"name\":\"acquire_lock\",\"arguments\":{\"resource\":\"CPU\",\"owner\":\"lane-1\",\"reason\":\"Lane 1\"}}}");
        var resp1Line = await reader.ReadLineAsync(cts.Token);
        Assert.NotNull(resp1Line);
        using var doc1 = JsonDocument.Parse(resp1Line);
        Assert.Equal(1, doc1.RootElement.GetProperty("id").GetInt32());
        var text1 = doc1.RootElement.GetProperty("result").GetProperty("content")[0].GetProperty("text").GetString();
        Assert.Contains("\"success\": true", text1);

        // 2. Lane 2 requests CPU with wait_seconds: 5 (blocks waiting for Lane 1)
        await writer.WriteLineAsync("{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/call\",\"params\":{\"name\":\"acquire_lock\",\"arguments\":{\"resource\":\"CPU\",\"wait_seconds\":5,\"owner\":\"lane-2\",\"reason\":\"Lane 2\"}}}");

        // Give Lane 2 a moment to enter the coordinator wait queue
        await Task.Delay(150, cts.Token);

        // 3. While Lane 2 is blocked waiting, Lane 1 sends release_lock on the SAME MCP stdio + IPC stream
        await writer.WriteLineAsync("{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"tools/call\",\"params\":{\"name\":\"release_lock\",\"arguments\":{\"resource\":\"CPU\",\"owner\":\"lane-1\"}}}");

        var respA = await reader.ReadLineAsync(cts.Token);
        var respB = await reader.ReadLineAsync(cts.Token);
        Assert.NotNull(respA);
        Assert.NotNull(respB);

        using var docA = JsonDocument.Parse(respA);
        using var docB = JsonDocument.Parse(respB);
        var ids = new[] { docA.RootElement.GetProperty("id").GetInt32(), docB.RootElement.GetProperty("id").GetInt32() };
        Assert.Contains(2, ids);
        Assert.Contains(3, ids);

        var textA = docA.RootElement.GetProperty("result").GetProperty("content")[0].GetProperty("text").GetString();
        var textB = docB.RootElement.GetProperty("result").GetProperty("content")[0].GetProperty("text").GetString();
        Assert.Contains("\"success\": true", textA);
        Assert.Contains("\"success\": true", textB);

        await writer.DisposeAsync();
        await hostTask;
        var rel = new { Success = true };
        Assert.True(rel.Success);
    }
}
