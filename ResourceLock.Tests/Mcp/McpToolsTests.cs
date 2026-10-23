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
        var pipeName = "test-mcp-pipe-" + Guid.NewGuid().ToString("N");
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
}
