namespace ResourceLock.Tests.Coordinator;

using System;
using System.IO;
using System.IO.Pipes;
using System.Net.Http;
using System.Net.Http.Json;
using System.Net.Sockets;
using System.Text.Json;
using System.Threading.Tasks;
using ResourceLock.Coordinator;
using ResourceLock.Core.Engine;
using ResourceLock.Core.Ipc;
using ResourceLock.Core.Models;
using Xunit;

public class CoordinatorServerTests
{
    private static async Task<Stream> ConnectClientAsync(string pipeOrSocketName)
    {
        if (OperatingSystem.IsWindows())
        {
            var clientStream = new NamedPipeClientStream(".", pipeOrSocketName, PipeDirection.InOut, PipeOptions.Asynchronous);
            await clientStream.ConnectAsync(5000);
            return clientStream;
        }
        else
        {
            var socket = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
            await socket.ConnectAsync(new UnixDomainSocketEndPoint(pipeOrSocketName));
            return new NetworkStream(socket, ownsSocket: true);
        }
    }

    [Fact]
    public async Task ClientDisconnect_AutomaticallyReleasesHeldLocks()
    {
        var engine = new InMemoryLockEngine();
        var pipeName = OperatingSystem.IsWindows()
            ? "ResourceLock-Test-" + Guid.NewGuid()
            : Path.Combine(Path.GetTempPath(), $"resourcelock-test-{Guid.NewGuid()}.sock");

        await using var server = new CoordinatorServer(engine, httpPort: -1, customPipeOrSocketName: pipeName);
        await server.StartAsync();

        // Connect client
        var clientStream = await ConnectClientAsync(pipeName);

        // Register session
        var registerEnvelope = new IpcEnvelope(
            "msg-reg",
            IpcMessageType.RegisterSession,
            JsonSerializer.Serialize(new SessionInfo("sess-disconnect", 1234, DateTimeOffset.UtcNow), IpcFrameCodec.DefaultJsonOptions));
        await IpcFrameCodec.WriteFrameAsync(clientStream, registerEnvelope);
        var regResponse = await IpcFrameCodec.ReadFrameAsync(clientStream);
        Assert.NotNull(regResponse);

        // Acquire lock
        var req = new LockRequest("GPU", "sess-disconnect", 1234);
        var acquireEnvelope = new IpcEnvelope(
            "msg-acq",
            IpcMessageType.Acquire,
            JsonSerializer.Serialize(req, IpcFrameCodec.DefaultJsonOptions));
        await IpcFrameCodec.WriteFrameAsync(clientStream, acquireEnvelope);
        var acqResponse = await IpcFrameCodec.ReadFrameAsync(clientStream);
        Assert.NotNull(acqResponse);
        var result = JsonSerializer.Deserialize<LockResult>(acqResponse.PayloadJson!, IpcFrameCodec.DefaultJsonOptions);
        Assert.NotNull(result);
        Assert.True(result.Success);

        // Verify lock is held in engine (normalized to lowercase)
        var heldLocks = await engine.ListLocksAsync();
        Assert.Single(heldLocks);
        Assert.Equal("gpu", heldLocks[0].Resource, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("sess-disconnect", heldLocks[0].SessionId);

        // Close stream abruptly
        clientStream.Dispose();

        // Assert engine automatically freed the lock
        var start = DateTime.UtcNow;
        IReadOnlyList<LockRecord> remainingLocks = [];
        while ((DateTime.UtcNow - start).TotalSeconds < 3)
        {
            remainingLocks = await engine.ListLocksAsync();
            if (remainingLocks.Count == 0)
            {
                break;
            }
            await Task.Delay(50);
        }

        Assert.Empty(remainingLocks);
        await server.StopAsync();
    }

    [Fact]
    public async Task Ipc_ExplicitAcquireReleaseAndList_Succeeds()
    {
        var engine = new InMemoryLockEngine();
        var pipeName = OperatingSystem.IsWindows()
            ? "ResourceLock-Test-" + Guid.NewGuid()
            : Path.Combine(Path.GetTempPath(), $"resourcelock-test-{Guid.NewGuid()}.sock");

        await using var server = new CoordinatorServer(engine, httpPort: -1, customPipeOrSocketName: pipeName);
        await server.StartAsync();

        await using var clientStream = await ConnectClientAsync(pipeName);

        // Acquire
        var acqReq = new LockRequest("CPU", "sess-1", 200);
        await IpcFrameCodec.WriteFrameAsync(clientStream, new IpcEnvelope("m1", IpcMessageType.Acquire, JsonSerializer.Serialize(acqReq, IpcFrameCodec.DefaultJsonOptions)));
        var acqResp = await IpcFrameCodec.ReadFrameAsync(clientStream);
        Assert.NotNull(acqResp);
        var acqResult = JsonSerializer.Deserialize<LockResult>(acqResp.PayloadJson!, IpcFrameCodec.DefaultJsonOptions);
        Assert.NotNull(acqResult);
        Assert.True(acqResult.Success);

        // List
        await IpcFrameCodec.WriteFrameAsync(clientStream, new IpcEnvelope("m2", IpcMessageType.List));
        var listResp = await IpcFrameCodec.ReadFrameAsync(clientStream);
        Assert.NotNull(listResp);
        var listResult = JsonSerializer.Deserialize<LockRecord[]>(listResp.PayloadJson!, IpcFrameCodec.DefaultJsonOptions);
        Assert.NotNull(listResult);
        Assert.Single(listResult);
        Assert.Equal("cpu", listResult[0].Resource, StringComparer.OrdinalIgnoreCase);

        // Release
        var relReq = new { Resource = "CPU", SessionId = "sess-1" };
        await IpcFrameCodec.WriteFrameAsync(clientStream, new IpcEnvelope("m3", IpcMessageType.Release, JsonSerializer.Serialize(relReq, IpcFrameCodec.DefaultJsonOptions)));
        var relResp = await IpcFrameCodec.ReadFrameAsync(clientStream);
        Assert.NotNull(relResp);
        var relResult = JsonSerializer.Deserialize<LockResult>(relResp.PayloadJson!, IpcFrameCodec.DefaultJsonOptions);
        Assert.NotNull(relResult);
        Assert.True(relResult.Success);

        // Verify engine is empty
        var emptyLocks = await engine.ListLocksAsync();
        Assert.Empty(emptyLocks);

        await server.StopAsync();
    }

    [Fact]
    public async Task HttpEndpoints_AcquireReleaseAndList_WorkCorrectly()
    {
        var engine = new InMemoryLockEngine();
        var pipeName = OperatingSystem.IsWindows()
            ? "ResourceLock-Test-" + Guid.NewGuid()
            : Path.Combine(Path.GetTempPath(), $"resourcelock-test-{Guid.NewGuid()}.sock");

        // Use port 0 for ephemeral port
        await using var server = new CoordinatorServer(engine, httpPort: 0, customPipeOrSocketName: pipeName);
        await server.StartAsync();

        Assert.NotNull(server.HttpUri);

        using var httpClient = new HttpClient { BaseAddress = server.HttpUri };

        // Ping
        var pingResp = await httpClient.GetAsync("/api/ping");
        Assert.True(pingResp.IsSuccessStatusCode);

        // Acquire via HTTP
        var acqReq = new LockRequest("NPU", "sess-http", 999);
        var postAcqResp = await httpClient.PostAsJsonAsync("/api/locks/acquire", acqReq, IpcFrameCodec.DefaultJsonOptions);
        Assert.True(postAcqResp.IsSuccessStatusCode);
        var acqResult = await postAcqResp.Content.ReadFromJsonAsync<LockResult>(IpcFrameCodec.DefaultJsonOptions);
        Assert.NotNull(acqResult);
        Assert.True(acqResult.Success);

        // List via HTTP
        var listResp = await httpClient.GetFromJsonAsync<LockRecord[]>("/api/locks", IpcFrameCodec.DefaultJsonOptions);
        Assert.NotNull(listResp);
        Assert.Single(listResp);
        Assert.Equal("npu", listResp[0].Resource, StringComparer.OrdinalIgnoreCase);

        // Release via HTTP
        var relReq = new { Resource = "NPU", SessionId = "sess-http" };
        var postRelResp = await httpClient.PostAsJsonAsync("/api/locks/release", relReq, IpcFrameCodec.DefaultJsonOptions);
        Assert.True(postRelResp.IsSuccessStatusCode);
        var relResult = await postRelResp.Content.ReadFromJsonAsync<LockResult>(IpcFrameCodec.DefaultJsonOptions);
        Assert.NotNull(relResult);
        Assert.True(relResult.Success);

        await server.StopAsync();
    }

    [Fact]
    public async Task CoordinatorServer_ConfigurableHost_BindsAndResponds()
    {
        var engine = new InMemoryLockEngine();
        var pipeName = OperatingSystem.IsWindows()
            ? "ResourceLock-Test-" + Guid.NewGuid()
            : Path.Combine(Path.GetTempPath(), $"resourcelock-test-{Guid.NewGuid()}.sock");

        // Explicitly test binding with 0.0.0.0 and port 0
        await using var server = new CoordinatorServer(
            engine,
            httpPort: 0,
            customPipeOrSocketName: pipeName,
            httpHost: "0.0.0.0");

        await server.StartAsync();

        Assert.NotNull(server.HttpUri);
        Assert.True(server.HttpPort > 0);

        using var httpClient = new HttpClient { BaseAddress = server.HttpUri };
        var ping = await httpClient.GetAsync("/api/ping");
        Assert.True(ping.IsSuccessStatusCode);

        await server.StopAsync();
    }
}
