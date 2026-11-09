namespace ResourceLock.Tests.Coordinator;

using System;
using System.IO;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using ResourceLock.Coordinator;
using ResourceLock.Core.Engine;
using ResourceLock.Core.Ipc;
using ResourceLock.Core.Models;
using ResourceLock.Lifecycle;
using Xunit;

public class CoordinatorRestartTests
{
    private static string NewEndpoint() => OperatingSystem.IsWindows()
        ? "ResourceLock-Test-" + Guid.NewGuid()
        : Path.Combine(Path.GetTempPath(), $"resourcelock-test-{Guid.NewGuid()}.sock");

    private static async Task<Stream> ConnectAsync(string endpoint)
        => await DaemonSpawner.TryConnectAsync(endpoint, 5000, CancellationToken.None)
           ?? throw new TimeoutException("connect failed");

    [Fact]
    public async Task ShutdownMessage_RepliesWithHeldLocks_AndSignalsShutdown()
    {
        var engine = new InMemoryLockEngine();
        var endpoint = NewEndpoint();
        await using var server = new CoordinatorServer(engine, httpPort: -1, customPipeOrSocketName: endpoint);
        await server.StartAsync();
        await engine.AcquireAsync(new LockRequest("db", "sess-a", 42), CancellationToken.None);

        await using var client = await ConnectAsync(endpoint);
        await IpcFrameCodec.WriteFrameAsync(client, new IpcEnvelope("m1", IpcMessageType.Shutdown));
        var resp = await IpcFrameCodec.ReadFrameAsync(client);

        Assert.NotNull(resp);
        Assert.Equal("m1", resp.MessageId);
        using var doc = JsonDocument.Parse(resp.PayloadJson!);
        var held = doc.RootElement.GetProperty("heldLocks");
        Assert.Equal(1, held.GetArrayLength());
        Assert.Equal("sess-a", held[0].GetProperty("sessionId").GetString());

        await server.ShutdownRequested.WaitAsync(TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task Restart_RefusesWithoutForce_WhenLocksHeld()
    {
        var engine = new InMemoryLockEngine();
        var endpoint = NewEndpoint();
        await using var server = new CoordinatorServer(engine, httpPort: -1, customPipeOrSocketName: endpoint);
        await server.StartAsync();
        await engine.AcquireAsync(new LockRequest("db", "sess-a", 42), CancellationToken.None);

        var started = false;
        using var output = new StringWriter();
        using var error = new StringWriter();
        var code = await CoordinatorRestarter.RestartAsync(endpoint, false, output, error,
            _ => { started = true; throw new InvalidOperationException(); }, TimeSpan.FromSeconds(5));

        Assert.Equal(CoordinatorRestarter.ExitLocksHeld, code);
        Assert.Contains("sess-a", error.ToString());
        Assert.False(started);
        Assert.False(server.ShutdownRequested.IsCompleted);
    }

    [Fact]
    public async Task Restart_WithForce_StopsDaemonAndStartsFresh()
    {
        var engine = new InMemoryLockEngine();
        var endpoint = NewEndpoint();
        var server = new CoordinatorServer(engine, httpPort: -1, customPipeOrSocketName: endpoint);
        await server.StartAsync();
        await engine.AcquireAsync(new LockRequest("db", "sess-a", 42), CancellationToken.None);

        // Mimic Program.RunCoordinatorAsync: stop the server once shutdown is requested.
        var stopper = Task.Run(async () =>
        {
            await server.ShutdownRequested;
            await server.DisposeAsync();
        });

        CoordinatorServer? fresh = null;
        using var output = new StringWriter();
        using var error = new StringWriter();
        try
        {
            var code = await CoordinatorRestarter.RestartAsync(endpoint, true, output, error, async _ =>
            {
                fresh = new CoordinatorServer(new InMemoryLockEngine(), httpPort: -1, customPipeOrSocketName: endpoint);
                await fresh.StartAsync();
                return await ConnectAsync(endpoint);
            }, TimeSpan.FromSeconds(10));

            await stopper;
            Assert.Equal(CoordinatorRestarter.ExitOk, code);
            Assert.Contains("Revoking 1 lock", output.ToString());
            Assert.NotNull(fresh);
        }
        finally
        {
            if (fresh != null) await fresh.DisposeAsync();
        }
    }

    [Fact]
    public async Task Restart_WithNoDaemon_JustStartsFresh()
    {
        var started = false;
        using var output = new StringWriter();
        using var error = new StringWriter();
        var code = await CoordinatorRestarter.RestartAsync(NewEndpoint(), false, output, error,
            _ => { started = true; return Task.FromResult<Stream>(new MemoryStream()); }, TimeSpan.FromSeconds(1));

        Assert.Equal(0, code);
        Assert.True(started);
    }
}
