namespace ResourceLock.Tests.Coordinator;

using System;
using System.IO;
using System.Threading.Tasks;
using ResourceLock.Coordinator;
using ResourceLock.Core.Engine;
using ResourceLock.Core.Models;
using ResourceLock.Ipc;
using ResourceLock.Lifecycle;
using Xunit;

public class CoordinatorStatusTests
{
    private static string NewEndpoint() => OperatingSystem.IsWindows()
        ? "ResourceLock-Status-" + Guid.NewGuid()
        : Path.Combine(Path.GetTempPath(), $"resourcelock-status-{Guid.NewGuid()}.sock");

    private static async Task<IpcLockEngine> ConnectAsync(string endpoint)
    {
        var stream = await DaemonSpawner.TryConnectExistingAsync(endpoint, 5000);
        Assert.NotNull(stream);
        return new IpcLockEngine(stream);
    }

    [Fact]
    public async Task Status_ReportsPidStartTimeClientsAndLocks()
    {
        var endpoint = NewEndpoint();
        var before = DateTimeOffset.UtcNow.AddSeconds(-1);
        await using var server = new CoordinatorServer(new InMemoryLockEngine(), httpPort: -1, customPipeOrSocketName: endpoint);
        await server.StartAsync();

        await using var client = await ConnectAsync(endpoint);
        await client.RegisterSessionAsync("sess-status", 4321);
        await client.AcquireAsync(new LockRequest("GPU", "sess-status", 4321));

        await using var observer = await ConnectAsync(endpoint);
        var status = await observer.GetStatusAsync();

        Assert.NotNull(status);
        Assert.Equal(Environment.ProcessId, status.Pid);
        Assert.True(status.StartedAt >= before && status.StartedAt <= DateTimeOffset.UtcNow);
        Assert.Equal(2, status.ConnectedClients);
        Assert.Equal(1, status.HeldLocks);
        Assert.False(string.IsNullOrEmpty(status.Version));
    }

    [Fact]
    public async Task Status_ClientCountDropsAfterDisconnect()
    {
        var endpoint = NewEndpoint();
        await using var server = new CoordinatorServer(new InMemoryLockEngine(), httpPort: -1, customPipeOrSocketName: endpoint);
        await server.StartAsync();

        await using var observer = await ConnectAsync(endpoint);
        var other = await ConnectAsync(endpoint);
        await other.RegisterSessionAsync("sess-x", 1);
        Assert.Equal(2, (await observer.GetStatusAsync())!.ConnectedClients);

        await other.DisposeAsync();

        CoordinatorStatus? status = null;
        for (var i = 0; i < 50; i++)
        {
            status = await observer.GetStatusAsync();
            if (status!.ConnectedClients == 1) break;
            await Task.Delay(100);
        }

        Assert.Equal(1, status!.ConnectedClients);
    }

    [Fact]
    public async Task RunStatus_WhenNoCoordinator_ReturnsOneWithoutSpawning()
    {
        var writer = new StringWriter();
        var code = await Program.RunStatusAsync(NewEndpoint(), writer);

        Assert.Equal(1, code);
        Assert.Contains("coordinator not running", writer.ToString());
    }

    [Fact]
    public async Task RunStatus_WhenCoordinatorRunning_PrintsDetailsAndReturnsZero()
    {
        var endpoint = NewEndpoint();
        await using var server = new CoordinatorServer(new InMemoryLockEngine(), httpPort: -1, customPipeOrSocketName: endpoint);
        await server.StartAsync();

        var writer = new StringWriter();
        var code = await Program.RunStatusAsync(endpoint, writer);

        Assert.Equal(0, code);
        var text = writer.ToString();
        Assert.Contains(Environment.ProcessId.ToString(), text);
        Assert.Contains("held locks:        0", text);
    }
}
