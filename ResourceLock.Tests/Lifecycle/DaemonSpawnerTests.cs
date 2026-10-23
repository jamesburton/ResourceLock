namespace ResourceLock.Tests.Lifecycle;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using ResourceLock.Coordinator;
using ResourceLock.Core.Engine;
using ResourceLock.Core.Ipc;
using ResourceLock.Hooks;
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

    [Fact]
    public void ProcessLiveness_ReturnsFalse_ForNonPositivePid()
    {
        Assert.False(ProcessLivenessWatchdog.IsPidAlive(0));
        Assert.False(ProcessLivenessWatchdog.IsPidAlive(-10));
    }

    [Fact]
    public void ProcessLiveness_ReturnsTrue_ForCurrentPid()
    {
        var isAlive = ProcessLivenessWatchdog.IsPidAlive(Environment.ProcessId);
        Assert.True(isAlive);
    }

    [Fact]
    public async Task ExitCleanupHook_ExecutesRegisteredCallbacks()
    {
        ExitCleanupHook.ResetForTesting();
        var executed = false;

        ExitCleanupHook.Register(() =>
        {
            executed = true;
            return Task.CompletedTask;
        });

        ExitCleanupHook.ExecuteCleanup();

        Assert.True(executed);
    }

    [Fact]
    public async Task ExitCleanupHook_ExecutesOnlyOnce()
    {
        ExitCleanupHook.ResetForTesting();
        var callCount = 0;

        ExitCleanupHook.Register(() =>
        {
            Interlocked.Increment(ref callCount);
            return Task.CompletedTask;
        });

        ExitCleanupHook.ExecuteCleanup();
        ExitCleanupHook.ExecuteCleanup();

        Assert.Equal(1, callCount);
    }

    [Fact]
    public async Task SpawnLock_PreventsConcurrentAcquisitionUntilDisposed()
    {
        var tempLockFile = Path.Combine(Path.GetTempPath(), $"spawn_lock_test_{Guid.NewGuid():N}.lock");
        try
        {
            var lock1 = await DaemonSpawner.AcquireSpawnLockAsync(tempLockFile, TimeSpan.FromSeconds(1), CancellationToken.None);
            Assert.NotNull(lock1);

            // Attempting to acquire again should time out
            await Assert.ThrowsAsync<TimeoutException>(async () =>
            {
                await DaemonSpawner.AcquireSpawnLockAsync(tempLockFile, TimeSpan.FromMilliseconds(200), CancellationToken.None);
            });

            // Disposing first lock should allow second acquisition
            lock1.Dispose();

            var lock2 = await DaemonSpawner.AcquireSpawnLockAsync(tempLockFile, TimeSpan.FromSeconds(1), CancellationToken.None);
            Assert.NotNull(lock2);
            lock2.Dispose();
        }
        finally
        {
            if (File.Exists(tempLockFile))
            {
                try { File.Delete(tempLockFile); } catch { }
            }
        }
    }

    [Fact]
    public async Task DaemonSpawner_ConnectsDirectly_WhenCoordinatorAlreadyRunning()
    {
        var pipeOrSocketName = OperatingSystem.IsWindows()
            ? $"TestPipe_DaemonSpawner_{Guid.NewGuid():N}"
            : Path.Combine(Path.GetTempPath(), $"test_spawner_{Guid.NewGuid():N}.sock");

        var engine = new InMemoryLockEngine();
        await using var server = new CoordinatorServer(engine, httpPort: -1, customPipeOrSocketName: pipeOrSocketName);
        await server.StartAsync();

        // DaemonSpawner should detect that the coordinator is already listening and connect immediately
        await using var stream = await DaemonSpawner.EnsureCoordinatorAndConnectAsync(
            pipeOrSocketName: pipeOrSocketName,
            cancellationToken: CancellationToken.None);

        Assert.NotNull(stream);
        Assert.True(stream.CanRead);
        Assert.True(stream.CanWrite);

        // Send a ping and receive pong response to prove the stream is functioning
        var ping = new IpcEnvelope("msg-1", IpcMessageType.Ping);
        await IpcFrameCodec.WriteFrameAsync(stream, ping);

        var response = await IpcFrameCodec.ReadFrameAsync(stream);
        Assert.NotNull(response);
        Assert.Equal("msg-1", response.MessageId);
        Assert.Equal(IpcMessageType.Response, response.Type);
    }

    [Fact]
    public async Task DaemonSpawner_ConcurrentCallers_AllConnectSuccessfully()
    {
        var pipeOrSocketName = OperatingSystem.IsWindows()
            ? $"TestPipe_DaemonSpawner_Concurrent_{Guid.NewGuid():N}"
            : Path.Combine(Path.GetTempPath(), $"test_spawner_concurrent_{Guid.NewGuid():N}.sock");

        var tempLockFile = Path.Combine(Path.GetTempPath(), $"spawn_lock_concurrent_{Guid.NewGuid():N}.lock");

        var engine = new InMemoryLockEngine();
        await using var server = new CoordinatorServer(engine, httpPort: -1, customPipeOrSocketName: pipeOrSocketName);
        await server.StartAsync();

        try
        {
            // Spawn 5 concurrent callers connecting through DaemonSpawner
            var tasks = new Task<Stream>[5];
            for (var i = 0; i < 5; i++)
            {
                tasks[i] = DaemonSpawner.EnsureCoordinatorAndConnectAsync(
                    pipeOrSocketName: pipeOrSocketName,
                    spawnLockPath: tempLockFile,
                    cancellationToken: CancellationToken.None);
            }

            var streams = await Task.WhenAll(tasks);
            Assert.Equal(5, streams.Length);

            for (var i = 0; i < streams.Length; i++)
            {
                var s = streams[i];
                Assert.NotNull(s);
                var ping = new IpcEnvelope($"client-{i}", IpcMessageType.Ping);
                await IpcFrameCodec.WriteFrameAsync(s, ping);
                var response = await IpcFrameCodec.ReadFrameAsync(s);
                Assert.NotNull(response);
                Assert.Equal($"client-{i}", response.MessageId);
                await s.DisposeAsync();
            }
        }
        finally
        {
            if (File.Exists(tempLockFile))
            {
                try { File.Delete(tempLockFile); } catch { }
            }
        }
    }

    [Fact]
    public void ExitCleanupHook_HandlesNullTaskCallbackGracefully()
    {
        ExitCleanupHook.ResetForTesting();

        ExitCleanupHook.Register(() => null!);
        var exception = Record.Exception(() => ExitCleanupHook.ExecuteCleanup());
        Assert.Null(exception);
    }

    [Fact]
    public async Task AcquireSpawnLockAsync_ThrowsOperationCanceledException_WhenCancelled()
    {
        var tempLockFile = Path.Combine(Path.GetTempPath(), $"spawn_lock_cancel_{Guid.NewGuid():N}.lock");
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await DaemonSpawner.AcquireSpawnLockAsync(tempLockFile, TimeSpan.FromSeconds(5), cts.Token);
        });
    }

    [Fact]
    public async Task DaemonSpawner_ColdStartConcurrency_HoldingLockSynchronizesCallers()
    {
        var pipeOrSocketName = OperatingSystem.IsWindows()
            ? $"TestPipe_DaemonSpawner_ColdStart_{Guid.NewGuid():N}"
            : Path.Combine(Path.GetTempPath(), $"test_spawner_coldstart_{Guid.NewGuid():N}.sock");

        var tempLockFile = Path.Combine(Path.GetTempPath(), $"spawn_lock_coldstart_{Guid.NewGuid():N}.lock");

        var engine = new InMemoryLockEngine();
        await using var server = new CoordinatorServer(engine, httpPort: -1, customPipeOrSocketName: pipeOrSocketName);

        // Start server with 150ms delay to simulate daemon process cold-start startup delay
        _ = Task.Run(async () =>
        {
            await Task.Delay(150);
            await server.StartAsync();
        });

        try
        {
            // Callers race to connect before server is listening
            var task1 = DaemonSpawner.EnsureCoordinatorAndConnectAsync(
                pipeOrSocketName: pipeOrSocketName,
                spawnLockPath: tempLockFile,
                coordinatorExecutable: "dotnet",
                coordinatorArgs: "--version", // no-op dummy executable that exits immediately
                cancellationToken: CancellationToken.None);

            var task2 = DaemonSpawner.EnsureCoordinatorAndConnectAsync(
                pipeOrSocketName: pipeOrSocketName,
                spawnLockPath: tempLockFile,
                coordinatorExecutable: "dotnet",
                coordinatorArgs: "--version",
                cancellationToken: CancellationToken.None);

            var streams = await Task.WhenAll(task1, task2);
            Assert.Equal(2, streams.Length);

            foreach (var stream in streams)
            {
                Assert.NotNull(stream);
                var ping = new IpcEnvelope("sync-ping", IpcMessageType.Ping);
                await IpcFrameCodec.WriteFrameAsync(stream, ping);
                var pong = await IpcFrameCodec.ReadFrameAsync(stream);
                Assert.NotNull(pong);
                Assert.Equal("sync-ping", pong.MessageId);
                await stream.DisposeAsync();
            }
        }
        finally
        {
            if (File.Exists(tempLockFile))
            {
                try { File.Delete(tempLockFile); } catch { }
            }
        }
    }

    [Fact]
    public void CreateProcessStartInfo_RedirectsAllStdioHandles()
    {
        var psi = DaemonSpawner.CreateProcessStartInfo("customExe", "coordinator --daemon");

        Assert.Equal("customExe", psi.FileName);
        Assert.Equal("coordinator --daemon", psi.Arguments);
        Assert.False(psi.UseShellExecute);
        Assert.True(psi.CreateNoWindow);
        Assert.True(psi.RedirectStandardInput, "StandardInput must be redirected to cleanly detach from parent stdio.");
        Assert.True(psi.RedirectStandardOutput, "StandardOutput must be redirected to cleanly detach from parent stdio.");
        Assert.True(psi.RedirectStandardError, "StandardError must be redirected to cleanly detach from parent stdio.");
    }
}
