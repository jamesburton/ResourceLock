namespace ResourceLock.Lifecycle;

using System;
using System.Diagnostics;
using System.IO;
using System.IO.Pipes;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using ResourceLock.Core.Ipc;

public static class DaemonSpawner
{
    public static Task<Stream> EnsureCoordinatorAndConnectAsync(CancellationToken cancellationToken = default)
    {
        return EnsureCoordinatorAndConnectAsync(
            pipeOrSocketName: null,
            coordinatorExecutable: null,
            coordinatorArgs: null,
            spawnLockPath: null,
            cancellationToken: cancellationToken);
    }

    public static async Task<Stream> EnsureCoordinatorAndConnectAsync(
        string? pipeOrSocketName,
        string? coordinatorExecutable = null,
        string? coordinatorArgs = null,
        string? spawnLockPath = null,
        CancellationToken cancellationToken = default)
    {
        var endpoint = ResolveEndpoint(pipeOrSocketName);

        // 1. Initial probe - if coordinator is already running, connect immediately
        var stream = await TryConnectAsync(endpoint, timeoutMs: 150, cancellationToken).ConfigureAwait(false);
        if (stream != null)
        {
            return stream;
        }

        // 2. Coordinator not responding; acquire inter-process spawn lock to avoid spawn races
        using (await AcquireSpawnLockAsync(spawnLockPath, TimeSpan.FromSeconds(10), cancellationToken).ConfigureAwait(false))
        {
            // Double-check: another process might have launched coordinator while waiting for lock
            stream = await TryConnectAsync(endpoint, timeoutMs: 150, cancellationToken).ConfigureAwait(false);
            if (stream != null)
            {
                return stream;
            }

            // Spawn detached background daemon process
            SpawnCoordinatorProcess(coordinatorExecutable, coordinatorArgs);

            // 3. Poll IPC connection with exponential backoff while holding spawn lock
            // This guarantees waiting callers only acquire the lock after the coordinator
            // is listening, preventing duplicate spawn races during cold-start.
            var delayMs = 25;
            var maxDelayMs = 250;
            var totalTimeout = TimeSpan.FromSeconds(5);
            var sw = Stopwatch.StartNew();

            while (!cancellationToken.IsCancellationRequested && sw.Elapsed < totalTimeout)
            {
                stream = await TryConnectAsync(endpoint, timeoutMs: 100, cancellationToken).ConfigureAwait(false);
                if (stream != null)
                {
                    return stream;
                }

                await Task.Delay(delayMs, cancellationToken).ConfigureAwait(false);
                delayMs = Math.Min(delayMs * 2, maxDelayMs);
            }

            cancellationToken.ThrowIfCancellationRequested();
            throw new TimeoutException($"Failed to connect to coordinator daemon at '{endpoint}' within {totalTimeout.TotalSeconds} seconds.");
        }
    }

    public static string GetSpawnLockPath()
    {
        var home = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
        if (string.IsNullOrEmpty(home))
        {
            home = Environment.GetEnvironmentVariable("HOME")
                   ?? Environment.GetEnvironmentVariable("USERPROFILE")
                   ?? ".";
        }
        return Path.Combine(home, ".resourcelock", "spawn.lock");
    }

    internal static async Task<IDisposable> AcquireSpawnLockAsync(
        string? lockPath,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        var path = lockPath ?? GetSpawnLockPath();
        var dir = Path.GetDirectoryName(path);
        if (!string.IsNullOrEmpty(dir))
        {
            Directory.CreateDirectory(dir);
        }

        var sw = Stopwatch.StartNew();
        var retryDelay = 50;

        while (!cancellationToken.IsCancellationRequested && sw.Elapsed < timeout)
        {
            try
            {
                var fs = new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
                return fs;
            }
            catch (IOException)
            {
                // File locked by another process, wait and retry
                await Task.Delay(retryDelay, cancellationToken).ConfigureAwait(false);
            }
        }

        cancellationToken.ThrowIfCancellationRequested();
        throw new TimeoutException($"Timed out acquiring spawn lock at '{path}' after {timeout.TotalSeconds} seconds.");
    }

    internal static async Task<Stream?> TryConnectAsync(string endpoint, int timeoutMs, CancellationToken cancellationToken)
    {
        if (OperatingSystem.IsWindows())
        {
            var client = new NamedPipeClientStream(".", endpoint, PipeDirection.InOut, PipeOptions.Asynchronous);
            try
            {
                await client.ConnectAsync(timeoutMs, cancellationToken).ConfigureAwait(false);
                return client;
            }
            catch
            {
                client.Dispose();
                return null;
            }
        }
        else
        {
            var sw = Stopwatch.StartNew();
            var delay = 25;
            while (!cancellationToken.IsCancellationRequested)
            {
                var socket = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
                try
                {
                    var socketEndPoint = new UnixDomainSocketEndPoint(endpoint);
                    var remaining = timeoutMs - (int)sw.ElapsedMilliseconds;
                    if (remaining <= 0 && sw.ElapsedMilliseconds > 0)
                    {
                        socket.Dispose();
                        break;
                    }
                    using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                    cts.CancelAfter(Math.Min(250, Math.Max(10, remaining)));
                    await socket.ConnectAsync(socketEndPoint, cts.Token).ConfigureAwait(false);
                    return new NetworkStream(socket, ownsSocket: true);
                }
                catch
                {
                    socket.Dispose();
                    if (sw.ElapsedMilliseconds >= timeoutMs)
                    {
                        break;
                    }
                    await Task.Delay(Math.Min(delay, Math.Max(1, timeoutMs - (int)sw.ElapsedMilliseconds)), cancellationToken).ConfigureAwait(false);
                    delay = Math.Min(delay * 2, 200);
                }
            }
            return null;
        }
    }

    internal static ProcessStartInfo CreateProcessStartInfo(string? coordinatorExecutable, string? coordinatorArgs)
    {
        var exe = coordinatorExecutable ?? Environment.ProcessPath ?? "ResourceLock";
        var args = coordinatorArgs ?? "coordinator --daemon";

        return new ProcessStartInfo
        {
            FileName = exe,
            Arguments = args,
            UseShellExecute = false,
            CreateNoWindow = true,
            WindowStyle = ProcessWindowStyle.Hidden,
            RedirectStandardInput = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true
        };
    }

    private static void SpawnCoordinatorProcess(string? coordinatorExecutable, string? coordinatorArgs)
    {
        var psi = CreateProcessStartInfo(coordinatorExecutable, coordinatorArgs);
        var proc = Process.Start(psi);
        if (proc == null)
        {
            throw new InvalidOperationException($"Failed to spawn coordinator daemon process '{psi.FileName} {psi.Arguments}'.");
        }
    }

    private static string ResolveEndpoint(string? customEndpoint)
    {
        if (!string.IsNullOrWhiteSpace(customEndpoint))
        {
            return customEndpoint;
        }

        return OperatingSystem.IsWindows()
            ? IpcConstants.GetNamedPipeName()
            : IpcConstants.GetUnixSocketPath();
    }
}
