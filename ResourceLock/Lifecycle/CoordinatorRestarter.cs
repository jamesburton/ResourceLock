namespace ResourceLock.Lifecycle;

using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using ResourceLock.Core.Ipc;
using ResourceLock.Core.Models;

/// <summary>
/// Implements <c>resourcelock coordinator --restart [--force]</c>: asks the running daemon to shut
/// down gracefully, waits for its endpoint to disappear, then starts a fresh daemon.
/// Held locks are not handed over; clients learn about the revocation via their reconnect notice.
/// </summary>
public static class CoordinatorRestarter
{
    public const int ExitOk = 0;
    public const int ExitFailed = 1;
    public const int ExitLocksHeld = 2;

    public static Task<int> RestartAsync(bool force, TextWriter output, TextWriter error, CancellationToken cancellationToken = default)
        => RestartAsync(
            endpoint: null,
            force,
            output,
            error,
            ct => DaemonSpawner.EnsureCoordinatorAndConnectAsync(ct),
            TimeSpan.FromSeconds(10),
            cancellationToken);

    public static async Task<int> RestartAsync(
        string? endpoint,
        bool force,
        TextWriter output,
        TextWriter error,
        Func<CancellationToken, Task<Stream>> startFresh,
        TimeSpan shutdownTimeout,
        CancellationToken cancellationToken = default)
    {
        endpoint ??= OperatingSystem.IsWindows() ? IpcConstants.GetNamedPipeName() : IpcConstants.GetUnixSocketPath();

        var stream = await DaemonSpawner.TryConnectAsync(endpoint, 1000, cancellationToken).ConfigureAwait(false);
        if (stream == null)
        {
            await output.WriteLineAsync("No coordinator is running; starting a fresh one.").ConfigureAwait(false);
        }
        else
        {
            await using (stream.ConfigureAwait(false))
            {
                try
                {
                    var current = await RequestLocksAsync(stream, IpcMessageType.List, "list", cancellationToken).ConfigureAwait(false);
                    if (current.Count > 0 && !force)
                    {
                        await error.WriteLineAsync($"Refusing to restart: {current.Count} lock(s) are held and would be revoked:").ConfigureAwait(false);
                        await PrintLocksAsync(error, current).ConfigureAwait(false);
                        await error.WriteLineAsync("Re-run with --force to restart anyway.").ConfigureAwait(false);
                        return ExitLocksHeld;
                    }

                    var revoked = await RequestLocksAsync(stream, IpcMessageType.Shutdown, "shutdown", cancellationToken).ConfigureAwait(false);
                    if (revoked.Count > 0)
                    {
                        await output.WriteLineAsync($"Revoking {revoked.Count} lock(s):").ConfigureAwait(false);
                        await PrintLocksAsync(output, revoked).ConfigureAwait(false);
                    }
                }
                catch (Exception ex) when (ex is IOException or JsonException or InvalidOperationException)
                {
                    await error.WriteLineAsync($"Failed to communicate with coordinator: {ex.Message}").ConfigureAwait(false);
                    return ExitFailed;
                }
            }

            var deadline = DateTime.UtcNow + shutdownTimeout;
            while (true)
            {
                var probe = await DaemonSpawner.TryConnectAsync(endpoint, 100, cancellationToken).ConfigureAwait(false);
                if (probe == null)
                {
                    break;
                }
                await probe.DisposeAsync().ConfigureAwait(false);
                if (DateTime.UtcNow >= deadline)
                {
                    await error.WriteLineAsync("Timed out waiting for the coordinator to stop.").ConfigureAwait(false);
                    return ExitFailed;
                }
                await Task.Delay(100, cancellationToken).ConfigureAwait(false);
            }
        }

        try
        {
            var fresh = await startFresh(cancellationToken).ConfigureAwait(false);
            await fresh.DisposeAsync().ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is TimeoutException or IOException or InvalidOperationException)
        {
            await error.WriteLineAsync($"Failed to start a fresh coordinator: {ex.Message}").ConfigureAwait(false);
            return ExitFailed;
        }

        await output.WriteLineAsync("Coordinator restarted.").ConfigureAwait(false);
        return ExitOk;
    }

    private static async Task<IReadOnlyList<LockRecord>> RequestLocksAsync(
        Stream stream, IpcMessageType type, string id, CancellationToken ct)
    {
        await IpcFrameCodec.WriteFrameAsync(stream, new IpcEnvelope($"restart-{id}", type), ct).ConfigureAwait(false);
        var resp = await IpcFrameCodec.ReadFrameAsync(stream, ct).ConfigureAwait(false)
                   ?? throw new EndOfStreamException("Coordinator closed the connection.");
        if (string.IsNullOrEmpty(resp.PayloadJson))
        {
            return Array.Empty<LockRecord>();
        }

        if (type == IpcMessageType.List)
        {
            return JsonSerializer.Deserialize<List<LockRecord>>(resp.PayloadJson, IpcFrameCodec.DefaultJsonOptions)
                   ?? new List<LockRecord>();
        }

        using var doc = JsonDocument.Parse(resp.PayloadJson);
        if (doc.RootElement.TryGetProperty("heldLocks", out var held))
        {
            return held.Deserialize<List<LockRecord>>(IpcFrameCodec.DefaultJsonOptions) ?? new List<LockRecord>();
        }
        return Array.Empty<LockRecord>();
    }

    private static async Task PrintLocksAsync(TextWriter writer, IEnumerable<LockRecord> locks)
    {
        foreach (var l in locks)
        {
            await writer.WriteLineAsync($"  {l.Resource}  session={l.SessionId}  pid={l.OwnerPid}  since={l.AcquiredAt:u}" +
                                        (string.IsNullOrEmpty(l.Reason) ? "" : $"  reason={l.Reason}")).ConfigureAwait(false);
        }
    }
}
