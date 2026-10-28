namespace ResourceLock.Coordinator.Transports;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using ResourceLock.Coordinator.Federation;
using ResourceLock.Core.Engine;
using ResourceLock.Core.Ipc;
using ResourceLock.Core.Models;

public readonly record struct RemoteLockKey(string Host, string Resource, string SessionId);

public sealed class IpcConnectionHandler
{
    private readonly ILockEngine _lockEngine;
    private readonly IRemoteCoordinatorClient _remoteCoordinatorClient;

    public IpcConnectionHandler(ILockEngine lockEngine, IRemoteCoordinatorClient? remoteCoordinatorClient = null)
    {
        _lockEngine = lockEngine ?? throw new ArgumentNullException(nameof(lockEngine));
        _remoteCoordinatorClient = remoteCoordinatorClient ?? new RemoteCoordinatorClient();
    }

    private sealed class ConnectionContext
    {
        public object SyncRoot { get; } = new();
        public HashSet<string> SessionIds { get; } = new(StringComparer.Ordinal);
        public string? DefaultSessionId { get; set; }
        public int? DefaultPid { get; set; }
        public HashSet<RemoteLockKey> RemoteLocks { get; } = new();
    }

    public async Task HandleAsync(Stream stream, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(stream);

        var context = new ConnectionContext();
        using var writeGate = new SemaphoreSlim(1, 1);
        var inFlightTasks = new ConcurrentDictionary<Task, byte>();

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                IpcEnvelope? envelope;
                try
                {
                    envelope = await IpcFrameCodec.ReadFrameAsync(stream, cancellationToken).ConfigureAwait(false);
                }
                catch (EndOfStreamException)
                {
                    break;
                }
                catch (IOException)
                {
                    break;
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception)
                {
                    break;
                }

                if (envelope == null)
                {
                    // EOF reached
                    break;
                }

                var task = ProcessEnvelopeAsync(stream, writeGate, envelope, context, cancellationToken);
                if (!task.IsCompleted)
                {
                    inFlightTasks.TryAdd(task, 0);
                    _ = task.ContinueWith(
                        t => inFlightTasks.TryRemove(t, out _),
                        CancellationToken.None,
                        TaskContinuationOptions.ExecuteSynchronously,
                        TaskScheduler.Default);
                }
            }
        }
        finally
        {
            var pending = inFlightTasks.Keys.ToArray();
            if (pending.Length > 0)
            {
                try
                {
                    await Task.WhenAll(pending).ConfigureAwait(false);
                }
                catch
                {
                }
            }

            string[] sessionIdsToRelease;
            RemoteLockKey[] remoteLocksToRelease;
            lock (context.SyncRoot)
            {
                if (!string.IsNullOrEmpty(context.DefaultSessionId))
                {
                    context.SessionIds.Add(context.DefaultSessionId);
                }
                sessionIdsToRelease = context.SessionIds.ToArray();
                remoteLocksToRelease = context.RemoteLocks.ToArray();
                context.RemoteLocks.Clear();
            }

            foreach (var sessionId in sessionIdsToRelease)
            {
                try
                {
                    await _lockEngine.ReleaseAllForSessionAsync(sessionId, CancellationToken.None).ConfigureAwait(false);
                }
                catch
                {
                    // Ignore exceptions during disconnect cleanup
                }
            }

            foreach (var remoteLock in remoteLocksToRelease)
            {
                try
                {
                    await _remoteCoordinatorClient.ForwardReleaseAsync(
                        remoteLock.Host,
                        remoteLock.Resource,
                        remoteLock.SessionId,
                        CancellationToken.None).ConfigureAwait(false);
                }
                catch
                {
                    // Ignore exceptions during disconnect cleanup
                }
            }
        }
    }

    private static async Task WriteFrameSafeAsync(
        Stream stream,
        SemaphoreSlim writeGate,
        IpcEnvelope envelope,
        CancellationToken cancellationToken)
    {
        await writeGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await IpcFrameCodec.WriteFrameAsync(stream, envelope, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            writeGate.Release();
        }
    }

    private async Task ProcessEnvelopeAsync(
        Stream stream,
        SemaphoreSlim writeGate,
        IpcEnvelope envelope,
        ConnectionContext context,
        CancellationToken cancellationToken)
    {
        switch (envelope.Type)
        {
            case IpcMessageType.RegisterSession:
            {
                if (!string.IsNullOrEmpty(envelope.PayloadJson))
                {
                    var session = JsonSerializer.Deserialize<SessionInfo>(envelope.PayloadJson, IpcFrameCodec.DefaultJsonOptions);
                    if (session != null)
                    {
                        lock (context.SyncRoot)
                        {
                            context.DefaultSessionId = session.SessionId;
                            context.DefaultPid = session.OwnerPid;
                            context.SessionIds.Add(session.SessionId);
                        }
                    }
                }

                var resp = new IpcEnvelope(
                    envelope.MessageId,
                    IpcMessageType.Response,
                    JsonSerializer.Serialize(new { success = true }, IpcFrameCodec.DefaultJsonOptions));
                await WriteFrameSafeAsync(stream, writeGate, resp, cancellationToken).ConfigureAwait(false);
                break;
            }

            case IpcMessageType.Acquire:
            {
                string? defaultSessionId;
                int? defaultPid;
                lock (context.SyncRoot)
                {
                    defaultSessionId = context.DefaultSessionId;
                    defaultPid = context.DefaultPid;
                }

                if (string.IsNullOrEmpty(envelope.PayloadJson))
                {
                    var errorResult = new LockResult(false, "", defaultSessionId, defaultPid, "Payload is required for Acquire", null);
                    var errEnvelope = new IpcEnvelope(envelope.MessageId, IpcMessageType.Response, JsonSerializer.Serialize(errorResult, IpcFrameCodec.DefaultJsonOptions));
                    await WriteFrameSafeAsync(stream, writeGate, errEnvelope, cancellationToken).ConfigureAwait(false);
                    return;
                }

                var request = JsonSerializer.Deserialize<LockRequest>(envelope.PayloadJson, IpcFrameCodec.DefaultJsonOptions);
                if (request == null)
                {
                    var errorResult = new LockResult(false, "", defaultSessionId, defaultPid, "Invalid LockRequest payload", null);
                    var errEnvelope = new IpcEnvelope(envelope.MessageId, IpcMessageType.Response, JsonSerializer.Serialize(errorResult, IpcFrameCodec.DefaultJsonOptions));
                    await WriteFrameSafeAsync(stream, writeGate, errEnvelope, cancellationToken).ConfigureAwait(false);
                    return;
                }

                lock (context.SyncRoot)
                {
                    context.SessionIds.Add(request.SessionId);
                    context.DefaultSessionId ??= request.SessionId;
                    context.DefaultPid ??= request.OwnerPid;
                }

                var parsed = RemoteResourceParser.Parse(request.Resource);
                LockResult result;
                if (parsed.Host != null)
                {
                    result = await _remoteCoordinatorClient.ForwardAcquireAsync(parsed.Host, request, cancellationToken).ConfigureAwait(false);
                    if (result.Success)
                    {
                        lock (context.SyncRoot)
                        {
                            context.RemoteLocks.Add(new RemoteLockKey(parsed.Host, parsed.Resource, request.SessionId));
                        }
                    }
                }
                else
                {
                    result = await _lockEngine.AcquireAsync(request, cancellationToken).ConfigureAwait(false);
                }

                var resp = new IpcEnvelope(
                    envelope.MessageId,
                    IpcMessageType.Response,
                    JsonSerializer.Serialize(result, IpcFrameCodec.DefaultJsonOptions));
                await WriteFrameSafeAsync(stream, writeGate, resp, cancellationToken).ConfigureAwait(false);
                break;
            }

            case IpcMessageType.Release:
            {
                string? resource = null;
                string? sessionId;
                int? defaultPid;
                lock (context.SyncRoot)
                {
                    sessionId = context.DefaultSessionId;
                    defaultPid = context.DefaultPid;
                }

                if (!string.IsNullOrEmpty(envelope.PayloadJson))
                {
                    using var doc = JsonDocument.Parse(envelope.PayloadJson);
                    if (doc.RootElement.TryGetProperty("resource", out var resProp) || doc.RootElement.TryGetProperty("Resource", out resProp))
                    {
                        resource = resProp.GetString();
                    }
                    if (doc.RootElement.TryGetProperty("sessionId", out var sessProp) || doc.RootElement.TryGetProperty("SessionId", out sessProp))
                    {
                        sessionId = sessProp.GetString() ?? sessionId;
                    }
                }

                if (string.IsNullOrEmpty(resource))
                {
                    var errorResult = new LockResult(false, "", sessionId, defaultPid, "Resource is required for Release", null);
                    var errEnvelope = new IpcEnvelope(envelope.MessageId, IpcMessageType.Response, JsonSerializer.Serialize(errorResult, IpcFrameCodec.DefaultJsonOptions));
                    await WriteFrameSafeAsync(stream, writeGate, errEnvelope, cancellationToken).ConfigureAwait(false);
                    return;
                }

                var parsed = RemoteResourceParser.Parse(resource);
                LockResult result;
                if (parsed.Host != null)
                {
                    result = await _remoteCoordinatorClient.ForwardReleaseAsync(parsed.Host, parsed.Resource, sessionId ?? "", cancellationToken).ConfigureAwait(false);
                    if (result.Success)
                    {
                        lock (context.SyncRoot)
                        {
                            context.RemoteLocks.Remove(new RemoteLockKey(parsed.Host, parsed.Resource, sessionId ?? ""));
                        }
                    }
                }
                else
                {
                    result = await _lockEngine.ReleaseAsync(resource, sessionId ?? "", cancellationToken).ConfigureAwait(false);
                }

                var resp = new IpcEnvelope(
                    envelope.MessageId,
                    IpcMessageType.Response,
                    JsonSerializer.Serialize(result, IpcFrameCodec.DefaultJsonOptions));
                await WriteFrameSafeAsync(stream, writeGate, resp, cancellationToken).ConfigureAwait(false);
                break;
            }

            case IpcMessageType.ReleaseAll:
            {
                string? sessionId;
                lock (context.SyncRoot)
                {
                    sessionId = context.DefaultSessionId;
                }

                if (!string.IsNullOrEmpty(envelope.PayloadJson))
                {
                    using var doc = JsonDocument.Parse(envelope.PayloadJson);
                    if (doc.RootElement.TryGetProperty("sessionId", out var sessProp) || doc.RootElement.TryGetProperty("SessionId", out sessProp))
                    {
                        sessionId = sessProp.GetString() ?? sessionId;
                    }
                }

                int count = 0;
                if (!string.IsNullOrEmpty(sessionId))
                {
                    count = await _lockEngine.ReleaseAllForSessionAsync(sessionId, cancellationToken).ConfigureAwait(false);

                    List<RemoteLockKey> sessionRemoteLocks;
                    lock (context.SyncRoot)
                    {
                        sessionRemoteLocks = context.RemoteLocks.Where(l => l.SessionId == sessionId).ToList();
                    }

                    foreach (var remoteLock in sessionRemoteLocks)
                    {
                        try
                        {
                            var releaseResult = await _remoteCoordinatorClient.ForwardReleaseAsync(
                                remoteLock.Host,
                                remoteLock.Resource,
                                remoteLock.SessionId,
                                cancellationToken).ConfigureAwait(false);

                            if (releaseResult.Success)
                            {
                                count++;
                                lock (context.SyncRoot)
                                {
                                    context.RemoteLocks.Remove(remoteLock);
                                }
                            }
                        }
                        catch
                        {
                            // Ignore exceptions during cleanup
                        }
                    }
                }

                var resp = new IpcEnvelope(
                    envelope.MessageId,
                    IpcMessageType.Response,
                    JsonSerializer.Serialize(new { count }, IpcFrameCodec.DefaultJsonOptions));
                await WriteFrameSafeAsync(stream, writeGate, resp, cancellationToken).ConfigureAwait(false);
                break;
            }

            case IpcMessageType.List:
            {
                var locks = await _lockEngine.ListLocksAsync(cancellationToken).ConfigureAwait(false);
                var resp = new IpcEnvelope(
                    envelope.MessageId,
                    IpcMessageType.Response,
                    JsonSerializer.Serialize(locks, IpcFrameCodec.DefaultJsonOptions));
                await WriteFrameSafeAsync(stream, writeGate, resp, cancellationToken).ConfigureAwait(false);
                break;
            }

            case IpcMessageType.Ping:
            {
                var resp = new IpcEnvelope(
                    envelope.MessageId,
                    IpcMessageType.Response,
                    JsonSerializer.Serialize(new { status = "pong" }, IpcFrameCodec.DefaultJsonOptions));
                await WriteFrameSafeAsync(stream, writeGate, resp, cancellationToken).ConfigureAwait(false);
                break;
            }

            default:
                break;
        }
    }
}
