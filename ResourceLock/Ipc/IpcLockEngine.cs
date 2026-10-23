namespace ResourceLock.Ipc;

using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using ResourceLock.Core.Engine;
using ResourceLock.Core.Ipc;
using ResourceLock.Core.Models;

/// <summary>
/// Implements <see cref="ILockEngine"/> over a duplex IPC stream connected to the Coordinator daemon.
/// </summary>
public sealed class IpcLockEngine : ILockEngine, IDisposable, IAsyncDisposable
{
    private readonly Stream _stream;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private bool _disposed;

    public IpcLockEngine(Stream stream)
    {
        _stream = stream ?? throw new ArgumentNullException(nameof(stream));
    }

    public async ValueTask<LockResult> AcquireAsync(LockRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var msgId = Guid.NewGuid().ToString("N");
            var envelope = new IpcEnvelope(
                msgId,
                IpcMessageType.Acquire,
                JsonSerializer.Serialize(request, IpcFrameCodec.DefaultJsonOptions));

            await IpcFrameCodec.WriteFrameAsync(_stream, envelope, cancellationToken).ConfigureAwait(false);
            var response = await IpcFrameCodec.ReadFrameAsync(_stream, cancellationToken).ConfigureAwait(false);

            if (response?.PayloadJson != null)
            {
                var result = JsonSerializer.Deserialize<LockResult>(response.PayloadJson, IpcFrameCodec.DefaultJsonOptions);
                if (result != null) return result;
            }

            return new LockResult(false, request.Resource, request.SessionId, request.OwnerPid, "No response from coordinator", null);
        }
        finally
        {
            _gate.Release();
        }
    }

    public async ValueTask<LockResult> ReleaseAsync(string resource, string sessionId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(resource);
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var msgId = Guid.NewGuid().ToString("N");
            var payload = JsonSerializer.Serialize(new { resource, sessionId }, IpcFrameCodec.DefaultJsonOptions);
            var envelope = new IpcEnvelope(msgId, IpcMessageType.Release, payload);

            await IpcFrameCodec.WriteFrameAsync(_stream, envelope, cancellationToken).ConfigureAwait(false);
            var response = await IpcFrameCodec.ReadFrameAsync(_stream, cancellationToken).ConfigureAwait(false);

            if (response?.PayloadJson != null)
            {
                var result = JsonSerializer.Deserialize<LockResult>(response.PayloadJson, IpcFrameCodec.DefaultJsonOptions);
                if (result != null) return result;
            }

            return new LockResult(false, resource, sessionId, null, "No response from coordinator", null);
        }
        finally
        {
            _gate.Release();
        }
    }

    public async ValueTask<int> ReleaseAllForSessionAsync(string sessionId, CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var msgId = Guid.NewGuid().ToString("N");
            var payload = JsonSerializer.Serialize(new { sessionId }, IpcFrameCodec.DefaultJsonOptions);
            var envelope = new IpcEnvelope(msgId, IpcMessageType.ReleaseAll, payload);

            await IpcFrameCodec.WriteFrameAsync(_stream, envelope, cancellationToken).ConfigureAwait(false);
            var response = await IpcFrameCodec.ReadFrameAsync(_stream, cancellationToken).ConfigureAwait(false);

            if (response?.PayloadJson != null)
            {
                using var doc = JsonDocument.Parse(response.PayloadJson);
                if (doc.RootElement.TryGetProperty("count", out var countProp) ||
                    doc.RootElement.TryGetProperty("Count", out countProp))
                {
                    return countProp.GetInt32();
                }
            }

            return 0;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async ValueTask<IReadOnlyList<LockRecord>> ListLocksAsync(CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var msgId = Guid.NewGuid().ToString("N");
            var envelope = new IpcEnvelope(msgId, IpcMessageType.List);

            await IpcFrameCodec.WriteFrameAsync(_stream, envelope, cancellationToken).ConfigureAwait(false);
            var response = await IpcFrameCodec.ReadFrameAsync(_stream, cancellationToken).ConfigureAwait(false);

            if (response?.PayloadJson != null)
            {
                var list = JsonSerializer.Deserialize<IReadOnlyList<LockRecord>>(response.PayloadJson, IpcFrameCodec.DefaultJsonOptions);
                if (list != null) return list;
            }

            return Array.Empty<LockRecord>();
        }
        finally
        {
            _gate.Release();
        }
    }

    public ValueTask<bool> EvictDeadPidAsync(int pid, CancellationToken cancellationToken = default)
    {
        return ValueTask.FromResult(false);
    }

    public async Task RegisterSessionAsync(string sessionId, int ownerPid, CancellationToken cancellationToken = default)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var msgId = Guid.NewGuid().ToString("N");
            var payload = JsonSerializer.Serialize(new SessionInfo(sessionId, ownerPid, DateTimeOffset.UtcNow), IpcFrameCodec.DefaultJsonOptions);
            var envelope = new IpcEnvelope(msgId, IpcMessageType.RegisterSession, payload);

            await IpcFrameCodec.WriteFrameAsync(_stream, envelope, cancellationToken).ConfigureAwait(false);
            await IpcFrameCodec.ReadFrameAsync(_stream, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _gate.Release();
        }
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;
            _gate.Dispose();
            _stream.Dispose();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (!_disposed)
        {
            _disposed = true;
            _gate.Dispose();
            await _stream.DisposeAsync().ConfigureAwait(false);
        }
    }
}
