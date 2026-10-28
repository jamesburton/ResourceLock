namespace ResourceLock.Ipc;

using System;
using System.Collections.Concurrent;
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
/// Supports concurrent multiplexed in-flight requests matched by <see cref="IpcEnvelope.MessageId"/>.
/// </summary>
public sealed class IpcLockEngine : ILockEngine, IDisposable, IAsyncDisposable
{
    private readonly Stream _stream;
    private readonly SemaphoreSlim _writeGate = new(1, 1);
    private readonly ConcurrentDictionary<string, TaskCompletionSource<IpcEnvelope?>> _pendingRequests = new(StringComparer.Ordinal);
    private readonly CancellationTokenSource _disposeCts = new();
    private readonly object _readerSync = new();
    private Task? _readerTask;
    private bool _disposed;

    public IpcLockEngine(Stream stream)
    {
        _stream = stream ?? throw new ArgumentNullException(nameof(stream));
    }

    private void EnsureReaderStarted()
    {
        if (_readerTask != null) return;
        lock (_readerSync)
        {
            _readerTask ??= Task.Run(ReadLoopAsync);
        }
    }

    private async Task ReadLoopAsync()
    {
        try
        {
            while (!_disposeCts.Token.IsCancellationRequested)
            {
                var response = await IpcFrameCodec.ReadFrameAsync(_stream, _disposeCts.Token).ConfigureAwait(false);
                if (response == null)
                {
                    break;
                }

                if (_pendingRequests.TryRemove(response.MessageId, out var tcs))
                {
                    tcs.TrySetResult(response);
                }
            }
        }
        catch
        {
            // Stream closed or disposed
        }
        finally
        {
            foreach (var kvp in _pendingRequests)
            {
                if (_pendingRequests.TryRemove(kvp.Key, out var tcs))
                {
                    tcs.TrySetResult(null);
                }
            }
        }
    }

    private async Task<IpcEnvelope?> SendRequestAsync(IpcEnvelope envelope, CancellationToken cancellationToken)
    {
        EnsureReaderStarted();

        var tcs = new TaskCompletionSource<IpcEnvelope?>(TaskCreationOptions.RunContinuationsAsynchronously);
        _pendingRequests[envelope.MessageId] = tcs;

        using var reg = cancellationToken.CanBeCanceled
            ? cancellationToken.Register(() =>
            {
                if (_pendingRequests.TryRemove(envelope.MessageId, out var removed))
                {
                    removed.TrySetCanceled(cancellationToken);
                }
            })
            : default;

        try
        {
            await _writeGate.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                await IpcFrameCodec.WriteFrameAsync(_stream, envelope, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                _writeGate.Release();
            }

            return await tcs.Task.ConfigureAwait(false);
        }
        catch
        {
            _pendingRequests.TryRemove(envelope.MessageId, out _);
            throw;
        }
    }

    public async ValueTask<LockResult> AcquireAsync(LockRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        var msgId = Guid.NewGuid().ToString("N");
        var envelope = new IpcEnvelope(
            msgId,
            IpcMessageType.Acquire,
            JsonSerializer.Serialize(request, IpcFrameCodec.DefaultJsonOptions));

        var response = await SendRequestAsync(envelope, cancellationToken).ConfigureAwait(false);
        if (response?.PayloadJson != null)
        {
            var result = JsonSerializer.Deserialize<LockResult>(response.PayloadJson, IpcFrameCodec.DefaultJsonOptions);
            if (result != null) return result;
        }

        return new LockResult(false, request.Resource, request.SessionId, request.OwnerPid, "No response from coordinator", null);
    }

    public async ValueTask<LockResult> ReleaseAsync(string resource, string sessionId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(resource);
        var msgId = Guid.NewGuid().ToString("N");
        var payload = JsonSerializer.Serialize(new { resource, sessionId }, IpcFrameCodec.DefaultJsonOptions);
        var envelope = new IpcEnvelope(msgId, IpcMessageType.Release, payload);

        var response = await SendRequestAsync(envelope, cancellationToken).ConfigureAwait(false);
        if (response?.PayloadJson != null)
        {
            var result = JsonSerializer.Deserialize<LockResult>(response.PayloadJson, IpcFrameCodec.DefaultJsonOptions);
            if (result != null) return result;
        }

        return new LockResult(false, resource, sessionId, null, "No response from coordinator", null);
    }

    public async ValueTask<int> ReleaseAllForSessionAsync(string sessionId, CancellationToken cancellationToken = default)
    {
        var msgId = Guid.NewGuid().ToString("N");
        var payload = JsonSerializer.Serialize(new { sessionId }, IpcFrameCodec.DefaultJsonOptions);
        var envelope = new IpcEnvelope(msgId, IpcMessageType.ReleaseAll, payload);

        var response = await SendRequestAsync(envelope, cancellationToken).ConfigureAwait(false);
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

    public async ValueTask<IReadOnlyList<LockRecord>> ListLocksAsync(CancellationToken cancellationToken = default)
    {
        var msgId = Guid.NewGuid().ToString("N");
        var envelope = new IpcEnvelope(msgId, IpcMessageType.List);

        var response = await SendRequestAsync(envelope, cancellationToken).ConfigureAwait(false);
        if (response?.PayloadJson != null)
        {
            var list = JsonSerializer.Deserialize<IReadOnlyList<LockRecord>>(response.PayloadJson, IpcFrameCodec.DefaultJsonOptions);
            if (list != null) return list;
        }

        return Array.Empty<LockRecord>();
    }

    public ValueTask<bool> EvictDeadPidAsync(int pid, CancellationToken cancellationToken = default)
    {
        return ValueTask.FromResult(false);
    }

    public async Task RegisterSessionAsync(string sessionId, int ownerPid, CancellationToken cancellationToken = default)
    {
        var msgId = Guid.NewGuid().ToString("N");
        var payload = JsonSerializer.Serialize(new SessionInfo(sessionId, ownerPid, DateTimeOffset.UtcNow), IpcFrameCodec.DefaultJsonOptions);
        var envelope = new IpcEnvelope(msgId, IpcMessageType.RegisterSession, payload);

        await SendRequestAsync(envelope, cancellationToken).ConfigureAwait(false);
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;
            _disposeCts.Cancel();
            _stream.Dispose();
            _writeGate.Dispose();
            _disposeCts.Dispose();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (!_disposed)
        {
            _disposed = true;
            _disposeCts.Cancel();
            await _stream.DisposeAsync().ConfigureAwait(false);
            if (_readerTask != null)
            {
                try
                {
                    await _readerTask.ConfigureAwait(false);
                }
                catch
                {
                }
            }
            _writeGate.Dispose();
            _disposeCts.Dispose();
        }
    }
}
