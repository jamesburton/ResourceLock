namespace ResourceLock.Core.Engine;

using System.Diagnostics;
using ResourceLock.Core.Models;

public sealed class InMemoryLockEngine : ILockEngine, IDisposable
{
    private sealed class ActiveLock
    {
        public required LockRecord Record { get; set; }
        public CancellationTokenSource? ExpirationCts { get; set; }
    }

    private sealed class Waiter
    {
        public required LockRequest Request { get; init; }
        public required TaskCompletionSource<LockResult> Tcs { get; init; }
        public required DateTimeOffset EnqueuedAt { get; init; }
        public CancellationTokenSource? WaitTimeoutCts { get; set; }
        public CancellationTokenRegistration CancellationReg { get; set; }
        public bool IsCompleted { get; set; }
    }

    private readonly object _gate = new();
    private readonly Dictionary<string, ActiveLock> _locks = new();
    private readonly Dictionary<string, LinkedList<Waiter>> _waiters = new();
    private readonly TimeProvider _timeProvider;
    private readonly Func<int, bool> _isPidDead;
    private bool _isDisposed;

    public InMemoryLockEngine(TimeProvider? timeProvider = null, Func<int, bool>? isPidDead = null)
    {
        _timeProvider = timeProvider ?? TimeProvider.System;
        _isPidDead = isPidDead ?? DefaultIsPidDead;
    }

    public ValueTask<LockResult> AcquireAsync(LockRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        var resource = NormalizeResource(request.Resource);

        if (cancellationToken.IsCancellationRequested)
        {
            return ValueTask.FromCanceled<LockResult>(cancellationToken);
        }

        lock (_gate)
        {
            ThrowIfDisposed();

            // Check if active lock is expired or held by a dead process
            ActiveLock? activeLock;
            while (_locks.TryGetValue(resource, out activeLock) &&
                   (IsExpired(activeLock.Record) || _isPidDead(activeLock.Record.OwnerPid)))
            {
                EvictActiveLockInternal(resource, activeLock);
                PromoteNextWaiterInternal(resource);
            }
            _locks.TryGetValue(resource, out activeLock);

            var hasWaiters = _waiters.TryGetValue(resource, out var queue) && queue.Count > 0;

            // If free and no queue ahead
            if (activeLock == null && !hasWaiters)
            {
                var granted = GrantLockInternal(request, resource);
                return ValueTask.FromResult(granted);
            }

            // If lock held by same session, allow re-entrancy / update
            if (activeLock != null && activeLock.Record.SessionId == request.SessionId)
            {
                if (!request.Renew)
                {
                    var existingReason = string.IsNullOrWhiteSpace(activeLock.Record.Reason)
                        ? "unspecified"
                        : activeLock.Record.Reason;

                    return ValueTask.FromResult(new LockResult(
                        false,
                        resource,
                        request.SessionId,
                        activeLock.Record.OwnerPid,
                        $"Resource '{resource}' is already locked by this session (reason: '{existingReason}'). If you already acquired this lock earlier in your task, you do not need to acquire it again. If you are a parallel subagent or lane sharing this session, retry acquire_lock with a unique 'owner' identifier (e.g., owner: 'lane-2') so your request queues properly.",
                        activeLock.Record.ExpiresAt,
                        activeLock.Record));
                }

                activeLock.ExpirationCts?.Dispose();
                activeLock.ExpirationCts = null;

                var now = _timeProvider.GetUtcNow();
                DateTimeOffset? expiresAt = request.TimeoutSeconds is > 0
                    ? now.AddSeconds(request.TimeoutSeconds.Value)
                    : null;

                var updatedRecord = new LockRecord(
                    resource,
                    request.SessionId,
                    request.OwnerPid,
                    now,
                    expiresAt,
                    request.Reason);

                activeLock.Record = updatedRecord;
                ScheduleExpiration(resource, activeLock, request.TimeoutSeconds);

                return ValueTask.FromResult(new LockResult(
                    true,
                    resource,
                    request.SessionId,
                    request.OwnerPid,
                    "Lock acquired successfully.",
                    expiresAt));
            }

            // Lock is held by another session or has queued waiters ahead
            var currentHolder = activeLock?.Record;

            if (request.WaitSeconds <= 0)
            {
                return ValueTask.FromResult(new LockResult(
                    false,
                    resource,
                    null,
                    null,
                    "Resource is held by another process.",
                    currentHolder?.ExpiresAt,
                    currentHolder));
            }

            // WaitSeconds > 0: queue waiter
            if (queue == null)
            {
                queue = new LinkedList<Waiter>();
                _waiters[resource] = queue;
            }

            var tcs = new TaskCompletionSource<LockResult>(TaskCreationOptions.RunContinuationsAsynchronously);
            var waiter = new Waiter
            {
                Request = request,
                Tcs = tcs,
                EnqueuedAt = _timeProvider.GetUtcNow()
            };

            var node = queue.AddLast(waiter);

            // Wait timeout
            var waitTimeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(request.WaitSeconds), _timeProvider);
            waiter.WaitTimeoutCts = waitTimeoutCts;

            waitTimeoutCts.Token.Register(() =>
            {
                lock (_gate)
                {
                    if (waiter.IsCompleted) return;
                    waiter.IsCompleted = true;
                    queue.Remove(node);
                    if (queue.Count == 0) _waiters.Remove(resource);
                    waiter.CancellationReg.Unregister();

                    var holder = _locks.TryGetValue(resource, out var al) ? al.Record : null;
                    waiter.Tcs.TrySetResult(new LockResult(
                        false,
                        resource,
                        null,
                        null,
                        "Lock acquisition timed out waiting in queue.",
                        holder?.ExpiresAt,
                        holder));
                }
            });

            // Caller cancellation
            if (cancellationToken.CanBeCanceled)
            {
                waiter.CancellationReg = cancellationToken.Register(() =>
                {
                    lock (_gate)
                    {
                        if (waiter.IsCompleted) return;
                        waiter.IsCompleted = true;
                        queue.Remove(node);
                        if (queue.Count == 0) _waiters.Remove(resource);
                        waiter.WaitTimeoutCts?.Dispose();

                        waiter.Tcs.TrySetCanceled(cancellationToken);
                    }
                });
            }

            return new ValueTask<LockResult>(tcs.Task);
        }
    }

    public ValueTask<LockResult> ReleaseAsync(string resource, string sessionId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(resource);
        ArgumentException.ThrowIfNullOrWhiteSpace(sessionId);
        var normalized = NormalizeResource(resource);

        lock (_gate)
        {
            ThrowIfDisposed();

            if (!_locks.TryGetValue(normalized, out var activeLock))
            {
                return ValueTask.FromResult(new LockResult(
                    false,
                    normalized,
                    sessionId,
                    null,
                    $"Lock on '{normalized}' is not currently held.",
                    null));
            }

            if (activeLock.Record.SessionId != sessionId)
            {
                return ValueTask.FromResult(new LockResult(
                    false,
                    normalized,
                    sessionId,
                    activeLock.Record.OwnerPid,
                    $"Lock is not held by this session.",
                    activeLock.Record.ExpiresAt,
                    activeLock.Record));
            }

            EvictActiveLockInternal(normalized, activeLock);
            PromoteNextWaiterInternal(normalized);

            return ValueTask.FromResult(new LockResult(
                true,
                normalized,
                sessionId,
                activeLock.Record.OwnerPid,
                $"Lock on '{normalized}' released.",
                null));
        }
    }

    public ValueTask<int> ReleaseAllForSessionAsync(string sessionId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sessionId);

        lock (_gate)
        {
            ThrowIfDisposed();

            var releasedCount = 0;
            var toRelease = new List<string>();

            foreach (var (res, al) in _locks)
            {
                if (al.Record.SessionId == sessionId)
                {
                    toRelease.Add(res);
                }
            }

            foreach (var res in toRelease)
            {
                if (_locks.TryGetValue(res, out var al))
                {
                    EvictActiveLockInternal(res, al);
                    PromoteNextWaiterInternal(res);
                    releasedCount++;
                }
            }

            // Also remove any queued waiters from this session
            var emptyQueues = new List<string>();
            foreach (var (res, queue) in _waiters)
            {
                var current = queue.First;
                while (current != null)
                {
                    var next = current.Next;
                    if (current.Value.Request.SessionId == sessionId)
                    {
                        var waiter = current.Value;
                        if (!waiter.IsCompleted)
                        {
                            waiter.IsCompleted = true;
                            waiter.CancellationReg.Unregister();
                            waiter.WaitTimeoutCts?.Dispose();
                            waiter.Tcs.TrySetResult(new LockResult(
                                false,
                                res,
                                sessionId,
                                null,
                                "Session released all locks and waiter was cancelled.",
                                null));
                        }
                        queue.Remove(current);
                    }
                    current = next;
                }

                if (queue.Count == 0)
                {
                    emptyQueues.Add(res);
                }
            }

            foreach (var res in emptyQueues)
            {
                _waiters.Remove(res);
            }

            return ValueTask.FromResult(releasedCount);
        }
    }

    public ValueTask<IReadOnlyList<LockRecord>> ListLocksAsync(CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            ThrowIfDisposed();

            // Purge expired locks
            var expired = new List<string>();
            foreach (var (res, al) in _locks)
            {
                if (IsExpired(al.Record))
                {
                    expired.Add(res);
                }
            }

            foreach (var res in expired)
            {
                if (_locks.TryGetValue(res, out var al))
                {
                    EvictActiveLockInternal(res, al);
                    PromoteNextWaiterInternal(res);
                }
            }

            var result = _locks.Values.Select(l => l.Record).ToList();
            return ValueTask.FromResult<IReadOnlyList<LockRecord>>(result);
        }
    }

    public ValueTask<bool> EvictDeadPidAsync(int pid, CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            ThrowIfDisposed();

            if (!_isPidDead(pid))
            {
                return ValueTask.FromResult(false);
            }

            var evictedAny = false;
            var toEvict = new List<string>();

            foreach (var (res, al) in _locks)
            {
                if (al.Record.OwnerPid == pid)
                {
                    toEvict.Add(res);
                }
            }

            foreach (var res in toEvict)
            {
                if (_locks.TryGetValue(res, out var al))
                {
                    EvictActiveLockInternal(res, al);
                    PromoteNextWaiterInternal(res);
                    evictedAny = true;
                }
            }

            // Also clean up any waiters with this PID
            var emptyQueues = new List<string>();
            foreach (var (res, queue) in _waiters)
            {
                var current = queue.First;
                while (current != null)
                {
                    var next = current.Next;
                    if (current.Value.Request.OwnerPid == pid)
                    {
                        var waiter = current.Value;
                        if (!waiter.IsCompleted)
                        {
                            waiter.IsCompleted = true;
                            waiter.CancellationReg.Unregister();
                            waiter.WaitTimeoutCts?.Dispose();
                            waiter.Tcs.TrySetResult(new LockResult(
                                false,
                                res,
                                waiter.Request.SessionId,
                                pid,
                                "Owner PID is dead, waiter evicted.",
                                null));
                            evictedAny = true;
                        }
                        queue.Remove(current);
                    }
                    current = next;
                }

                if (queue.Count == 0)
                {
                    emptyQueues.Add(res);
                }
            }

            foreach (var res in emptyQueues)
            {
                _waiters.Remove(res);
            }

            return ValueTask.FromResult(evictedAny);
        }
    }

    public void Dispose()
    {
        lock (_gate)
        {
            if (_isDisposed) return;
            _isDisposed = true;

            foreach (var al in _locks.Values)
            {
                al.ExpirationCts?.Dispose();
                al.ExpirationCts = null;
            }
            _locks.Clear();

            foreach (var queue in _waiters.Values)
            {
                foreach (var waiter in queue)
                {
                    if (!waiter.IsCompleted)
                    {
                        waiter.IsCompleted = true;
                        waiter.CancellationReg.Unregister();
                        waiter.WaitTimeoutCts?.Dispose();
                        waiter.Tcs.TrySetResult(new LockResult(
                            false,
                            waiter.Request.Resource,
                            waiter.Request.SessionId,
                            waiter.Request.OwnerPid,
                            "Lock engine disposed.",
                            null));
                    }
                }
            }
            _waiters.Clear();
        }
    }

    private LockResult GrantLockInternal(LockRequest request, string resource)
    {
        var now = _timeProvider.GetUtcNow();
        DateTimeOffset? expiresAt = request.TimeoutSeconds is > 0
            ? now.AddSeconds(request.TimeoutSeconds.Value)
            : null;

        var record = new LockRecord(
            resource,
            request.SessionId,
            request.OwnerPid,
            now,
            expiresAt,
            request.Reason);

        var activeLock = new ActiveLock
        {
            Record = record
        };

        _locks[resource] = activeLock;
        ScheduleExpiration(resource, activeLock, request.TimeoutSeconds);

        return new LockResult(
            true,
            resource,
            request.SessionId,
            request.OwnerPid,
            "Lock acquired successfully.",
            expiresAt);
    }

    private void PromoteNextWaiterInternal(string resource)
    {
        if (!_waiters.TryGetValue(resource, out var queue))
        {
            return;
        }

        while (queue.First != null)
        {
            var node = queue.First;
            var waiter = node.Value;
            queue.RemoveFirst();

            if (waiter.IsCompleted)
            {
                continue;
            }

            // Skip waiters whose process died while queued
            if (_isPidDead(waiter.Request.OwnerPid))
            {
                waiter.IsCompleted = true;
                waiter.CancellationReg.Unregister();
                waiter.WaitTimeoutCts?.Dispose();
                waiter.Tcs.TrySetResult(new LockResult(
                    false,
                    resource,
                    null,
                    null,
                    "Process terminated while waiting.",
                    null));
                continue;
            }

            waiter.IsCompleted = true;
            waiter.CancellationReg.Unregister();
            waiter.WaitTimeoutCts?.Dispose();

            var result = GrantLockInternal(waiter.Request, resource);
            waiter.Tcs.TrySetResult(result);
            break;
        }

        if (queue.Count == 0)
        {
            _waiters.Remove(resource);
        }
    }

    private void ScheduleExpiration(string resource, ActiveLock activeLock, int? timeoutSeconds)
    {
        if (timeoutSeconds is null or <= 0)
        {
            return;
        }

        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(timeoutSeconds.Value), _timeProvider);
        activeLock.ExpirationCts = cts;

        cts.Token.Register(() =>
        {
            lock (_gate)
            {
                if (_isDisposed) return;
                if (_locks.TryGetValue(resource, out var current) && 
                    ReferenceEquals(current, activeLock) && 
                    ReferenceEquals(current.ExpirationCts, cts))
                {
                    EvictActiveLockInternal(resource, current);
                    PromoteNextWaiterInternal(resource);
                }
            }
        });
    }

    private void EvictActiveLockInternal(string resource, ActiveLock activeLock)
    {
        _locks.Remove(resource);
        activeLock.ExpirationCts?.Dispose();
        activeLock.ExpirationCts = null;
    }

    private bool IsExpired(LockRecord record)
    {
        return record.ExpiresAt.HasValue && record.ExpiresAt.Value <= _timeProvider.GetUtcNow();
    }

    private static bool DefaultIsPidDead(int pid)
    {
        if (pid <= 0) return true;
        try
        {
            using var process = Process.GetProcessById(pid);
            return process.HasExited;
        }
        catch (ArgumentException)
        {
            return true;
        }
        catch (InvalidOperationException)
        {
            return true;
        }
        catch
        {
            return false;
        }
    }

    private static string NormalizeResource(string resource)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(resource);
        return resource.Trim().ToLowerInvariant();
    }

    private void ThrowIfDisposed()
    {
        ObjectDisposedException.ThrowIf(_isDisposed, this);
    }
}
