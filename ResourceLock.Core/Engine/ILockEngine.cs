namespace ResourceLock.Core.Engine;

using ResourceLock.Core.Models;

public interface ILockEngine
{
    ValueTask<LockResult> AcquireAsync(LockRequest request, CancellationToken cancellationToken = default);
    ValueTask<LockResult> ReleaseAsync(string resource, string sessionId, CancellationToken cancellationToken = default);
    ValueTask<int> ReleaseAllForSessionAsync(string sessionId, CancellationToken cancellationToken = default);
    ValueTask<IReadOnlyList<LockRecord>> ListLocksAsync(CancellationToken cancellationToken = default);
    ValueTask<bool> EvictDeadPidAsync(int pid, CancellationToken cancellationToken = default);
}
