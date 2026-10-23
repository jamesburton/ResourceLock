namespace ResourceLock.Core.Models;

public sealed record LockRequest(
    string Resource,
    string SessionId,
    int OwnerPid,
    int WaitSeconds = 0,
    int? TimeoutSeconds = null,
    string? Reason = null);
