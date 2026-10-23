namespace ResourceLock.Core.Models;

public sealed record LockRecord(
    string Resource,
    string SessionId,
    int OwnerPid,
    DateTimeOffset AcquiredAt,
    DateTimeOffset? ExpiresAt,
    string? Reason);
