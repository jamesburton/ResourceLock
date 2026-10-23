namespace ResourceLock.Core.Models;

public sealed record LockResult(
    bool Success,
    string Resource,
    string? SessionId,
    int? OwnerPid,
    string? Message,
    DateTimeOffset? ExpiresAt,
    LockRecord? CurrentHolder = null);
