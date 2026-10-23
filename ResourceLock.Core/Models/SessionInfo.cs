namespace ResourceLock.Core.Models;

public sealed record SessionInfo(
    string SessionId,
    int OwnerPid,
    DateTimeOffset ConnectedAt);
