namespace ResourceLock.Core.Models;

public sealed record CoordinatorStatus(
    int Pid,
    DateTimeOffset StartedAt,
    int ConnectedClients,
    int HeldLocks,
    string Version);
