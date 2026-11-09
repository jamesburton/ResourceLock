namespace ResourceLock.Coordinator;

using System;
using System.Threading;

/// <summary>Tracks coordinator start time and connected-client count for status reporting.</summary>
public sealed class CoordinatorStats
{
    private int _connectedClients;

    public DateTimeOffset StartedAt { get; } = DateTimeOffset.UtcNow;
    public int ConnectedClients => Volatile.Read(ref _connectedClients);

    public void ClientConnected() => Interlocked.Increment(ref _connectedClients);
    public void ClientDisconnected() => Interlocked.Decrement(ref _connectedClients);
}
