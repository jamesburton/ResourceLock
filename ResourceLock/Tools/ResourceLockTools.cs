namespace ResourceLock.Tools;

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.AI;
using ResourceLock.Core.Engine;
using ResourceLock.Core.Models;

/// <summary>
/// Result returned when releasing all locks held by a session.
/// </summary>
public sealed record ReleaseAllResult(
    [property: JsonPropertyName("success")] bool Success,
    [property: JsonPropertyName("released_count")] int ReleasedCount,
    [property: JsonPropertyName("resources")] IReadOnlyList<string>? Resources = null);

/// <summary>
/// Exposes high-level locking operations as tools compatible with Microsoft.Extensions.AI and MCP.
/// </summary>
public class ResourceLockTools
{
    private readonly ILockEngine _engine;
    private readonly string _sessionId;
    private readonly int _ownerPid;

    public ResourceLockTools(ILockEngine engine, string? sessionId = null, int? ownerPid = null)
    {
        _engine = engine ?? throw new ArgumentNullException(nameof(engine));
        _sessionId = sessionId ?? Guid.NewGuid().ToString("N");
        _ownerPid = ownerPid ?? Environment.ProcessId;
    }

    public string SessionId => _sessionId;
    public int OwnerPid => _ownerPid;
    public ILockEngine Engine => _engine;

    /// <summary>
    /// Acquires an exclusive lock on a specified resource (e.g., 'CPU', 'GPU', 'NPU', 'Strix:CPU').
    /// </summary>
    /// <param name="resource">Identifier of the resource.</param>
    /// <param name="wait_seconds">Maximum seconds to block waiting if the resource is currently locked. Set to 0 for non-blocking.</param>
    /// <param name="timeout_seconds">Max duration in seconds to hold the lock before automatic release.</param>
    /// <param name="reason">Short explanation of work being performed.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    [Description("Acquires an exclusive lock on a specified resource (e.g., 'CPU', 'GPU', 'NPU', 'Strix:CPU').")]
    public async Task<LockResult> AcquireLockAsync(
        [Description("Identifier of the resource (e.g. 'CPU', 'GPU', 'NPU').")] string resource,
        [Description("Maximum seconds to block waiting if the resource is currently locked. Set to 0 for non-blocking.")] int wait_seconds = 0,
        [Description("Max duration in seconds to hold the lock before automatic release.")] int? timeout_seconds = null,
        [Description("Short explanation of work being performed.")] string? reason = null,
        CancellationToken cancellationToken = default)
    {
        var request = new LockRequest(resource, _sessionId, _ownerPid, wait_seconds, timeout_seconds, reason);
        return await _engine.AcquireAsync(request, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Releases a previously acquired lock on a resource held by the current session.
    /// </summary>
    /// <param name="resource">Identifier of the resource.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    [Description("Releases a previously acquired lock on a resource held by the current session.")]
    public async Task<LockResult> ReleaseLockAsync(
        [Description("Identifier of the resource to release.")] string resource,
        CancellationToken cancellationToken = default)
    {
        return await _engine.ReleaseAsync(resource, _sessionId, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Lists all currently held locks across the system with owner session, PID, acquisition time, and reason.
    /// </summary>
    [Description("Lists all currently held locks across the system with owner session, PID, acquisition time, and reason.")]
    public async Task<IReadOnlyList<LockRecord>> ListLocksAsync(CancellationToken cancellationToken = default)
    {
        return await _engine.ListLocksAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Releases all locks currently held by this session.
    /// </summary>
    [Description("Releases all locks currently held by this session.")]
    public async Task<ReleaseAllResult> ReleaseAllLocksAsync(CancellationToken cancellationToken = default)
    {
        IReadOnlyList<string>? myResources = null;
        try
        {
            var active = await _engine.ListLocksAsync(cancellationToken).ConfigureAwait(false);
            myResources = active.Where(l => l.SessionId == _sessionId).Select(l => l.Resource).ToList();
        }
        catch
        {
            // Proceed even if listing fails
        }

        var count = await _engine.ReleaseAllForSessionAsync(_sessionId, cancellationToken).ConfigureAwait(false);
        return new ReleaseAllResult(true, count, myResources);
    }

    // Snake_case aliases matching MCP tool names for direct AIFunctionFactory invocation
    [Description("Acquires an exclusive lock on a specified resource (e.g., 'CPU', 'GPU', 'NPU', 'Strix:CPU').")]
    public Task<LockResult> acquire_lock(string resource, int wait_seconds = 0, int? timeout_seconds = null, string? reason = null)
        => AcquireLockAsync(resource, wait_seconds, timeout_seconds, reason);

    [Description("Releases a previously acquired lock on a resource held by the current session.")]
    public Task<LockResult> release_lock(string resource)
        => ReleaseLockAsync(resource);

    [Description("Lists all currently held locks across the system with owner session, PID, acquisition time, and reason.")]
    public Task<IReadOnlyList<LockRecord>> list_locks()
        => ListLocksAsync();

    [Description("Releases all locks currently held by this session.")]
    public Task<ReleaseAllResult> release_all_locks()
        => ReleaseAllLocksAsync();

    /// <summary>
    /// Returns the collection of tools as Microsoft.Extensions.AI <see cref="AIFunction"/> instances.
    /// </summary>
    public IEnumerable<AIFunction> AsAIFunctions()
    {
        yield return AIFunctionFactory.Create(acquire_lock, name: "acquire_lock", description: "Acquires an exclusive lock on a specified resource (e.g., 'CPU', 'GPU', 'NPU', 'Strix:CPU').");
        yield return AIFunctionFactory.Create(release_lock, name: "release_lock", description: "Releases a previously acquired lock on a resource held by the current session.");
        yield return AIFunctionFactory.Create(list_locks, name: "list_locks", description: "Lists all currently held locks across the system with their owner session, PID, acquisition time, and reason.");
        yield return AIFunctionFactory.Create(release_all_locks, name: "release_all_locks", description: "Releases all locks currently held by this session. Recommended before finishing work.");
    }
}
