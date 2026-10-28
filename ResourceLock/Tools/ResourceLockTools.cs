namespace ResourceLock.Tools;

using System;
using System.Collections.Concurrent;
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
    private readonly ConcurrentDictionary<string, byte> _knownSessionIds = new(StringComparer.Ordinal);

    public ResourceLockTools(ILockEngine engine, string? sessionId = null, int? ownerPid = null)
    {
        _engine = engine ?? throw new ArgumentNullException(nameof(engine));
        _sessionId = sessionId ?? Guid.NewGuid().ToString("N");
        _ownerPid = ownerPid ?? Environment.ProcessId;
        _knownSessionIds.TryAdd(_sessionId, 0);
    }

    public string SessionId => _sessionId;
    public int OwnerPid => _ownerPid;
    public ILockEngine Engine => _engine;

    private string GetEffectiveSessionId(string? owner)
    {
        if (string.IsNullOrWhiteSpace(owner))
        {
            return _sessionId;
        }

        var effective = $"{_sessionId}:{owner.Trim()}";
        _knownSessionIds.TryAdd(effective, 0);
        return effective;
    }

    /// <summary>
    /// Acquires an exclusive lock on a specified resource (e.g., 'CPU', 'GPU', 'NPU', 'Strix:CPU').
    /// </summary>
    /// <param name="resource">Identifier of the resource.</param>
    /// <param name="wait_seconds">Maximum seconds to block waiting if the resource is currently locked. Set to 0 for non-blocking.</param>
    /// <param name="timeout_seconds">Max duration in seconds to hold the lock before automatic release.</param>
    /// <param name="reason">Short explanation of work being performed.</param>
    /// <param name="owner">Optional unique caller/subagent identifier when running parallel subagents in the same session.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    [Description("Acquires an exclusive lock on a specified resource (e.g., 'CPU', 'GPU', 'NPU', 'Strix:CPU').")]
    public async Task<LockResult> AcquireLockAsync(
        [Description("Identifier of the resource (e.g. 'CPU', 'GPU', 'NPU').")] string resource,
        [Description("Maximum seconds to block waiting if the resource is currently locked. Set to 0 for non-blocking.")] int wait_seconds = 0,
        [Description("Max duration in seconds to hold the lock before automatic release.")] int? timeout_seconds = null,
        [Description("Short explanation of work being performed.")] string? reason = null,
        [Description("Optional unique caller/subagent identifier (e.g. 'lane-1') when running parallel subagents in the same session.")] string? owner = null,
        CancellationToken cancellationToken = default)
    {
        var effectiveSessionId = GetEffectiveSessionId(owner);
        var request = new LockRequest(resource, effectiveSessionId, _ownerPid, wait_seconds, timeout_seconds, reason);
        return await _engine.AcquireAsync(request, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Releases a previously acquired lock on a resource held by the current session.
    /// </summary>
    /// <param name="resource">Identifier of the resource.</param>
    /// <param name="owner">Optional unique caller/subagent identifier if one was used when acquiring the lock.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    [Description("Releases a previously acquired lock on a resource held by the current session.")]
    public async Task<LockResult> ReleaseLockAsync(
        [Description("Identifier of the resource to release.")] string resource,
        [Description("Optional unique caller/subagent identifier if one was used when acquiring the lock.")] string? owner = null,
        CancellationToken cancellationToken = default)
    {
        var effectiveSessionId = GetEffectiveSessionId(owner);
        var result = await _engine.ReleaseAsync(resource, effectiveSessionId, cancellationToken).ConfigureAwait(false);

        if (!result.Success &&
            string.IsNullOrWhiteSpace(owner) &&
            result.CurrentHolder != null &&
            result.CurrentHolder.SessionId.StartsWith(_sessionId + ":", StringComparison.Ordinal))
        {
            var subOwner = result.CurrentHolder.SessionId[(_sessionId.Length + 1)..];
            return result with
            {
                Message = $"Lock on '{result.Resource}' is held by owner '{subOwner}' in this session. Pass owner: '{subOwner}' to release_lock."
            };
        }

        return result;
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
    /// Releases all locks currently held by this session (or subagent owner).
    /// </summary>
    [Description("Releases all locks currently held by this session.")]
    public async Task<ReleaseAllResult> ReleaseAllLocksAsync(
        [Description("Optional unique caller/subagent identifier if releasing locks for a specific subagent.")] string? owner = null,
        CancellationToken cancellationToken = default)
    {
        var effectiveSessionId = GetEffectiveSessionId(owner);
        IReadOnlyList<string>? myResources = null;
        try
        {
            var active = await _engine.ListLocksAsync(cancellationToken).ConfigureAwait(false);
            myResources = active.Where(l => l.SessionId == effectiveSessionId).Select(l => l.Resource).ToList();
        }
        catch
        {
            // Proceed even if listing fails
        }

        var count = await _engine.ReleaseAllForSessionAsync(effectiveSessionId, cancellationToken).ConfigureAwait(false);
        return new ReleaseAllResult(true, count, myResources);
    }

    /// <summary>
    /// Releases all locks held by the base session and any subagent owners created on this MCP connection.
    /// </summary>
    public async Task<int> ReleaseAllSessionAndSubsessionLocksAsync(CancellationToken cancellationToken = default)
    {
        var total = 0;
        foreach (var sessionId in _knownSessionIds.Keys)
        {
            try
            {
                total += await _engine.ReleaseAllForSessionAsync(sessionId, cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                // Ignore errors during exit cleanup
            }
        }
        return total;
    }

    // Snake_case aliases matching MCP tool names for direct AIFunctionFactory invocation
    [Description("Acquires an exclusive lock on a specified resource (e.g., 'CPU', 'GPU', 'NPU', 'Strix:CPU').")]
    public Task<LockResult> acquire_lock(string resource, int wait_seconds = 0, int? timeout_seconds = null, string? reason = null, string? owner = null)
        => AcquireLockAsync(resource, wait_seconds, timeout_seconds, reason, owner);

    [Description("Releases a previously acquired lock on a resource held by the current session.")]
    public Task<LockResult> release_lock(string resource, string? owner = null)
        => ReleaseLockAsync(resource, owner);

    [Description("Lists all currently held locks across the system with owner session, PID, acquisition time, and reason.")]
    public Task<IReadOnlyList<LockRecord>> list_locks()
        => ListLocksAsync();

    [Description("Releases all locks currently held by this session.")]
    public Task<ReleaseAllResult> release_all_locks(string? owner = null)
        => ReleaseAllLocksAsync(owner);

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
