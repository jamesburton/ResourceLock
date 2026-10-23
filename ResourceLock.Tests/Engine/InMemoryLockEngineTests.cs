namespace ResourceLock.Tests.Engine;

using System.Diagnostics;
using ResourceLock.Core.Engine;
using ResourceLock.Core.Models;
using Xunit;

public class InMemoryLockEngineTests
{
    private static readonly int LivePid = Environment.ProcessId;

    [Fact]
    public async Task Acquire_NonContended_Succeeds()
    {
        var engine = new InMemoryLockEngine();
        var req = new LockRequest("GPU", "sess-1", LivePid);
        var result = await engine.AcquireAsync(req);

        Assert.True(result.Success);
        Assert.Equal("gpu", result.Resource); // Normalized to lowercase
    }

    [Fact]
    public async Task Acquire_Contended_Immediate_FailsWithCurrentHolder()
    {
        var engine = new InMemoryLockEngine();
        await engine.AcquireAsync(new LockRequest("GPU", "sess-1", LivePid, Reason: "Busy"));
        var result = await engine.AcquireAsync(new LockRequest("gpu", "sess-2", LivePid, WaitSeconds: 0));

        Assert.False(result.Success);
        Assert.NotNull(result.CurrentHolder);
        Assert.Equal("sess-1", result.CurrentHolder.SessionId);
    }

    [Fact]
    public async Task Release_Unblocks_QueuedWaiter()
    {
        var engine = new InMemoryLockEngine();
        await engine.AcquireAsync(new LockRequest("CPU", "sess-1", LivePid));

        var waiterTask = engine.AcquireAsync(new LockRequest("CPU", "sess-2", LivePid, WaitSeconds: 5));
        Assert.False(waiterTask.IsCompleted);

        await engine.ReleaseAsync("CPU", "sess-1");
        var waiterResult = await waiterTask;

        Assert.True(waiterResult.Success);
        Assert.Equal("sess-2", waiterResult.SessionId);
    }

    [Fact]
    public async Task Waiters_AreGrantedInFifoOrder()
    {
        var engine = new InMemoryLockEngine();
        await engine.AcquireAsync(new LockRequest("gpu", "sess-holder", LivePid));

        var waiter1Task = engine.AcquireAsync(new LockRequest("gpu", "sess-waiter1", LivePid, WaitSeconds: 5));
        var waiter2Task = engine.AcquireAsync(new LockRequest("gpu", "sess-waiter2", LivePid, WaitSeconds: 5));

        Assert.False(waiter1Task.IsCompleted);
        Assert.False(waiter2Task.IsCompleted);

        // Release first holder -> waiter 1 gets it
        await engine.ReleaseAsync("gpu", "sess-holder");
        var res1 = await waiter1Task;
        Assert.True(res1.Success);
        Assert.Equal("sess-waiter1", res1.SessionId);
        Assert.False(waiter2Task.IsCompleted);

        // Release waiter 1 -> waiter 2 gets it
        await engine.ReleaseAsync("gpu", "sess-waiter1");
        var res2 = await waiter2Task;
        Assert.True(res2.Success);
        Assert.Equal("sess-waiter2", res2.SessionId);
    }

    [Fact]
    public async Task Waiter_TimesOut_WhenLockNotFreed()
    {
        var engine = new InMemoryLockEngine();
        await engine.AcquireAsync(new LockRequest("gpu", "sess-holder", LivePid));

        var waiterResult = await engine.AcquireAsync(new LockRequest("gpu", "sess-waiter", LivePid, WaitSeconds: 1));

        Assert.False(waiterResult.Success);
        Assert.NotNull(waiterResult.CurrentHolder);
        Assert.Equal("sess-holder", waiterResult.CurrentHolder.SessionId);
    }

    [Fact]
    public async Task LockHoldTimeout_AutoReleasesAndGrantsToNextWaiter()
    {
        var engine = new InMemoryLockEngine();
        // Holder has 1-second timeout
        await engine.AcquireAsync(new LockRequest("gpu", "sess-holder", LivePid, TimeoutSeconds: 1));

        // Waiter waits up to 5 seconds
        var waiterTask = engine.AcquireAsync(new LockRequest("gpu", "sess-waiter", LivePid, WaitSeconds: 5));

        var waiterResult = await waiterTask;
        Assert.True(waiterResult.Success);
        Assert.Equal("sess-waiter", waiterResult.SessionId);
    }

    [Fact]
    public async Task ReleaseAllForSession_ReleasesMultipleLocksAndUnblocksWaiters()
    {
        var engine = new InMemoryLockEngine();
        await engine.AcquireAsync(new LockRequest("cpu", "sess-1", LivePid));
        await engine.AcquireAsync(new LockRequest("gpu", "sess-1", LivePid));

        var waiterCpuTask = engine.AcquireAsync(new LockRequest("cpu", "sess-2", LivePid, WaitSeconds: 5));

        var releasedCount = await engine.ReleaseAllForSessionAsync("sess-1");
        Assert.Equal(2, releasedCount);

        var waiterResult = await waiterCpuTask;
        Assert.True(waiterResult.Success);
        Assert.Equal("sess-2", waiterResult.SessionId);

        var list = await engine.ListLocksAsync();
        Assert.Single(list);
        Assert.Equal("sess-2", list[0].SessionId);
    }

    [Fact]
    public async Task EvictDeadPid_Unblocks_QueuedWaiter()
    {
        var isDead = false;
        var engine = new InMemoryLockEngine(isPidDead: pid => pid == 999999 ? isDead : false);
        await engine.AcquireAsync(new LockRequest("npu", "sess-dead", 999999));

        var waiterTask = engine.AcquireAsync(new LockRequest("npu", "sess-live", LivePid, WaitSeconds: 5));
        Assert.False(waiterTask.IsCompleted);

        isDead = true;
        var evicted = await engine.EvictDeadPidAsync(999999);
        Assert.True(evicted);

        var waiterResult = await waiterTask;
        Assert.True(waiterResult.Success);
        Assert.Equal("sess-live", waiterResult.SessionId);
    }

    [Fact]
    public async Task Acquire_WhenHeldByDeadPid_EvictsStaleLockImmediately()
    {
        var engine = new InMemoryLockEngine();
        await engine.AcquireAsync(new LockRequest("npu", "sess-dead", 999999));

        // When sess-live attempts to acquire, it detects sess-dead is already dead and acquires immediately without waiting
        var result = await engine.AcquireAsync(new LockRequest("npu", "sess-live", LivePid, WaitSeconds: 0));
        Assert.True(result.Success);
        Assert.Equal("sess-live", result.SessionId);
    }

    [Fact]
    public async Task EvictDeadPid_DirectCall_EvictsStaleLock()
    {
        var engine = new InMemoryLockEngine();
        await engine.AcquireAsync(new LockRequest("strix:cpu", "sess-dead", 999999));

        var evicted = await engine.EvictDeadPidAsync(999999);
        Assert.True(evicted);

        var list = await engine.ListLocksAsync();
        Assert.Empty(list);
    }

    [Fact]
    public async Task EvictDeadPid_AliveProcess_IsNotEvicted()
    {
        var engine = new InMemoryLockEngine();
        int currentPid = Process.GetCurrentProcess().Id;
        await engine.AcquireAsync(new LockRequest("alive-res", "sess-live", currentPid));

        var evicted = await engine.EvictDeadPidAsync(currentPid);
        Assert.False(evicted);

        var list = await engine.ListLocksAsync();
        Assert.Single(list);
        Assert.Equal("alive-res", list[0].Resource);
    }

    [Fact]
    public async Task Release_NonExistentOrUnowned_ReturnsFailure()
    {
        var engine = new InMemoryLockEngine();
        var res1 = await engine.ReleaseAsync("ghost", "sess-1");
        Assert.False(res1.Success);

        await engine.AcquireAsync(new LockRequest("cpu", "sess-1", LivePid));
        var res2 = await engine.ReleaseAsync("cpu", "sess-2");
        Assert.False(res2.Success);
    }

    [Fact]
    public async Task ResourceNormalization_TrimsAndLowercases()
    {
        var engine = new InMemoryLockEngine();
        var result = await engine.AcquireAsync(new LockRequest("  Strix:GPU  ", "sess-1", LivePid));
        Assert.True(result.Success);
        Assert.Equal("strix:gpu", result.Resource);

        var releaseResult = await engine.ReleaseAsync(" STRIX:gpu ", "sess-1");
        Assert.True(releaseResult.Success);
    }

    [Fact]
    public async Task Release_WithTimeout_MultipleWaiters_PromotesOnlyFirstWaiter()
    {
        var engine = new InMemoryLockEngine();
        await engine.AcquireAsync(new LockRequest("gpu", "sess-holder", LivePid, TimeoutSeconds: 60));

        var waiter1Task = engine.AcquireAsync(new LockRequest("gpu", "sess-waiter1", LivePid, WaitSeconds: 5));
        var waiter2Task = engine.AcquireAsync(new LockRequest("gpu", "sess-waiter2", LivePid, WaitSeconds: 5));

        var releaseResult = await engine.ReleaseAsync("gpu", "sess-holder");
        Assert.True(releaseResult.Success);

        var res1 = await waiter1Task;
        Assert.True(res1.Success);
        Assert.Equal("sess-waiter1", res1.SessionId);

        Assert.False(waiter2Task.IsCompleted);

        var locks = await engine.ListLocksAsync();
        Assert.Single(locks);
        Assert.Equal("sess-waiter1", locks[0].SessionId);
    }

    [Fact]
    public async Task Renewal_OfExpiringLock_DoesNotTriggerPrematurePromotion()
    {
        var engine = new InMemoryLockEngine();
        await engine.AcquireAsync(new LockRequest("gpu", "sess-holder", LivePid, TimeoutSeconds: 60));

        var waiterTask = engine.AcquireAsync(new LockRequest("gpu", "sess-waiter", LivePid, WaitSeconds: 5));
        Assert.False(waiterTask.IsCompleted);

        var renewResult = await engine.AcquireAsync(new LockRequest("gpu", "sess-holder", LivePid, TimeoutSeconds: 120));
        Assert.True(renewResult.Success);

        Assert.False(waiterTask.IsCompleted);

        var locks = await engine.ListLocksAsync();
        Assert.Single(locks);
        Assert.Equal("sess-holder", locks[0].SessionId);
    }

    [Fact]
    public async Task Renewal_SupersedesExpirationTimer_DoesNotEvictOnOldTimer()
    {
        var timeProvider = new ManualTimeProvider();
        var engine = new InMemoryLockEngine(timeProvider);

        // 1. Acquire with 5 second timeout
        await engine.AcquireAsync(new LockRequest("gpu", "sess-holder", LivePid, TimeoutSeconds: 5));

        // 2. Waiter queues up
        var waiterTask = engine.AcquireAsync(new LockRequest("gpu", "sess-waiter", LivePid, WaitSeconds: 30));
        Assert.False(waiterTask.IsCompleted);

        // 3. Advance time 2 seconds
        timeProvider.Advance(TimeSpan.FromSeconds(2));

        // 4. Renew lock with 60 second timeout (should supersede the 5-second timer)
        var renewResult = await engine.AcquireAsync(new LockRequest("gpu", "sess-holder", LivePid, TimeoutSeconds: 60));
        Assert.True(renewResult.Success);

        // 5. Advance time by 4 more seconds (total 6 seconds: the original 5s timer has expired)
        timeProvider.Advance(TimeSpan.FromSeconds(4));

        // 6. Active lock must still be sess-holder and waiter must NOT be prematurely unblocked
        Assert.False(waiterTask.IsCompleted);
        var locks = await engine.ListLocksAsync();
        Assert.Single(locks);
        Assert.Equal("sess-holder", locks[0].SessionId);
    }

    [Fact]
    public async Task LazyExpiration_InAcquireAsync_PromotesHeadWaiter()
    {
        var timeProvider = new ManualTimeProvider();
        var engine = new InMemoryLockEngine(timeProvider);

        await engine.AcquireAsync(new LockRequest("gpu", "sess-holder", LivePid, TimeoutSeconds: 5));

        var waiterTask = engine.AcquireAsync(new LockRequest("gpu", "sess-waiter", LivePid, WaitSeconds: 30));
        Assert.False(waiterTask.IsCompleted);

        // Advance time past the 5 second timeout
        timeProvider.Advance(TimeSpan.FromSeconds(10));

        // Third session attempts immediate acquisition
        var session3Result = await engine.AcquireAsync(new LockRequest("gpu", "sess-3", LivePid, WaitSeconds: 0));

        // Head waiter should have been promoted
        var waiterResult = await waiterTask;
        Assert.True(waiterResult.Success);
        Assert.Equal("sess-waiter", waiterResult.SessionId);

        // Session 3 should see contention with the promoted waiter
        Assert.False(session3Result.Success);
        Assert.NotNull(session3Result.CurrentHolder);
        Assert.Equal("sess-waiter", session3Result.CurrentHolder.SessionId);
    }

    [Fact]
    public async Task CallerCancellation_RemovesWaiterAndCleansUp()
    {
        var engine = new InMemoryLockEngine();
        await engine.AcquireAsync(new LockRequest("gpu", "sess-holder", LivePid));

        using var cts = new CancellationTokenSource();
        var waiterTask = engine.AcquireAsync(new LockRequest("gpu", "sess-waiter", LivePid, WaitSeconds: 30), cts.Token);

        Assert.False(waiterTask.IsCompleted);

        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await waiterTask);

        var releaseResult = await engine.ReleaseAsync("gpu", "sess-holder");
        Assert.True(releaseResult.Success);

        var locks = await engine.ListLocksAsync();
        Assert.Empty(locks);
    }

    [Fact]
    public async Task Dispose_CleansUpActiveLocksAndWaiters()
    {
        var engine = new InMemoryLockEngine();
        await engine.AcquireAsync(new LockRequest("gpu", "sess-holder", LivePid, TimeoutSeconds: 60));
        var waiterTask = engine.AcquireAsync(new LockRequest("gpu", "sess-waiter", LivePid, WaitSeconds: 30));

        engine.Dispose();

        var waiterResult = await waiterTask;
        Assert.False(waiterResult.Success);
        Assert.Contains("disposed", waiterResult.Message, StringComparison.OrdinalIgnoreCase);

        await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
            await engine.AcquireAsync(new LockRequest("gpu", "sess-new", LivePid)));
    }

    private sealed class ManualTimeProvider : TimeProvider
    {
        private DateTimeOffset _utcNow = new(2026, 10, 23, 12, 0, 0, TimeSpan.Zero);

        public override DateTimeOffset GetUtcNow() => _utcNow;

        public void Advance(TimeSpan delta) => _utcNow = _utcNow.Add(delta);
    }
}
