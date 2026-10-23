namespace ResourceLock.Tests.Engine;

using System.Diagnostics;
using ResourceLock.Core.Engine;
using ResourceLock.Core.Models;
using Xunit;

public class InMemoryLockEngineTests
{
    [Fact]
    public async Task Acquire_NonContended_Succeeds()
    {
        var engine = new InMemoryLockEngine();
        var req = new LockRequest("GPU", "sess-1", 100);
        var result = await engine.AcquireAsync(req);

        Assert.True(result.Success);
        Assert.Equal("gpu", result.Resource); // Normalized to lowercase
    }

    [Fact]
    public async Task Acquire_Contended_Immediate_FailsWithCurrentHolder()
    {
        var engine = new InMemoryLockEngine();
        await engine.AcquireAsync(new LockRequest("GPU", "sess-1", 100, Reason: "Busy"));
        var result = await engine.AcquireAsync(new LockRequest("gpu", "sess-2", 200, WaitSeconds: 0));

        Assert.False(result.Success);
        Assert.NotNull(result.CurrentHolder);
        Assert.Equal("sess-1", result.CurrentHolder.SessionId);
    }

    [Fact]
    public async Task Release_Unblocks_QueuedWaiter()
    {
        var engine = new InMemoryLockEngine();
        await engine.AcquireAsync(new LockRequest("CPU", "sess-1", 100));

        var waiterTask = engine.AcquireAsync(new LockRequest("CPU", "sess-2", 200, WaitSeconds: 5));
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
        await engine.AcquireAsync(new LockRequest("gpu", "sess-holder", 100));

        var waiter1Task = engine.AcquireAsync(new LockRequest("gpu", "sess-waiter1", 101, WaitSeconds: 5));
        var waiter2Task = engine.AcquireAsync(new LockRequest("gpu", "sess-waiter2", 102, WaitSeconds: 5));

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
        await engine.AcquireAsync(new LockRequest("gpu", "sess-holder", 100));

        var waiterResult = await engine.AcquireAsync(new LockRequest("gpu", "sess-waiter", 200, WaitSeconds: 1));

        Assert.False(waiterResult.Success);
        Assert.NotNull(waiterResult.CurrentHolder);
        Assert.Equal("sess-holder", waiterResult.CurrentHolder.SessionId);
    }

    [Fact]
    public async Task LockHoldTimeout_AutoReleasesAndGrantsToNextWaiter()
    {
        var engine = new InMemoryLockEngine();
        // Holder has 1-second timeout
        await engine.AcquireAsync(new LockRequest("gpu", "sess-holder", 100, TimeoutSeconds: 1));

        // Waiter waits up to 5 seconds
        var waiterTask = engine.AcquireAsync(new LockRequest("gpu", "sess-waiter", 200, WaitSeconds: 5));

        var waiterResult = await waiterTask;
        Assert.True(waiterResult.Success);
        Assert.Equal("sess-waiter", waiterResult.SessionId);
    }

    [Fact]
    public async Task ReleaseAllForSession_ReleasesMultipleLocksAndUnblocksWaiters()
    {
        var engine = new InMemoryLockEngine();
        await engine.AcquireAsync(new LockRequest("cpu", "sess-1", 100));
        await engine.AcquireAsync(new LockRequest("gpu", "sess-1", 100));

        var waiterCpuTask = engine.AcquireAsync(new LockRequest("cpu", "sess-2", 200, WaitSeconds: 5));

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
        var engine = new InMemoryLockEngine();
        await engine.AcquireAsync(new LockRequest("npu", "sess-dead", 999999));

        var waiterTask = engine.AcquireAsync(new LockRequest("npu", "sess-live", 1234, WaitSeconds: 5));
        Assert.False(waiterTask.IsCompleted);

        var evicted = await engine.EvictDeadPidAsync(999999);
        Assert.True(evicted);

        var waiterResult = await waiterTask;
        Assert.True(waiterResult.Success);
        Assert.Equal("sess-live", waiterResult.SessionId);
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

        await engine.AcquireAsync(new LockRequest("cpu", "sess-1", 100));
        var res2 = await engine.ReleaseAsync("cpu", "sess-2");
        Assert.False(res2.Success);
    }

    [Fact]
    public async Task ResourceNormalization_TrimsAndLowercases()
    {
        var engine = new InMemoryLockEngine();
        var result = await engine.AcquireAsync(new LockRequest("  Strix:GPU  ", "sess-1", 100));
        Assert.True(result.Success);
        Assert.Equal("strix:gpu", result.Resource);

        var releaseResult = await engine.ReleaseAsync(" STRIX:gpu ", "sess-1");
        Assert.True(releaseResult.Success);
    }
}
