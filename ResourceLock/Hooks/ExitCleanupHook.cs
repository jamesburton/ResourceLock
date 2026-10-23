namespace ResourceLock.Hooks;

using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

public static class ExitCleanupHook
{
    private static readonly ConcurrentBag<Func<Task>> _cleanupCallbacks = new();
    private static int _registered;
    private static int _executed;

    public static void Register(Func<Task> onExitCleanupAsync)
    {
        if (onExitCleanupAsync == null)
        {
            throw new ArgumentNullException(nameof(onExitCleanupAsync));
        }

        _cleanupCallbacks.Add(onExitCleanupAsync);
        EnsureHooksRegistered();
    }

    private static void EnsureHooksRegistered()
    {
        if (Interlocked.CompareExchange(ref _registered, 1, 0) == 0)
        {
            AppDomain.CurrentDomain.ProcessExit += OnProcessExit;
            try
            {
                Console.CancelKeyPress += OnCancelKeyPress;
            }
            catch
            {
                // In headless, non-interactive or redirected console environments, CancelKeyPress may not be supported.
            }
        }
    }

    private static void OnProcessExit(object? sender, EventArgs e)
    {
        ExecuteCleanup();
    }

    private static void OnCancelKeyPress(object? sender, ConsoleCancelEventArgs e)
    {
        ExecuteCleanup();
    }

    public static void ExecuteCleanup()
    {
        if (Interlocked.CompareExchange(ref _executed, 1, 0) != 0)
        {
            return;
        }

        var callbacks = _cleanupCallbacks.ToArray();
        if (callbacks.Length == 0)
        {
            return;
        }

        var tasks = callbacks.Select(cb =>
        {
            try
            {
                return cb() ?? Task.CompletedTask;
            }
            catch
            {
                return Task.CompletedTask;
            }
        }).ToArray();

        try
        {
            Task.WaitAll(tasks, TimeSpan.FromSeconds(3));
        }
        catch
        {
        }
    }

    internal static void ResetForTesting()
    {
        _executed = 0;
        while (_cleanupCallbacks.TryTake(out _)) { }
    }
}
