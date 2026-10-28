namespace ResourceLock;

using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ResourceLock.Coordinator;
using ResourceLock.Core.Engine;
using ResourceLock.Hooks;
using ResourceLock.Ipc;
using ResourceLock.Lifecycle;
using ResourceLock.Mcp;
using ResourceLock.Tools;

public class Program
{
    public static async Task<int> Main(string[] args)
    {
        if (args.Length > 0 && args.Any(a => a.Equals("--version", StringComparison.OrdinalIgnoreCase) || a.Equals("-v", StringComparison.OrdinalIgnoreCase)))
        {
            var version = typeof(Program).Assembly.GetName().Version?.ToString() ?? "0.1.0";
            Console.WriteLine($"resourcelock version {version}");
            return 0;
        }

        if (args.Length > 0 && args.Any(a => a.Equals("--help", StringComparison.OrdinalIgnoreCase) || a.Equals("-h", StringComparison.OrdinalIgnoreCase) || a.Equals("/?", StringComparison.OrdinalIgnoreCase)))
        {
            PrintUsage();
            return 0;
        }

        if (args.Length > 0 && args.Any(a => a.Equals("coordinator", StringComparison.OrdinalIgnoreCase)))
        {
            return await RunCoordinatorAsync(args).ConfigureAwait(false);
        }

        return await RunMcpServerAsync(args).ConfigureAwait(false);
    }

    public static void PrintUsage()
    {
        Console.WriteLine(@"ResourceLock - Cross-process and cross-machine resource locking for agents and CLI

Usage:
  resourcelock [options]
  resourcelock coordinator [options]

Commands:
  coordinator        Run in standalone coordinator daemon mode

Options:
  -h, --help         Show help and usage information
  -v, --version      Show version information
");
    }

    public static async Task<int> RunCoordinatorAsync(string[] args, CancellationToken cancellationToken = default)
    {
        var engine = new InMemoryLockEngine();
        await using var coordinator = new CoordinatorServer(engine);

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        Console.CancelKeyPress += (s, e) =>
        {
            e.Cancel = true;
            cts.Cancel();
        };
        AppDomain.CurrentDomain.ProcessExit += (s, e) => cts.Cancel();

        await coordinator.StartAsync(cts.Token).ConfigureAwait(false);

        try
        {
            await Task.Delay(Timeout.Infinite, cts.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
        }

        return 0;
    }

    public static async Task<int> RunMcpServerAsync(
        string[]? args = null,
        Stream? customIpcStream = null,
        Stream? stdin = null,
        Stream? stdout = null,
        CancellationToken cancellationToken = default)
    {
        var sessionId = Guid.NewGuid().ToString("N");
        var pid = Environment.ProcessId;

        Stream stream;
        if (customIpcStream != null)
        {
            stream = customIpcStream;
        }
        else
        {
            try
            {
                stream = await DaemonSpawner.EnsureCoordinatorAndConnectAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                await Console.Error.WriteLineAsync($"[ResourceLock] Failed to connect to coordinator: {ex.Message}").ConfigureAwait(false);
                return 1;
            }
        }

        await using var ipcEngine = new IpcLockEngine(stream);
        await ipcEngine.RegisterSessionAsync(sessionId, pid, cancellationToken).ConfigureAwait(false);

        var tools = new ResourceLockTools(ipcEngine, sessionId, pid);

        ExitCleanupHook.Register(async () =>
        {
            try
            {
                await tools.ReleaseAllSessionAndSubsessionLocksAsync().ConfigureAwait(false);
            }
            catch
            {
            }
        });

        var host = new McpServerHost(tools);
        var inStream = stdin ?? Console.OpenStandardInput();
        var outStream = stdout ?? Console.OpenStandardOutput();

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        Console.CancelKeyPress += (s, e) =>
        {
            e.Cancel = true;
            cts.Cancel();
        };

        try
        {
            await host.RunAsync(inStream, outStream, cts.Token).ConfigureAwait(false);
        }
        finally
        {
            ExitCleanupHook.ExecuteCleanup();
            if (stdin == null) inStream.Dispose();
            if (stdout == null) outStream.Dispose();
        }

        return 0;
    }
}
