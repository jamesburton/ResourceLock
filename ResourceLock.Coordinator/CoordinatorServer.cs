namespace ResourceLock.Coordinator;

using System;
using System.Collections.Concurrent;
using System.IO;
using System.IO.Pipes;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using ResourceLock.Coordinator.Transports;
using ResourceLock.Core.Engine;
using ResourceLock.Core.Ipc;

public sealed class CoordinatorServer : IAsyncDisposable
{
    private readonly ILockEngine _lockEngine;
    private readonly int _httpPort;
    private readonly string _pipeOrSocketName;
    private readonly ConcurrentDictionary<Task, bool> _activeConnections = new();

    private CancellationTokenSource? _cts;
    private Task? _ipcLoopTask;
    private WebApplication? _app;
    private int _started;
    private int _stopped;

    public CoordinatorServer(ILockEngine lockEngine, int httpPort = 51234, string? customPipeOrSocketName = null)
    {
        _lockEngine = lockEngine ?? throw new ArgumentNullException(nameof(lockEngine));
        _httpPort = httpPort;

        if (!string.IsNullOrWhiteSpace(customPipeOrSocketName))
        {
            _pipeOrSocketName = customPipeOrSocketName;
        }
        else if (OperatingSystem.IsWindows())
        {
            _pipeOrSocketName = IpcConstants.GetNamedPipeName();
        }
        else
        {
            _pipeOrSocketName = IpcConstants.GetUnixSocketPath();
        }
    }

    public string PipeOrSocketName => _pipeOrSocketName;
    public int? HttpPort { get; private set; }
    public Uri? HttpUri { get; private set; }

    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        if (Interlocked.CompareExchange(ref _started, 1, 0) != 0)
        {
            return;
        }

        _cts = new CancellationTokenSource();

        // 1. Start IPC Listener
        if (OperatingSystem.IsWindows())
        {
            _ipcLoopTask = Task.Run(() => RunNamedPipeLoopAsync(_pipeOrSocketName, _cts.Token));
        }
        else
        {
            _ipcLoopTask = Task.Run(() => RunUnixSocketLoopAsync(_pipeOrSocketName, _cts.Token));
        }

        // 2. Start HTTP listener if enabled
        if (_httpPort >= 0)
        {
            var builder = WebApplication.CreateSlimBuilder();
            builder.Logging.ClearProviders();

            builder.WebHost.UseUrls($"http://127.0.0.1:{_httpPort}");

            _app = builder.Build();
            _app.MapCoordinatorEndpoints(_lockEngine);

            await _app.StartAsync(cancellationToken).ConfigureAwait(false);

            var serverAddresses = _app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>();
            var address = serverAddresses?.Addresses.FirstOrDefault();
            if (address != null && Uri.TryCreate(address, UriKind.Absolute, out var uri))
            {
                HttpPort = uri.Port;
                HttpUri = uri;
            }
            else if (_httpPort > 0)
            {
                HttpPort = _httpPort;
                HttpUri = new Uri($"http://127.0.0.1:{_httpPort}");
            }
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        if (Interlocked.CompareExchange(ref _stopped, 1, 0) != 0)
        {
            return;
        }

        try
        {
            _cts?.Cancel();
        }
        catch (ObjectDisposedException)
        {
        }

        if (_ipcLoopTask != null)
        {
            try
            {
                await _ipcLoopTask.ConfigureAwait(false);
            }
            catch (Exception)
            {
                // Ignore cancellation
            }
        }

        // Wait for active client handling tasks with timeout
        var connectionTasks = _activeConnections.Keys.ToArray();
        if (connectionTasks.Length > 0)
        {
            try
            {
                var timeoutTask = Task.Delay(2000, cancellationToken);
                var allTasks = Task.WhenAll(connectionTasks);
                await Task.WhenAny(allTasks, timeoutTask).ConfigureAwait(false);
            }
            catch (Exception)
            {
            }
        }

        if (_app != null)
        {
            try
            {
                await _app.StopAsync(cancellationToken).ConfigureAwait(false);
                await _app.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception)
            {
            }
            _app = null;
        }
    }

    private async Task RunNamedPipeLoopAsync(string pipeName, CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            NamedPipeServerStream? serverStream = null;
            try
            {
                serverStream = new NamedPipeServerStream(
                    pipeName,
                    PipeDirection.InOut,
                    NamedPipeServerStream.MaxAllowedServerInstances,
                    PipeTransmissionMode.Byte,
                    PipeOptions.Asynchronous);

                await serverStream.WaitForConnectionAsync(cancellationToken).ConfigureAwait(false);

                var stream = serverStream;
                var task = Task.Run(async () =>
                {
                    try
                    {
                        await using (stream.ConfigureAwait(false))
                        {
                            var handler = new IpcConnectionHandler(_lockEngine);
                            await handler.HandleAsync(stream, cancellationToken).ConfigureAwait(false);
                        }
                    }
                    catch (Exception)
                    {
                    }
                }, cancellationToken);

                TrackConnection(task);
            }
            catch (OperationCanceledException)
            {
                serverStream?.Dispose();
                break;
            }
            catch (Exception)
            {
                serverStream?.Dispose();
                if (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
                await Task.Delay(100, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private async Task RunUnixSocketLoopAsync(string socketPath, CancellationToken cancellationToken)
    {
        var directory = Path.GetDirectoryName(socketPath);
        if (!string.IsNullOrEmpty(directory))
        {
            Directory.CreateDirectory(directory);
        }

        if (File.Exists(socketPath))
        {
            try
            {
                File.Delete(socketPath);
            }
            catch
            {
            }
        }

        using var serverSocket = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
        serverSocket.Bind(new UnixDomainSocketEndPoint(socketPath));
        serverSocket.Listen(128);

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                Socket clientSocket;
                try
                {
                    clientSocket = await serverSocket.AcceptAsync(cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    break;
                }

                var task = Task.Run(async () =>
                {
                    try
                    {
                        using (clientSocket)
                        await using (var stream = new NetworkStream(clientSocket, ownsSocket: false))
                        {
                            var handler = new IpcConnectionHandler(_lockEngine);
                            await handler.HandleAsync(stream, cancellationToken).ConfigureAwait(false);
                        }
                    }
                    catch (Exception)
                    {
                    }
                }, cancellationToken);

                TrackConnection(task);
            }
        }
        finally
        {
            if (File.Exists(socketPath))
            {
                try
                {
                    File.Delete(socketPath);
                }
                catch
                {
                }
            }
        }
    }

    private void TrackConnection(Task task)
    {
        _activeConnections.TryAdd(task, true);
        _ = task.ContinueWith(t => _activeConnections.TryRemove(t, out _), TaskScheduler.Default);
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync().ConfigureAwait(false);
        _cts?.Dispose();
    }
}
