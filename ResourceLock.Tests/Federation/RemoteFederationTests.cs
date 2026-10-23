namespace ResourceLock.Tests.Federation;

using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Pipes;
using System.Net;
using System.Net.Http;
using System.Net.Http.Json;
using System.Net.Sockets;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using ResourceLock.Coordinator;
using ResourceLock.Coordinator.Federation;
using ResourceLock.Coordinator.Transports;
using ResourceLock.Core.Engine;
using ResourceLock.Core.Ipc;
using ResourceLock.Core.Models;
using Xunit;

public class RemoteFederationTests
{
    [Theory]
    [InlineData("Strix:CPU", "strix", "cpu")]
    [InlineData("Server1:GPU", "server1", "gpu")]
    public void ResourceParser_ExtractsHostAndResource(string input, string expectedHost, string expectedResource)
    {
        var parsed = RemoteResourceParser.Parse(input);
        Assert.Equal(expectedHost, parsed.Host);
        Assert.Equal(expectedResource, parsed.Resource);
    }

    [Theory]
    [InlineData("CPU", null, "cpu")]
    [InlineData("memory", null, "memory")]
    [InlineData("  Disk:Fast  ", "disk", "fast")]
    public void ResourceParser_WithoutHostOrWithSpaces_HandlesCorrectly(string input, string? expectedHost, string expectedResource)
    {
        var parsed = RemoteResourceParser.Parse(input);
        Assert.Equal(expectedHost, parsed.Host);
        Assert.Equal(expectedResource, parsed.Resource);
    }

    private sealed class DelegatingTestHandler : HttpMessageHandler
    {
        private readonly Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> _handler;

        public DelegatingTestHandler(Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> handler)
        {
            _handler = handler;
        }

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            return _handler(request, cancellationToken);
        }
    }

    [Fact]
    public async Task ForwardAcquireAsync_SuccessfulResponse_ReturnsSuccessResultWithOriginalResource()
    {
        HttpRequestMessage? interceptedRequest = null;
        string? interceptedBody = null;

        var handler = new DelegatingTestHandler(async (req, ct) =>
        {
            interceptedRequest = req;
            if (req.Content != null)
            {
                interceptedBody = await req.Content.ReadAsStringAsync(ct);
            }

            var result = new LockResult(true, "cpu", "sess-1", 1234, null, null);
            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = JsonContent.Create(result, options: IpcFrameCodec.DefaultJsonOptions)
            };
        });

        using var httpClient = new HttpClient(handler);
        using var client = new RemoteCoordinatorClient(httpClient, defaultPort: 51234);

        var request = new LockRequest("Strix:CPU", "sess-1", 1234);
        var result = await client.ForwardAcquireAsync("strix", request);

        Assert.NotNull(interceptedRequest);
        Assert.Equal(HttpMethod.Post, interceptedRequest.Method);
        Assert.Equal("http://strix:51234/api/locks/acquire", interceptedRequest.RequestUri?.ToString());
        Assert.NotNull(interceptedBody);

        var sentRequest = JsonSerializer.Deserialize<LockRequest>(interceptedBody, IpcFrameCodec.DefaultJsonOptions);
        Assert.NotNull(sentRequest);
        Assert.Equal("cpu", sentRequest.Resource); // remote host receives normalized resource without prefix

        Assert.True(result.Success);
        Assert.Equal("Strix:CPU", result.Resource); // original resource preserved
        Assert.Equal("sess-1", result.SessionId);
    }

    [Fact]
    public async Task ForwardAcquireAsync_UnreachableHost_ReturnsFailedLockResultGracefully()
    {
        var handler = new DelegatingTestHandler((req, ct) =>
        {
            throw new HttpRequestException("Connection refused");
        });

        using var httpClient = new HttpClient(handler);
        using var client = new RemoteCoordinatorClient(httpClient, defaultPort: 51234);

        var request = new LockRequest("Strix:CPU", "sess-1", 1234);
        var result = await client.ForwardAcquireAsync("strix", request);

        Assert.False(result.Success);
        Assert.Equal("Strix:CPU", result.Resource);
        Assert.Contains("Failed to contact remote coordinator at strix", result.Message);
    }

    [Fact]
    public async Task ForwardReleaseAsync_SuccessfulResponse_ReturnsSuccessResult()
    {
        HttpRequestMessage? interceptedRequest = null;
        string? interceptedBody = null;

        var handler = new DelegatingTestHandler(async (req, ct) =>
        {
            interceptedRequest = req;
            if (req.Content != null)
            {
                interceptedBody = await req.Content.ReadAsStringAsync(ct);
            }

            var result = new LockResult(true, "cpu", "sess-1", null, null, null);
            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = JsonContent.Create(result, options: IpcFrameCodec.DefaultJsonOptions)
            };
        });

        using var httpClient = new HttpClient(handler);
        using var client = new RemoteCoordinatorClient(httpClient, defaultPort: 51234);

        var result = await client.ForwardReleaseAsync("strix", "Strix:CPU", "sess-1");

        Assert.NotNull(interceptedRequest);
        Assert.Equal(HttpMethod.Post, interceptedRequest.Method);
        Assert.Equal("http://strix:51234/api/locks/release", interceptedRequest.RequestUri?.ToString());

        var sentPayload = JsonSerializer.Deserialize<ReleaseLockHttpRequest>(interceptedBody!, IpcFrameCodec.DefaultJsonOptions);
        Assert.NotNull(sentPayload);
        Assert.Equal("cpu", sentPayload.Resource);
        Assert.Equal("sess-1", sentPayload.SessionId);

        Assert.True(result.Success);
        Assert.Equal("Strix:CPU", result.Resource);
    }

    [Fact]
    public async Task ForwardReleaseAsync_UnreachableHost_ReturnsFailedLockResultGracefully()
    {
        var handler = new DelegatingTestHandler((req, ct) =>
        {
            throw new HttpRequestException("Host unreachable");
        });

        using var httpClient = new HttpClient(handler);
        using var client = new RemoteCoordinatorClient(httpClient, defaultPort: 51234);

        var result = await client.ForwardReleaseAsync("strix", "Strix:CPU", "sess-1");

        Assert.False(result.Success);
        Assert.Equal("Strix:CPU", result.Resource);
        Assert.Contains("Failed to contact remote coordinator at strix", result.Message);
    }

    [Fact]
    public async Task ForwardListAsync_SuccessfulResponse_ReturnsRecords()
    {
        var handler = new DelegatingTestHandler((req, ct) =>
        {
            var records = new List<LockRecord>
            {
                new("cpu", "sess-1", 1234, DateTimeOffset.UtcNow, null, null)
            };
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = JsonContent.Create(records, options: IpcFrameCodec.DefaultJsonOptions)
            });
        });

        using var httpClient = new HttpClient(handler);
        using var client = new RemoteCoordinatorClient(httpClient, defaultPort: 51234);

        var records = await client.ForwardListAsync("strix");

        Assert.Single(records);
        Assert.Equal("cpu", records[0].Resource);
    }

    [Fact]
    public async Task ForwardListAsync_UnreachableHost_ReturnsEmptyListGracefully()
    {
        var handler = new DelegatingTestHandler((req, ct) =>
        {
            throw new HttpRequestException("Timeout");
        });

        using var httpClient = new HttpClient(handler);
        using var client = new RemoteCoordinatorClient(httpClient, defaultPort: 51234);

        var records = await client.ForwardListAsync("strix");

        Assert.Empty(records);
    }

    private sealed class MockRemoteCoordinatorClient : IRemoteCoordinatorClient
    {
        public List<LockRequest> AcquireCalls { get; } = new();
        public List<(string Host, string Resource, string SessionId)> ReleaseCalls { get; } = new();

        public ValueTask<LockResult> ForwardAcquireAsync(string remoteHost, LockRequest request, CancellationToken ct = default)
        {
            AcquireCalls.Add(request);
            return ValueTask.FromResult(new LockResult(true, request.Resource, request.SessionId, request.OwnerPid, null, null));
        }

        public ValueTask<LockResult> ForwardReleaseAsync(string remoteHost, string resource, string sessionId, CancellationToken ct = default)
        {
            ReleaseCalls.Add((remoteHost, resource, sessionId));
            return ValueTask.FromResult(new LockResult(true, resource, sessionId, null, null, null));
        }

        public ValueTask<IReadOnlyList<LockRecord>> ForwardListAsync(string remoteHost, CancellationToken ct = default)
        {
            return ValueTask.FromResult<IReadOnlyList<LockRecord>>(Array.Empty<LockRecord>());
        }
    }

    private static (Stream ClientStream, Stream ServerStream) CreateConnectedStreamPair()
    {
        var (s1, s2) = BidirectionalStreamPair.Create();
        return (s1, s2);
    }

    private sealed class BidirectionalStreamPair : Stream
    {
        private readonly PipeStream _readable;
        private readonly PipeStream _writable;

        public static (Stream Client, Stream Server) Create()
        {
            var pipeClientToServer = new AnonymousPipeServerStream(PipeDirection.Out, HandleInheritability.None);
            var pipeClientToServerReader = new AnonymousPipeClientStream(PipeDirection.In, pipeClientToServer.ClientSafePipeHandle);

            var pipeServerToClient = new AnonymousPipeServerStream(PipeDirection.Out, HandleInheritability.None);
            var pipeServerToClientReader = new AnonymousPipeClientStream(PipeDirection.In, pipeServerToClient.ClientSafePipeHandle);

            var client = new BidirectionalStreamPair(pipeServerToClientReader, pipeClientToServer);
            var server = new BidirectionalStreamPair(pipeClientToServerReader, pipeServerToClient);

            return (client, server);
        }

        private BidirectionalStreamPair(PipeStream readable, PipeStream writable)
        {
            _readable = readable;
            _writable = writable;
        }

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }

        public override void Flush() => _writable.Flush();
        public override Task FlushAsync(CancellationToken cancellationToken) => _writable.FlushAsync(cancellationToken);
        public override int Read(byte[] buffer, int offset, int count) => _readable.Read(buffer, offset, count);
        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) => _readable.ReadAsync(buffer, cancellationToken);
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => _writable.Write(buffer, offset, count);
        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default) => _writable.WriteAsync(buffer, cancellationToken);

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                _readable.Dispose();
                _writable.Dispose();
            }
            base.Dispose(disposing);
        }
    }

    [Fact]
    public async Task IpcConnectionHandler_RemoteAcquireAndDisconnect_DelegatesAndReleasesRemoteLock()
    {
        var localEngine = new InMemoryLockEngine();
        var mockRemote = new MockRemoteCoordinatorClient();
        var handler = new IpcConnectionHandler(localEngine, mockRemote);

        var (clientStream, serverStream) = CreateConnectedStreamPair();

        using var cts = new CancellationTokenSource();
        var serverTask = Task.Run(() => handler.HandleAsync(serverStream, cts.Token));

        // 1. Acquire remote resource "Strix:CPU"
        var req = new LockRequest("Strix:CPU", "sess-test", 999);
        var acqEnvelope = new IpcEnvelope("msg-1", IpcMessageType.Acquire, JsonSerializer.Serialize(req, IpcFrameCodec.DefaultJsonOptions));
        await IpcFrameCodec.WriteFrameAsync(clientStream, acqEnvelope);

        var respEnvelope = await IpcFrameCodec.ReadFrameAsync(clientStream);
        Assert.NotNull(respEnvelope);
        var result = JsonSerializer.Deserialize<LockResult>(respEnvelope.PayloadJson!, IpcFrameCodec.DefaultJsonOptions);
        Assert.NotNull(result);
        Assert.True(result.Success);
        Assert.Equal("Strix:CPU", result.Resource);

        // Verify remote client was called and local engine was not
        Assert.Single(mockRemote.AcquireCalls);
        Assert.Equal("Strix:CPU", mockRemote.AcquireCalls[0].Resource);
        var localLocks = await localEngine.ListLocksAsync();
        Assert.Empty(localLocks);

        // 2. Disconnect client stream
        clientStream.Dispose();

        await serverTask;

        // Verify remote release was called on disconnect
        Assert.Single(mockRemote.ReleaseCalls);
        Assert.Equal("strix", mockRemote.ReleaseCalls[0].Host);
        Assert.Equal("cpu", mockRemote.ReleaseCalls[0].Resource);
        Assert.Equal("sess-test", mockRemote.ReleaseCalls[0].SessionId);
    }

    [Fact]
    public async Task IpcConnectionHandler_RemoteAcquireAndReleaseAll_ReleasesRemoteLock()
    {
        var localEngine = new InMemoryLockEngine();
        var mockRemote = new MockRemoteCoordinatorClient();
        var handler = new IpcConnectionHandler(localEngine, mockRemote);

        var (clientStream, serverStream) = CreateConnectedStreamPair();

        using var cts = new CancellationTokenSource();
        var serverTask = Task.Run(() => handler.HandleAsync(serverStream, cts.Token));

        // 1. Acquire local lock and remote lock
        var reqLocal = new LockRequest("LocalRAM", "sess-test", 999);
        await IpcFrameCodec.WriteFrameAsync(clientStream, new IpcEnvelope("msg-1", IpcMessageType.Acquire, JsonSerializer.Serialize(reqLocal, IpcFrameCodec.DefaultJsonOptions)));
        await IpcFrameCodec.ReadFrameAsync(clientStream);

        var reqRemote = new LockRequest("Strix:CPU", "sess-test", 999);
        await IpcFrameCodec.WriteFrameAsync(clientStream, new IpcEnvelope("msg-2", IpcMessageType.Acquire, JsonSerializer.Serialize(reqRemote, IpcFrameCodec.DefaultJsonOptions)));
        await IpcFrameCodec.ReadFrameAsync(clientStream);

        // 2. Send ReleaseAll
        var releaseAllEnvelope = new IpcEnvelope("msg-3", IpcMessageType.ReleaseAll, JsonSerializer.Serialize(new { sessionId = "sess-test" }, IpcFrameCodec.DefaultJsonOptions));
        await IpcFrameCodec.WriteFrameAsync(clientStream, releaseAllEnvelope);

        var releaseAllResp = await IpcFrameCodec.ReadFrameAsync(clientStream);
        Assert.NotNull(releaseAllResp);
        using var doc = JsonDocument.Parse(releaseAllResp.PayloadJson!);
        int count = doc.RootElement.GetProperty("count").GetInt32();
        Assert.Equal(2, count); // 1 local + 1 remote

        // Verify remote release was called
        Assert.Single(mockRemote.ReleaseCalls);
        Assert.Equal("strix", mockRemote.ReleaseCalls[0].Host);
        Assert.Equal("cpu", mockRemote.ReleaseCalls[0].Resource);

        // Disconnect and verify no duplicate release
        clientStream.Dispose();
        await serverTask;

        Assert.Single(mockRemote.ReleaseCalls);
    }

    [Fact]
    public async Task EndToEnd_RemoteCoordinatorFederation_AcquireAndDisconnect()
    {
        // Start Remote Coordinator (Coordinator B) with HTTP on a free port
        var engineB = new InMemoryLockEngine();
        await using var serverB = new CoordinatorServer(engineB, httpPort: 0, customPipeOrSocketName: "unused-" + Guid.NewGuid());
        await serverB.StartAsync();

        var remotePort = serverB.HttpPort!.Value;

        // Start Local Coordinator (Coordinator A) forwarding to Coordinator B
        var engineA = new InMemoryLockEngine();
        var remoteClient = new RemoteCoordinatorClient(defaultPort: remotePort);
        var pipeNameA = "ResourceLock-FedA-" + Guid.NewGuid();
        await using var serverA = new CoordinatorServer(engineA, httpPort: -1, customPipeOrSocketName: pipeNameA, remoteCoordinatorClient: remoteClient);
        await serverA.StartAsync();

        // Connect client to Local Coordinator A over NamedPipe
        using var clientStream = new NamedPipeClientStream(".", pipeNameA, PipeDirection.InOut, PipeOptions.Asynchronous);
        await clientStream.ConnectAsync(5000);

        // Register session on Coordinator A
        var regEnvelope = new IpcEnvelope("msg-reg", IpcMessageType.RegisterSession, JsonSerializer.Serialize(new SessionInfo("sess-fed", 1234, DateTimeOffset.UtcNow), IpcFrameCodec.DefaultJsonOptions));
        await IpcFrameCodec.WriteFrameAsync(clientStream, regEnvelope);
        await IpcFrameCodec.ReadFrameAsync(clientStream);

        // Acquire "127.0.0.1:GPU" through Coordinator A -> forwards to Coordinator B
        var acqEnvelope = new IpcEnvelope(
            "msg-acq",
            IpcMessageType.Acquire,
            JsonSerializer.Serialize(new LockRequest("127.0.0.1:GPU", "sess-fed", 1234), IpcFrameCodec.DefaultJsonOptions));
        await IpcFrameCodec.WriteFrameAsync(clientStream, acqEnvelope);

        var acqResp = await IpcFrameCodec.ReadFrameAsync(clientStream);
        Assert.NotNull(acqResp);
        var lockResult = JsonSerializer.Deserialize<LockResult>(acqResp.PayloadJson!, IpcFrameCodec.DefaultJsonOptions);
        Assert.NotNull(lockResult);
        Assert.True(lockResult.Success);

        // Verify lock is held on Coordinator B's engine
        var locksOnB = await engineB.ListLocksAsync();
        Assert.Single(locksOnB);
        Assert.Equal("gpu", locksOnB[0].Resource);
        Assert.Equal("sess-fed", locksOnB[0].SessionId);

        // Verify Coordinator A's engine has NO locks
        var locksOnA = await engineA.ListLocksAsync();
        Assert.Empty(locksOnA);

        // Disconnect client from Coordinator A
        clientStream.Dispose();

        // Wait up to 3 seconds for disconnect cleanup on Coordinator A to release lock on Coordinator B
        var start = DateTime.UtcNow;
        IReadOnlyList<LockRecord> remainingOnB = [];
        while ((DateTime.UtcNow - start).TotalSeconds < 3)
        {
            remainingOnB = await engineB.ListLocksAsync();
            if (remainingOnB.Count == 0)
            {
                break;
            }
            await Task.Delay(50);
        }

        Assert.Empty(remainingOnB);

        await serverA.StopAsync();
        await serverB.StopAsync();
    }
}
