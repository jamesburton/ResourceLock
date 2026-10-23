namespace ResourceLock.Tests.Ipc;

using System;
using System.IO;
using System.Threading.Tasks;
using ResourceLock.Core.Ipc;
using Xunit;

public class IpcFrameCodecTests
{
    [Fact]
    public async Task WriteAndReadFrame_PreservesEnvelope()
    {
        using var stream = new MemoryStream();
        var envelope = new IpcEnvelope("msg-1", IpcMessageType.Acquire, "{\"resource\":\"CPU\"}");

        await IpcFrameCodec.WriteFrameAsync(stream, envelope);
        stream.Position = 0;
        var read = await IpcFrameCodec.ReadFrameAsync(stream);

        Assert.NotNull(read);
        Assert.Equal("msg-1", read.MessageId);
        Assert.Equal(IpcMessageType.Acquire, read.Type);
        Assert.Equal("{\"resource\":\"CPU\"}", read.PayloadJson);
    }

    [Fact]
    public async Task ReadFrameAsync_EmptyStream_ReturnsNull()
    {
        using var stream = new MemoryStream();
        var read = await IpcFrameCodec.ReadFrameAsync(stream);

        Assert.Null(read);
    }

    [Fact]
    public async Task WriteAndReadMultipleFrames_ReadsSequentially()
    {
        using var stream = new MemoryStream();
        var env1 = new IpcEnvelope("msg-1", IpcMessageType.RegisterSession, "{\"sessionId\":\"s-1\"}");
        var env2 = new IpcEnvelope("msg-2", IpcMessageType.Ping, null);
        var env3 = new IpcEnvelope("msg-3", IpcMessageType.Release, "{\"resource\":\"GPU\"}");

        await IpcFrameCodec.WriteFrameAsync(stream, env1);
        await IpcFrameCodec.WriteFrameAsync(stream, env2);
        await IpcFrameCodec.WriteFrameAsync(stream, env3);

        stream.Position = 0;

        var read1 = await IpcFrameCodec.ReadFrameAsync(stream);
        var read2 = await IpcFrameCodec.ReadFrameAsync(stream);
        var read3 = await IpcFrameCodec.ReadFrameAsync(stream);
        var read4 = await IpcFrameCodec.ReadFrameAsync(stream);

        Assert.NotNull(read1);
        Assert.Equal("msg-1", read1.MessageId);
        Assert.Equal(IpcMessageType.RegisterSession, read1.Type);
        Assert.Equal("{\"sessionId\":\"s-1\"}", read1.PayloadJson);

        Assert.NotNull(read2);
        Assert.Equal("msg-2", read2.MessageId);
        Assert.Equal(IpcMessageType.Ping, read2.Type);
        Assert.Null(read2.PayloadJson);

        Assert.NotNull(read3);
        Assert.Equal("msg-3", read3.MessageId);
        Assert.Equal(IpcMessageType.Release, read3.Type);
        Assert.Equal("{\"resource\":\"GPU\"}", read3.PayloadJson);

        Assert.Null(read4);
    }

    [Fact]
    public async Task ReadFrameAsync_StreamClosedMidHeader_ThrowsEndOfStreamException()
    {
        using var stream = new MemoryStream(new byte[] { 0x00, 0x01 }); // Only 2 bytes instead of 4
        await Assert.ThrowsAsync<EndOfStreamException>(async () =>
        {
            await IpcFrameCodec.ReadFrameAsync(stream);
        });
    }

    [Fact]
    public async Task ReadFrameAsync_StreamClosedMidPayload_ThrowsEndOfStreamException()
    {
        // 4 bytes indicating length 10, but only 3 bytes of payload
        byte[] truncatedFrame = new byte[] { 0x00, 0x00, 0x00, 0x0A, 0x7B, 0x22, 0x61 };
        using var stream = new MemoryStream(truncatedFrame);

        await Assert.ThrowsAsync<EndOfStreamException>(async () =>
        {
            await IpcFrameCodec.ReadFrameAsync(stream);
        });
    }

    [Fact]
    public async Task ReadFrameAsync_NegativeLength_ThrowsInvalidDataException()
    {
        byte[] invalidHeader = new byte[] { 0xFF, 0xFF, 0xFF, 0xFF }; // -1
        using var stream = new MemoryStream(invalidHeader);

        await Assert.ThrowsAsync<InvalidDataException>(async () =>
        {
            await IpcFrameCodec.ReadFrameAsync(stream);
        });
    }

    [Fact]
    public async Task ReadFrameAsync_ExceedsMaxLength_ThrowsInvalidDataException()
    {
        // 100 MB frame length
        byte[] invalidHeader = new byte[] { 0x06, 0x40, 0x00, 0x00 };
        using var stream = new MemoryStream(invalidHeader);

        await Assert.ThrowsAsync<InvalidDataException>(async () =>
        {
            await IpcFrameCodec.ReadFrameAsync(stream);
        });
    }

    [Fact]
    public async Task WriteFrameAsync_ThrowsOnNullArguments()
    {
        using var stream = new MemoryStream();
        var envelope = new IpcEnvelope("msg-1", IpcMessageType.Ping, null);

        await Assert.ThrowsAsync<ArgumentNullException>(() => IpcFrameCodec.WriteFrameAsync(null!, envelope).AsTask());
        await Assert.ThrowsAsync<ArgumentNullException>(() => IpcFrameCodec.WriteFrameAsync(stream, null!).AsTask());
    }

    [Fact]
    public async Task ReadFrameAsync_ThrowsOnNullStream()
    {
        await Assert.ThrowsAsync<ArgumentNullException>(() => IpcFrameCodec.ReadFrameAsync(null!).AsTask());
    }

    [Fact]
    public void IpcConstants_GetNamedPipeName_MatchesExpectedFormat()
    {
        var pipeName = IpcConstants.GetNamedPipeName();
        Assert.Equal($"ResourceLock-Coordinator-{Environment.UserName}", pipeName);
    }

    [Fact]
    public void IpcConstants_GetUnixSocketPath_EndsWithExpectedFileName()
    {
        var socketPath = IpcConstants.GetUnixSocketPath();
        Assert.EndsWith(Path.Combine(".resourcelock", "coordinator.sock"), socketPath);
    }
}
