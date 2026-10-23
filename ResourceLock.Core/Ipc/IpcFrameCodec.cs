namespace ResourceLock.Core.Ipc;

using System;
using System.Buffers.Binary;
using System.IO;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;

public static class IpcFrameCodec
{
    public static readonly JsonSerializerOptions DefaultJsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        Converters = { new JsonStringEnumConverter() }
    };

    public static async ValueTask WriteFrameAsync(Stream stream, IpcEnvelope envelope, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(stream);
        ArgumentNullException.ThrowIfNull(envelope);

        byte[] jsonBytes = JsonSerializer.SerializeToUtf8Bytes(envelope, DefaultJsonOptions);
        int length = jsonBytes.Length;

        byte[] frame = new byte[4 + length];
        BinaryPrimitives.WriteInt32BigEndian(frame.AsSpan(0, 4), length);
        jsonBytes.CopyTo(frame.AsSpan(4));

        await stream.WriteAsync(frame.AsMemory(), ct).ConfigureAwait(false);
        await stream.FlushAsync(ct).ConfigureAwait(false);
    }

    public static async ValueTask<IpcEnvelope?> ReadFrameAsync(Stream stream, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(stream);

        byte[] header = new byte[4];
        int firstByteRead = await stream.ReadAsync(header.AsMemory(0, 1), ct).ConfigureAwait(false);
        if (firstByteRead == 0)
        {
            // Graceful EOF / disconnected stream
            return null;
        }

        // Read remaining 3 bytes of length prefix
        await stream.ReadExactlyAsync(header.AsMemory(1, 3), ct).ConfigureAwait(false);

        int length = BinaryPrimitives.ReadInt32BigEndian(header);
        if (length <= 0)
        {
            throw new InvalidDataException($"Invalid frame length: {length}. Length must be positive.");
        }

        if (length > IpcConstants.MaxFrameLength)
        {
            throw new InvalidDataException($"Frame length {length} exceeds maximum allowed size of {IpcConstants.MaxFrameLength} bytes.");
        }

        byte[] payload = new byte[length];
        await stream.ReadExactlyAsync(payload.AsMemory(0, length), ct).ConfigureAwait(false);

        var envelope = JsonSerializer.Deserialize<IpcEnvelope>(payload, DefaultJsonOptions);
        return envelope ?? throw new InvalidDataException("Decoded IPC frame envelope was null.");
    }
}
