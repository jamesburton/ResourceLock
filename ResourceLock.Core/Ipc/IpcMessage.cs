namespace ResourceLock.Core.Ipc;

using System.Text.Json.Serialization;

[JsonConverter(typeof(JsonStringEnumConverter))]
public enum IpcMessageType
{
    RegisterSession,
    Acquire,
    Release,
    ReleaseAll,
    List,
    Response,
    Ping
}

public sealed record IpcEnvelope(
    string MessageId,
    IpcMessageType Type,
    string? PayloadJson = null);
