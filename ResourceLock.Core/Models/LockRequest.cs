namespace ResourceLock.Core.Models;

using System.Text.Json.Serialization;

public sealed record LockRequest(
    string Resource,
    string SessionId,
    int OwnerPid,
    int WaitSeconds = 0,
    int? TimeoutSeconds = null,
    string? Reason = null,
    [property: JsonPropertyName("renew")] bool Renew = false);
