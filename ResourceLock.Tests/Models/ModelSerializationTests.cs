namespace ResourceLock.Tests.Models;

using System.Text.Json;
using ResourceLock.Core.Models;
using Xunit;

public class ModelSerializationTests
{
    [Fact]
    public void LockResult_SerializesAndDeserializes_Correctly()
    {
        var record = new LockRecord("CPU", "sess-1", 1234, DateTimeOffset.UtcNow, null, "test");
        var result = new LockResult(true, "CPU", "sess-1", 1234, "Acquired", null, record);
        var json = JsonSerializer.Serialize(result);
        var deserialized = JsonSerializer.Deserialize<LockResult>(json);

        Assert.NotNull(deserialized);
        Assert.True(deserialized.Success);
        Assert.Equal("CPU", deserialized.Resource);
        Assert.Equal(1234, deserialized.OwnerPid);
    }

    [Fact]
    public void LockRequest_SerializesAndDeserializes_Correctly()
    {
        var request = new LockRequest("GPU", "sess-2", 5678, WaitSeconds: 10, TimeoutSeconds: 60, Reason: "Model training");
        var json = JsonSerializer.Serialize(request);
        var deserialized = JsonSerializer.Deserialize<LockRequest>(json);

        Assert.NotNull(deserialized);
        Assert.Equal("GPU", deserialized.Resource);
        Assert.Equal("sess-2", deserialized.SessionId);
        Assert.Equal(5678, deserialized.OwnerPid);
        Assert.Equal(10, deserialized.WaitSeconds);
        Assert.Equal(60, deserialized.TimeoutSeconds);
        Assert.Equal("Model training", deserialized.Reason);
    }

    [Fact]
    public void SessionInfo_SerializesAndDeserializes_Correctly()
    {
        var now = DateTimeOffset.UtcNow;
        var session = new SessionInfo("sess-3", 9999, now);
        var json = JsonSerializer.Serialize(session);
        var deserialized = JsonSerializer.Deserialize<SessionInfo>(json);

        Assert.NotNull(deserialized);
        Assert.Equal("sess-3", deserialized.SessionId);
        Assert.Equal(9999, deserialized.OwnerPid);
        Assert.Equal(now, deserialized.ConnectedAt);
    }
}
