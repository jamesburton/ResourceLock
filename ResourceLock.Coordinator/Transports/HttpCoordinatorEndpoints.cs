namespace ResourceLock.Coordinator.Transports;

using System.Threading;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using ResourceLock.Core.Engine;
using ResourceLock.Core.Ipc;
using ResourceLock.Core.Models;

public sealed record ReleaseLockHttpRequest(string Resource, string SessionId);
public sealed record ReleaseAllHttpRequest(string SessionId);

public static class HttpCoordinatorEndpoints
{
    public static IEndpointRouteBuilder MapCoordinatorEndpoints(this IEndpointRouteBuilder endpoints, ILockEngine lockEngine)
    {
        endpoints.MapPost("/api/locks/acquire", async (LockRequest request, CancellationToken ct) =>
        {
            var result = await lockEngine.AcquireAsync(request, ct);
            return Results.Json(result, IpcFrameCodec.DefaultJsonOptions);
        });

        endpoints.MapPost("/api/locks/release", async (ReleaseLockHttpRequest request, CancellationToken ct) =>
        {
            var result = await lockEngine.ReleaseAsync(request.Resource, request.SessionId, ct);
            return Results.Json(result, IpcFrameCodec.DefaultJsonOptions);
        });

        endpoints.MapPost("/api/locks/release-all", async (ReleaseAllHttpRequest request, CancellationToken ct) =>
        {
            var count = await lockEngine.ReleaseAllForSessionAsync(request.SessionId, ct);
            return Results.Ok(new { count });
        });

        endpoints.MapGet("/api/locks", async (CancellationToken ct) =>
        {
            var locks = await lockEngine.ListLocksAsync(ct);
            return Results.Json(locks, IpcFrameCodec.DefaultJsonOptions);
        });

        endpoints.MapGet("/api/ping", () => Results.Ok(new { status = "healthy" }));

        return endpoints;
    }
}
