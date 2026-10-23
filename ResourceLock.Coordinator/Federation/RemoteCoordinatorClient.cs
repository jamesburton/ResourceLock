namespace ResourceLock.Coordinator.Federation;

using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Net.Http.Json;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using ResourceLock.Coordinator.Transports;
using ResourceLock.Core.Ipc;
using ResourceLock.Core.Models;

public sealed record ParsedRemoteResource(string? Host, string Resource);

public static class RemoteResourceParser
{
    public static ParsedRemoteResource Parse(string resource)
    {
        if (string.IsNullOrWhiteSpace(resource))
        {
            return new ParsedRemoteResource(null, string.Empty);
        }

        var colonIndex = resource.IndexOf(':');
        if (colonIndex < 0)
        {
            return new ParsedRemoteResource(null, resource.Trim().ToLowerInvariant());
        }

        var hostPart = resource[..colonIndex].Trim();
        var resourcePart = resource[(colonIndex + 1)..].Trim();

        var host = string.IsNullOrEmpty(hostPart) ? null : hostPart.ToLowerInvariant();
        var normalizedResource = resourcePart.ToLowerInvariant();

        return new ParsedRemoteResource(host, normalizedResource);
    }
}

public interface IRemoteCoordinatorClient
{
    ValueTask<LockResult> ForwardAcquireAsync(string remoteHost, LockRequest request, CancellationToken ct = default);
    ValueTask<LockResult> ForwardReleaseAsync(string remoteHost, string resource, string sessionId, CancellationToken ct = default);
    ValueTask<IReadOnlyList<LockRecord>> ForwardListAsync(string remoteHost, CancellationToken ct = default);
}

public sealed class RemoteCoordinatorClient : IRemoteCoordinatorClient, IDisposable
{
    private readonly HttpClient _httpClient;
    private readonly bool _disposeClient;
    private readonly int _defaultPort;

    public RemoteCoordinatorClient(HttpClient? httpClient = null, int defaultPort = 51234)
    {
        _defaultPort = defaultPort;
        if (httpClient != null)
        {
            _httpClient = httpClient;
            _disposeClient = false;
        }
        else
        {
            _httpClient = new HttpClient();
            _disposeClient = true;
        }
    }

    public async ValueTask<LockResult> ForwardAcquireAsync(string remoteHost, LockRequest request, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(remoteHost);
        ArgumentNullException.ThrowIfNull(request);

        var parsed = RemoteResourceParser.Parse(request.Resource);
        var remoteRequest = request with { Resource = parsed.Resource };

        try
        {
            var uri = BuildUri(remoteHost, "/api/locks/acquire");
            using var response = await _httpClient.PostAsJsonAsync(uri, remoteRequest, IpcFrameCodec.DefaultJsonOptions, ct).ConfigureAwait(false);

            var contentStream = await response.Content.ReadAsStreamAsync(ct).ConfigureAwait(false);
            var result = await JsonSerializer.DeserializeAsync<LockResult>(contentStream, IpcFrameCodec.DefaultJsonOptions, ct).ConfigureAwait(false);

            if (result != null)
            {
                return result with { Resource = request.Resource };
            }

            return new LockResult(
                Success: false,
                Resource: request.Resource,
                SessionId: request.SessionId,
                OwnerPid: request.OwnerPid,
                Message: $"Empty response from remote coordinator at {remoteHost}",
                ExpiresAt: null);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            return new LockResult(
                Success: false,
                Resource: request.Resource,
                SessionId: request.SessionId,
                OwnerPid: request.OwnerPid,
                Message: $"Failed to contact remote coordinator at {remoteHost}: {ex.Message}",
                ExpiresAt: null);
        }
    }

    public async ValueTask<LockResult> ForwardReleaseAsync(string remoteHost, string resource, string sessionId, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(remoteHost);
        ArgumentNullException.ThrowIfNull(resource);

        var parsed = RemoteResourceParser.Parse(resource);
        var payload = new ReleaseLockHttpRequest(parsed.Resource, sessionId);

        try
        {
            var uri = BuildUri(remoteHost, "/api/locks/release");
            using var response = await _httpClient.PostAsJsonAsync(uri, payload, IpcFrameCodec.DefaultJsonOptions, ct).ConfigureAwait(false);

            var contentStream = await response.Content.ReadAsStreamAsync(ct).ConfigureAwait(false);
            var result = await JsonSerializer.DeserializeAsync<LockResult>(contentStream, IpcFrameCodec.DefaultJsonOptions, ct).ConfigureAwait(false);

            if (result != null)
            {
                return result with { Resource = resource };
            }

            return new LockResult(
                Success: false,
                Resource: resource,
                SessionId: sessionId,
                OwnerPid: null,
                Message: $"Empty response from remote coordinator at {remoteHost}",
                ExpiresAt: null);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            return new LockResult(
                Success: false,
                Resource: resource,
                SessionId: sessionId,
                OwnerPid: null,
                Message: $"Failed to contact remote coordinator at {remoteHost}: {ex.Message}",
                ExpiresAt: null);
        }
    }

    public async ValueTask<IReadOnlyList<LockRecord>> ForwardListAsync(string remoteHost, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(remoteHost);

        try
        {
            var uri = BuildUri(remoteHost, "/api/locks");
            using var response = await _httpClient.GetAsync(uri, ct).ConfigureAwait(false);

            if (!response.IsSuccessStatusCode)
            {
                return Array.Empty<LockRecord>();
            }

            var contentStream = await response.Content.ReadAsStreamAsync(ct).ConfigureAwait(false);
            var records = await JsonSerializer.DeserializeAsync<List<LockRecord>>(contentStream, IpcFrameCodec.DefaultJsonOptions, ct).ConfigureAwait(false);

            return records ?? (IReadOnlyList<LockRecord>)Array.Empty<LockRecord>();
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception)
        {
            return Array.Empty<LockRecord>();
        }
    }

    private Uri BuildUri(string remoteHost, string path)
    {
        var cleanPath = path.StartsWith('/') ? path : "/" + path;

        if (Uri.TryCreate(remoteHost, UriKind.Absolute, out var uri) &&
            (uri.Scheme.Equals("http", StringComparison.OrdinalIgnoreCase) ||
             uri.Scheme.Equals("https", StringComparison.OrdinalIgnoreCase)))
        {
            var builder = new UriBuilder(uri);
            builder.Path = cleanPath;
            return builder.Uri;
        }

        if (_httpClient.BaseAddress != null &&
            (_httpClient.BaseAddress.Host.Equals(remoteHost, StringComparison.OrdinalIgnoreCase) ||
             remoteHost.Equals("localhost", StringComparison.OrdinalIgnoreCase)))
        {
            return new Uri(_httpClient.BaseAddress, cleanPath);
        }

        var colonIdx = remoteHost.IndexOf(':');
        if (colonIdx > 0 && int.TryParse(remoteHost[(colonIdx + 1)..], out var parsedPort))
        {
            var hostWithoutPort = remoteHost[..colonIdx];
            return new Uri($"http://{hostWithoutPort}:{parsedPort}{cleanPath}");
        }

        return new Uri($"http://{remoteHost}:{_defaultPort}{cleanPath}");
    }

    public void Dispose()
    {
        if (_disposeClient)
        {
            _httpClient.Dispose();
        }
    }
}
