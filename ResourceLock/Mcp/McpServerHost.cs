namespace ResourceLock.Mcp;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using ResourceLock.Tools;

/// <summary>
/// Implements standard Model Context Protocol (MCP) JSON-RPC 2.0 stdio server.
/// </summary>
public sealed class McpServerHost
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false
    };

    private static readonly JsonSerializerOptions PrettyJsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = true
    };

    private readonly ResourceLockTools _tools;

    public McpServerHost(ResourceLockTools tools)
    {
        _tools = tools ?? throw new ArgumentNullException(nameof(tools));
    }

    public ResourceLockTools Tools => _tools;

    public async Task RunAsync(Stream inputStream, Stream outputStream, CancellationToken cancellationToken = default)
    {
        using var reader = new StreamReader(inputStream, Encoding.UTF8, detectEncodingFromByteOrderMarks: false, bufferSize: 4096, leaveOpen: true);
        using var writer = new StreamWriter(outputStream, new UTF8Encoding(false), bufferSize: 4096, leaveOpen: true)
        {
            AutoFlush = true
        };

        while (!cancellationToken.IsCancellationRequested)
        {
            string? line;
            try
            {
                line = await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (EndOfStreamException)
            {
                break;
            }

            if (line == null)
            {
                // EOF reached
                break;
            }

            if (string.IsNullOrWhiteSpace(line))
            {
                continue;
            }

            // Handle potential Content-Length prefix (LSP / MCP header framing)
            if (line.StartsWith("Content-Length:", StringComparison.OrdinalIgnoreCase))
            {
                var lengthStr = line["Content-Length:".Length..].Trim();
                if (int.TryParse(lengthStr, out var length))
                {
                    while (!cancellationToken.IsCancellationRequested)
                    {
                        var emptyLine = await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false);
                        if (string.IsNullOrEmpty(emptyLine)) break;
                    }

                    var buffer = new char[length];
                    var charsRead = 0;
                    while (charsRead < length && !cancellationToken.IsCancellationRequested)
                    {
                        var count = await reader.ReadAsync(buffer.AsMemory(charsRead, length - charsRead), cancellationToken).ConfigureAwait(false);
                        if (count == 0) break;
                        charsRead += count;
                    }

                    line = new string(buffer, 0, charsRead);
                }
            }

            if (string.IsNullOrWhiteSpace(line))
            {
                continue;
            }

            var responseJson = await ProcessMessageAsync(line, cancellationToken).ConfigureAwait(false);
            if (responseJson != null)
            {
                await writer.WriteLineAsync(responseJson).ConfigureAwait(false);
            }
        }
    }

    public async Task<string?> ProcessMessageAsync(string messageJson, CancellationToken cancellationToken = default)
    {
        using var doc = JsonDocument.Parse(messageJson);
        var root = doc.RootElement;

        object? id = null;
        if (root.TryGetProperty("id", out var idProp))
        {
            id = idProp.ValueKind switch
            {
                JsonValueKind.Number => idProp.GetInt64(),
                JsonValueKind.String => idProp.GetString(),
                _ => null
            };
        }

        string? method = null;
        if (root.TryGetProperty("method", out var methodProp))
        {
            method = methodProp.GetString();
        }

        if (string.IsNullOrEmpty(method))
        {
            return null;
        }

        switch (method)
        {
            case "initialize":
            {
                var resp = new
                {
                    jsonrpc = "2.0",
                    id,
                    result = new
                    {
                        protocolVersion = "2024-11-05",
                        capabilities = new
                        {
                            tools = new { }
                        },
                        serverInfo = new
                        {
                            name = "ResourceLock",
                            version = "1.0.0"
                        }
                    }
                };
                return JsonSerializer.Serialize(resp, JsonOptions);
            }

            case "notifications/initialized":
            {
                // MCP client notification - no response required
                return null;
            }

            case "ping":
            {
                var resp = new
                {
                    jsonrpc = "2.0",
                    id,
                    result = new { }
                };
                return JsonSerializer.Serialize(resp, JsonOptions);
            }

            case "tools/list":
            {
                var resp = new
                {
                    jsonrpc = "2.0",
                    id,
                    result = new
                    {
                        tools = GetToolsDefinitions()
                    }
                };
                return JsonSerializer.Serialize(resp, JsonOptions);
            }

            case "tools/call":
            {
                if (!root.TryGetProperty("params", out var paramsProp))
                {
                    return CreateToolErrorResponse(id, "Missing 'params' in tools/call request.");
                }

                string? toolName = null;
                if (paramsProp.TryGetProperty("name", out var nameProp))
                {
                    toolName = nameProp.GetString();
                }

                if (string.IsNullOrEmpty(toolName))
                {
                    return CreateToolErrorResponse(id, "Missing tool 'name' in tools/call request.");
                }

                JsonElement arguments = default;
                if (paramsProp.TryGetProperty("arguments", out var argsProp) && argsProp.ValueKind == JsonValueKind.Object)
                {
                    arguments = argsProp;
                }

                return await DispatchToolCallAsync(id, toolName, arguments, cancellationToken).ConfigureAwait(false);
            }

            default:
            {
                if (id == null) return null; // notification
                var errResp = new
                {
                    jsonrpc = "2.0",
                    id,
                    error = new
                    {
                        code = -32601,
                        message = $"Method '{method}' not found."
                    }
                };
                return JsonSerializer.Serialize(errResp, JsonOptions);
            }
        }
    }

    private static object[] GetToolsDefinitions()
    {
        return
        [
            new
            {
                name = "acquire_lock",
                description = "Acquires an exclusive lock on a specified resource (e.g., 'CPU', 'GPU', 'NPU', 'Strix:CPU').",
                inputSchema = new
                {
                    type = "object",
                    properties = new Dictionary<string, object>
                    {
                        ["resource"] = new
                        {
                            type = "string",
                            description = "Identifier of the resource (e.g. 'CPU', 'GPU', 'NPU', 'Strix:CPU')."
                        },
                        ["wait_seconds"] = new
                        {
                            type = "integer",
                            description = "Maximum seconds to block waiting if the resource is currently locked. Set to 0 for non-blocking.",
                            @default = 0
                        },
                        ["timeout_seconds"] = new
                        {
                            type = "integer",
                            description = "Max duration in seconds to hold the lock before automatic release."
                        },
                        ["reason"] = new
                        {
                            type = "string",
                            description = "Short explanation of work being performed."
                        }
                    },
                    required = new[] { "resource" }
                }
            },
            new
            {
                name = "release_lock",
                description = "Releases a previously acquired lock on a resource held by the current session.",
                inputSchema = new
                {
                    type = "object",
                    properties = new Dictionary<string, object>
                    {
                        ["resource"] = new
                        {
                            type = "string",
                            description = "Identifier of the resource to release."
                        }
                    },
                    required = new[] { "resource" }
                }
            },
            new
            {
                name = "list_locks",
                description = "Lists all currently held locks across the system with their owner session, PID, acquisition time, and reason.",
                inputSchema = new
                {
                    type = "object",
                    properties = new Dictionary<string, object>()
                }
            },
            new
            {
                name = "release_all_locks",
                description = "Releases all locks currently held by this session. Recommended before finishing work.",
                inputSchema = new
                {
                    type = "object",
                    properties = new Dictionary<string, object>()
                }
            }
        ];
    }

    private async Task<string> DispatchToolCallAsync(
        object? id,
        string toolName,
        JsonElement arguments,
        CancellationToken cancellationToken)
    {
        try
        {
            switch (toolName)
            {
                case "acquire_lock":
                {
                    string resource = "";
                    int waitSeconds = 0;
                    int? timeoutSeconds = null;
                    string? reason = null;

                    if (arguments.ValueKind == JsonValueKind.Object)
                    {
                        if (arguments.TryGetProperty("resource", out var rProp) || arguments.TryGetProperty("Resource", out rProp))
                        {
                            resource = rProp.GetString() ?? "";
                        }
                        if (arguments.TryGetProperty("wait_seconds", out var wProp) || arguments.TryGetProperty("WaitSeconds", out wProp))
                        {
                            waitSeconds = wProp.GetInt32();
                        }
                        if (arguments.TryGetProperty("timeout_seconds", out var tProp) || arguments.TryGetProperty("TimeoutSeconds", out tProp))
                        {
                            if (tProp.ValueKind == JsonValueKind.Number)
                            {
                                timeoutSeconds = tProp.GetInt32();
                            }
                        }
                        if (arguments.TryGetProperty("reason", out var reaProp) || arguments.TryGetProperty("Reason", out reaProp))
                        {
                            reason = reaProp.GetString();
                        }
                    }

                    if (string.IsNullOrWhiteSpace(resource))
                    {
                        return CreateToolErrorResponse(id, "Parameter 'resource' is required.");
                    }

                    var lockResult = await _tools.AcquireLockAsync(resource, waitSeconds, timeoutSeconds, reason, cancellationToken).ConfigureAwait(false);

                    object payload = lockResult.Success
                        ? new
                        {
                            success = true,
                            resource = lockResult.Resource,
                            session_id = lockResult.SessionId,
                            expires_at = lockResult.ExpiresAt,
                            message = lockResult.Message ?? "Lock acquired successfully."
                        }
                        : new
                        {
                            success = false,
                            resource = lockResult.Resource,
                            message = lockResult.Message ?? "Resource is held by another process.",
                            held_by_session = lockResult.CurrentHolder?.SessionId,
                            held_by_pid = lockResult.CurrentHolder?.OwnerPid,
                            held_reason = lockResult.CurrentHolder?.Reason,
                            expires_at = lockResult.CurrentHolder?.ExpiresAt ?? lockResult.ExpiresAt
                        };

                    return CreateToolSuccessResponse(id, payload);
                }

                case "release_lock":
                {
                    string resource = "";
                    if (arguments.ValueKind == JsonValueKind.Object)
                    {
                        if (arguments.TryGetProperty("resource", out var rProp) || arguments.TryGetProperty("Resource", out rProp))
                        {
                            resource = rProp.GetString() ?? "";
                        }
                    }

                    if (string.IsNullOrWhiteSpace(resource))
                    {
                        return CreateToolErrorResponse(id, "Parameter 'resource' is required.");
                    }

                    var relResult = await _tools.ReleaseLockAsync(resource, cancellationToken).ConfigureAwait(false);
                    var payload = new
                    {
                        success = relResult.Success,
                        resource = relResult.Resource,
                        message = relResult.Message ?? (relResult.Success ? $"Lock on '{relResult.Resource}' released." : "Failed to release lock.")
                    };

                    return CreateToolSuccessResponse(id, payload);
                }

                case "list_locks":
                {
                    var locks = await _tools.ListLocksAsync(cancellationToken).ConfigureAwait(false);
                    var payload = locks.Select(l => new
                    {
                        resource = l.Resource,
                        session_id = l.SessionId,
                        owner_pid = l.OwnerPid,
                        acquired_at = l.AcquiredAt,
                        expires_at = l.ExpiresAt,
                        reason = l.Reason
                    });

                    return CreateToolSuccessResponse(id, payload);
                }

                case "release_all_locks":
                {
                    var res = await _tools.ReleaseAllLocksAsync(cancellationToken).ConfigureAwait(false);
                    var payload = new
                    {
                        success = res.Success,
                        released_count = res.ReleasedCount,
                        resources = res.Resources ?? Array.Empty<string>()
                    };

                    return CreateToolSuccessResponse(id, payload);
                }

                default:
                    return CreateToolErrorResponse(id, $"Unknown tool '{toolName}'.");
            }
        }
        catch (Exception ex)
        {
            return CreateToolErrorResponse(id, $"Tool execution failed: {ex.Message}");
        }
    }

    private static string CreateToolSuccessResponse(object? id, object payload)
    {
        var text = JsonSerializer.Serialize(payload, PrettyJsonOptions);
        var resp = new
        {
            jsonrpc = "2.0",
            id,
            result = new
            {
                content = new[]
                {
                    new
                    {
                        type = "text",
                        text
                    }
                }
            }
        };
        return JsonSerializer.Serialize(resp, JsonOptions);
    }

    private static string CreateToolErrorResponse(object? id, string errorMessage)
    {
        var resp = new
        {
            jsonrpc = "2.0",
            id,
            result = new
            {
                content = new[]
                {
                    new
                    {
                        type = "text",
                        text = errorMessage
                    }
                },
                isError = true
            }
        };
        return JsonSerializer.Serialize(resp, JsonOptions);
    }
}
