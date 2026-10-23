namespace ResourceLock.Core.Ipc;

using System;
using System.IO;

public static class IpcConstants
{
    public const int DefaultHttpPort = 51234;
    public const int MaxFrameLength = 16 * 1024 * 1024; // 16 MB

    public static string GetNamedPipeName()
    {
        return $"ResourceLock-Coordinator-{Environment.UserName}";
    }

    public static string GetUnixSocketPath()
    {
        var home = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
        if (string.IsNullOrEmpty(home))
        {
            home = Environment.GetEnvironmentVariable("HOME")
                   ?? Environment.GetEnvironmentVariable("USERPROFILE")
                   ?? ".";
        }
        return Path.Combine(home, ".resourcelock", "coordinator.sock");
    }
}
