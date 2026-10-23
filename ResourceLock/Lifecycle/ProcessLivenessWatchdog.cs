namespace ResourceLock.Lifecycle;

using System;
using System.Diagnostics;

public static class ProcessLivenessWatchdog
{
    public static bool IsPidAlive(int pid)
    {
        if (pid <= 0)
        {
            return false;
        }

        try
        {
            using var process = Process.GetProcessById(pid);
            return !process.HasExited;
        }
        catch (ArgumentException)
        {
            // Process does not exist
            return false;
        }
        catch (InvalidOperationException)
        {
            // Process has already exited or could not be inspected
            return false;
        }
        catch (Exception)
        {
            // Access denied or permission error indicates the process exists under another privilege ring
            return true;
        }
    }
}
