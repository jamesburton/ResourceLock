namespace ResourceLock.Tests.Cli;

using System;
using System.IO;
using System.Threading.Tasks;
using ResourceLock;
using Xunit;

public class ProgramCliTests
{
    [Theory]
    [InlineData("--version")]
    [InlineData("-v")]
    public async Task Main_VersionFlag_PrintsVersionAndExitsZero(string flag)
    {
        var originalOut = Console.Out;
        using var sw = new StringWriter();
        try
        {
            Console.SetOut(sw);
            var exitCode = await Program.Main(new[] { flag });

            Assert.Equal(0, exitCode);
            var output = sw.ToString();
            Assert.Contains("resourcelock version", output, StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            Console.SetOut(originalOut);
        }
    }

    [Theory]
    [InlineData("--help")]
    [InlineData("-h")]
    [InlineData("/?")]
    public async Task Main_HelpFlag_PrintsUsageAndExitsZero(string flag)
    {
        var originalOut = Console.Out;
        using var sw = new StringWriter();
        try
        {
            Console.SetOut(sw);
            var exitCode = await Program.Main(new[] { flag });

            Assert.Equal(0, exitCode);
            var output = sw.ToString();
            Assert.Contains("Usage:", output);
            Assert.Contains("coordinator", output);
            Assert.Contains("--help", output);
            Assert.Contains("--version", output);
        }
        finally
        {
            Console.SetOut(originalOut);
        }
    }
}
