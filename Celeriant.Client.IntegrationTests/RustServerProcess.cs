using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;

namespace Celeriant.Client.IntegrationTests;

/// <summary>How to start one <see cref="RustServerProcess"/>.</summary>
internal sealed record RustServerOptions
{
    /// <summary>Pass <c>--require-client-identity</c>: Identify must carry a verified key pair (else 10004).</summary>
    public bool RequireClientIdentity { get; init; }

    /// <summary>
    /// Raw 32-byte API keys to issue read-write. When set, an <c>api_keys.toml</c> is written into the data
    /// root before start, so every Identify must carry a valid API key (else 10005). Null means no keys file,
    /// so any API key is rejected (10006).
    /// </summary>
    public byte[]? ReadWriteApiKey { get; init; }

    /// <summary>A custom compression dictionary staged as <c>dictionary.zstd_dict</c> in the empty data root before first start.</summary>
    public byte[]? CustomDictionary { get; init; }
}

/// <summary>
/// One Rust <c>celeriant</c> server, spawned standalone with one shard on free loopback ports and a
/// fresh temp data root, killed and deleted on dispose.
///
/// <para>
/// The binary is <c>CELERIANT_SERVER_BIN</c> when set, else <c>target/release/celeriant</c> in a
/// <c>celeriant-db</c> checkout beside this repo. Tests skip with <see cref="SkipReason"/> when
/// neither exists. Build it with <c>cargo build --release -p celeriant</c>.
/// </para>
/// </summary>
internal sealed class RustServerProcess : IAsyncDisposable
{
    private const int LogTailLines = 60;
    private static readonly TimeSpan StartupBudget = TimeSpan.FromSeconds(30);

    private readonly Process _process;
    private readonly Queue<string> _log = new();

    public string Address { get; }
    public int MetricsPort { get; }
    public string DataRoot { get; }

    private RustServerProcess(Process process, string address, int metricsPort, string dataRoot)
    {
        _process = process;
        Address = address;
        MetricsPort = metricsPort;
        DataRoot = dataRoot;
    }

    /// <summary>The server binary, or null when none was found.</summary>
    public static string? BinaryPath { get; } = FindBinary();

    /// <summary>Why a live test cannot run here, or null when it can.</summary>
    public static string? SkipReason => BinaryPath is null
        ? "Rust celeriant server binary not found. Set CELERIANT_SERVER_BIN, or build the celeriant-db checkout beside this repo with `cargo build --release -p celeriant`."
        : null;

    private static string? FindBinary()
    {
        string? fromEnv = Environment.GetEnvironmentVariable("CELERIANT_SERVER_BIN");
        if (!string.IsNullOrWhiteSpace(fromEnv))
            return File.Exists(fromEnv) ? fromEnv : null;

        for (var dir = new DirectoryInfo(AppContext.BaseDirectory); dir is not null; dir = dir.Parent)
        {
            if (!File.Exists(Path.Combine(dir.FullName, "Celeriant.Client.sln")))
                continue;
            string candidate = Path.Combine(dir.Parent?.FullName ?? dir.FullName, "celeriant-db", "target", "release", "celeriant");
            return File.Exists(candidate) ? candidate : null;
        }
        return null;
    }

    public static async Task<RustServerProcess> StartAsync(RustServerOptions options)
    {
        string binary = BinaryPath ?? throw new InvalidOperationException(SkipReason);
        string dataRoot = Directory.CreateTempSubdirectory("celeriant-live-").FullName;
        if (options.ReadWriteApiKey is not null)
            WriteKeysFile(dataRoot, options.ReadWriteApiKey);
        if (options.CustomDictionary is not null)
            await File.WriteAllBytesAsync(Path.Combine(dataRoot, "dictionary.zstd_dict"), options.CustomDictionary);

        int clientPort = FreePort(), replicationPort = FreePort(), metricsPort = FreePort();
        var args = new List<string>
        {
            "--standalone",
            "--num-shards", "1",
            "--data-root", dataRoot,
            "--listen-address", "127.0.0.1",
            "--client-port", clientPort.ToString(),
            "--replication-port", replicationPort.ToString(),
            "--metrics-enabled",
            "--metrics-port", metricsPort.ToString(),
            "--shard-log-preallocate-bytes", (16 * 1024 * 1024).ToString(),
            "--memory-budget-bytes", (256 * 1024 * 1024).ToString(),
            "--insecure-allow-plaintext-auth",
            "--log-level", "warn",
        };
        if (options.RequireClientIdentity)
            args.Add("--require-client-identity");

        var info = new ProcessStartInfo(binary)
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        foreach (string arg in args)
            info.ArgumentList.Add(arg);

        var process = Process.Start(info) ?? throw new InvalidOperationException($"could not start {binary}");
        var server = new RustServerProcess(process, $"127.0.0.1:{clientPort}", metricsPort, dataRoot);
        process.OutputDataReceived += (_, e) => server.Record(e.Data);
        process.ErrorDataReceived += (_, e) => server.Record(e.Data);
        process.BeginOutputReadLine();
        process.BeginErrorReadLine();

        try
        {
            await server.WaitUntilListeningAsync(clientPort);
        }
        catch
        {
            await server.DisposeAsync();
            throw;
        }
        return server;
    }

    /// <summary>The last lines the server logged, for a failure message.</summary>
    public string LogTail()
    {
        lock (_log)
            return string.Join(Environment.NewLine, _log);
    }

    private void Record(string? line)
    {
        if (line is null)
            return;
        lock (_log)
        {
            if (_log.Count == LogTailLines)
                _log.Dequeue();
            _log.Enqueue(line);
        }
    }

    private async Task WaitUntilListeningAsync(int port)
    {
        var deadline = Stopwatch.StartNew();
        while (deadline.Elapsed < StartupBudget)
        {
            if (_process.HasExited)
                throw new InvalidOperationException($"server exited during startup with code {_process.ExitCode}:{Environment.NewLine}{LogTail()}");
            using var probe = new TcpClient();
            try
            {
                await probe.ConnectAsync(IPAddress.Loopback, port);
                return;
            }
            catch (SocketException)
            {
                await Task.Delay(50);
            }
        }
        throw new TimeoutException($"server did not listen on {port} within {StartupBudget}:{Environment.NewLine}{LogTail()}");
    }

    /// <summary>The server's keys file: <paramref name="readWriteKey"/> as primary read-write, fresh random keys in the other slots.</summary>
    private static void WriteKeysFile(string dataRoot, byte[] readWriteKey)
    {
        static string Hash(byte[] key) => Convert.ToHexString(SHA256.HashData(key)).ToLowerInvariant();
        string content =
            "[keys]\n" +
            $"primary_rw = \"{Hash(readWriteKey)}\"\n" +
            $"secondary_rw = \"{Hash(RandomNumberGenerator.GetBytes(32))}\"\n" +
            $"primary_ro = \"{Hash(RandomNumberGenerator.GetBytes(32))}\"\n" +
            $"secondary_ro = \"{Hash(RandomNumberGenerator.GetBytes(32))}\"\n";
        File.WriteAllText(Path.Combine(dataRoot, "api_keys.toml"), content);
    }

    private static int FreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        int port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            if (!_process.HasExited)
                _process.Kill(entireProcessTree: true);
            await _process.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(10));
        }
        catch (InvalidOperationException) { }
        catch (TimeoutException) { }
        _process.Dispose();
        try { Directory.Delete(DataRoot, recursive: true); } catch (IOException) { } catch (UnauthorizedAccessException) { }
    }
}
