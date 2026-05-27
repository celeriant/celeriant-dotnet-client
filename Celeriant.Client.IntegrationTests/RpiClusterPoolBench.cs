using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;

namespace Celeriant.Client.IntegrationTests;

/// <summary>
/// Write throughput benchmark against a remote Celeriant cluster using
/// <see cref="CeleriantPool"/> with mTLS.
///
/// <para>
/// Environment variables:
///   RPI_ADDRESS_1   — primary node (default: 10.0.0.50:10000)
///   RPI_ADDRESS_2   — seed node (default: 10.0.0.51:10000)
///   RPI_CA_CERT     — CA cert for server verification (default: ../../../../../../../celeriant-db/deploy/rpi-cluster/certs/client-ca.crt)
///   RPI_CLIENT_CERT — client cert for mTLS (default: ../../../../../../../celeriant-db/deploy/rpi-cluster/certs/client.crt)
///   RPI_CLIENT_KEY  — client key for mTLS (default: ../../../../../../../celeriant-db/deploy/rpi-cluster/certs/client.key)
///   RPI_SERVER_NAME — TLS SNI server name (default: 10.0.0.50)
///   RPI_CONNECTIONS — pool max connections (default: 200)
///   RPI_TASKS       — concurrent writer tasks (default: 2000)
///   RPI_DURATION    — test duration in seconds (default: 15)
/// </para>
///
/// <para>Run with: dotnet test --filter RpiClusterPoolBench</para>
/// </summary>
public sealed class RpiClusterPoolBench
{
    private static string Env(string key, string fallback) =>
        Environment.GetEnvironmentVariable(key) ?? fallback;

    private static string ResolveCertPath(string envKey, string defaultRelative)
    {
        var path = Env(envKey, defaultRelative);
        if (!Path.IsPathRooted(path))
        {
            var testDir = Path.GetDirectoryName(typeof(RpiClusterPoolBench).Assembly.Location)!;
            path = Path.GetFullPath(Path.Combine(testDir, path));
        }
        return path;
    }

    [SkippableFact]
    public async Task Run()
    {
        var addr1 = Env("RPI_ADDRESS_1", "10.0.0.50:10000");
        var addr2 = Env("RPI_ADDRESS_2", "10.0.0.51:10000");
        var serverName = Env("RPI_SERVER_NAME", "10.0.0.50");
        var numTasks = int.Parse(Env("RPI_TASKS", "2000"));
        var maxConns = int.Parse(Env("RPI_CONNECTIONS", "200"));
        var durationSecs = int.Parse(Env("RPI_DURATION", "15"));

        var caCertPath = ResolveCertPath("RPI_CA_CERT",
            "../../../../../../../celeriant-db/deploy/rpi-cluster/certs/client-ca.crt");
        var clientCertPath = ResolveCertPath("RPI_CLIENT_CERT",
            "../../../../../../../celeriant-db/deploy/rpi-cluster/certs/client.crt");
        var clientKeyPath = ResolveCertPath("RPI_CLIENT_KEY",
            "../../../../../../../celeriant-db/deploy/rpi-cluster/certs/client.key");

        Skip.IfNot(File.Exists(caCertPath), $"CA cert not found at {caCertPath}");

        var tlsConfig = BuildMtlsConfig(serverName, caCertPath, clientCertPath, clientKeyPath);

        Console.WriteLine("=== RPi Cluster Pool Benchmark (.NET) ===\n");
        Console.WriteLine($"  Primary:    {addr1}");
        Console.WriteLine($"  Seed:       {addr2}");
        Console.WriteLine($"  TLS:        mTLS");
        Console.WriteLine($"  Pool conns: {maxConns}");
        Console.WriteLine($"  Tasks:      {numTasks}");
        Console.WriteLine($"  Duration:   {durationSecs}s");
        Console.WriteLine();

        await using var pool = new CeleriantPool(new CeleriantPoolOptions
        {
            Address = addr1,
            SeedAddresses = [addr2],
            MaxConnections = maxConns,
            ConnectionTimeout = TimeSpan.FromSeconds(30),
            RequestTimeout = TimeSpan.FromSeconds(30),
            TlsConfig = tlsConfig,
        });

        // Smoke test
        Console.WriteLine("--- Smoke test ---");
        var smokeKey = new AggregateKey(Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid());
        await pool.WriteAsync(smokeKey, [new AggregateEvent
        {
            EventTypeMajor = 1,
            EventValue = "smoke-test"u8.ToArray(),
        }]);
        Console.WriteLine("  Write OK\n");

        // Throughput benchmark
        Console.WriteLine($"--- Throughput ({numTasks} tasks, {durationSecs}s) ---");
        var result = await RunBenchmarkAsync(pool, numTasks, durationSecs);
        PrintResult(result);

        Console.WriteLine("\n=== Done ===");
    }

    private static ClientTlsConfig BuildMtlsConfig(
        string serverName, string caCertPath, string clientCertPath, string clientKeyPath)
    {
        var caCert = X509CertificateLoader.LoadCertificateFromFile(caCertPath);
        var clientCert = X509Certificate2.CreateFromPemFile(clientCertPath, clientKeyPath);

        return ClientTlsConfig.FromSslOptions(new SslClientAuthenticationOptions
        {
            TargetHost = serverName,
            ClientCertificates = new X509Certificate2Collection { clientCert },
            RemoteCertificateValidationCallback = (_, cert, _, _) =>
            {
                if (cert is null) return false;
                using var chain = new X509Chain();
                chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
                chain.ChainPolicy.CustomTrustStore.Add(caCert);
                chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
                return chain.Build(new X509Certificate2(cert));
            },
        });
    }

    private record BenchmarkResult(
        int NumTasks, long TotalRequests, long Errors, double ThroughputPerSec,
        double AvgMs, long P50Ms, long P95Ms, long P99Ms, long MinMs, long MaxMs);

    private static void PrintResult(BenchmarkResult r)
    {
        Console.WriteLine(
            $"  Tasks: {r.NumTasks} | Requests: {r.TotalRequests} | Errors: {r.Errors} | Throughput: {r.ThroughputPerSec:F0} req/s");
        Console.WriteLine(
            $"  Latency — Avg: {r.AvgMs:F1}ms | P50: {r.P50Ms}ms | P95: {r.P95Ms}ms | P99: {r.P99Ms}ms | Min: {r.MinMs}ms | Max: {r.MaxMs}ms");
    }

    private static async Task<BenchmarkResult> RunBenchmarkAsync(
        CeleriantPool pool, int numTasks, int durationSecs)
    {
        long totalOk = 0;
        long totalErr = 0;
        var allLatencies = new ConcurrentBag<long>();
        var barrier = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var sw = Stopwatch.StartNew();

        var tasks = new Task[numTasks];
        for (int id = 0; id < numTasks; id++)
        {
            var taskId = id;
            tasks[id] = Task.Run(async () =>
            {
                var latencies = new List<long>();
                await barrier.Task;
                var deadline = sw.Elapsed + TimeSpan.FromSeconds(durationSecs);
                var seq = 0;

                while (sw.Elapsed < deadline)
                {
                    var key = new AggregateKey(
                        Guid.Parse("00000001-0001-0001-0001-000000000001"),
                        Guid.Parse("00000001-0001-0001-0001-000000000001"),
                        // unique aggregate per task
                        new Guid(0, 0, 0, [(byte)(taskId >> 24), (byte)(taskId >> 16),
                            (byte)(taskId >> 8), (byte)taskId, 0, 0, 0, 0]));

                    var ev = new AggregateEvent
                    {
                        EventTypeMajor = 1,
                        EventValue = Encoding.UTF8.GetBytes($"[t-{taskId}-r-{seq}] hello"),
                    };

                    var reqStart = sw.ElapsedMilliseconds;
                    try
                    {
                        await pool.WriteAsync(key, [ev]);
                        latencies.Add(sw.ElapsedMilliseconds - reqStart);
                        Interlocked.Increment(ref totalOk);
                    }
                    catch (Exception e)
                    {
                        Interlocked.Increment(ref totalErr);
                        Console.Error.WriteLine($"Task {taskId} error: {e.Message}");
                    }
                    seq++;
                }

                foreach (var l in latencies)
                    allLatencies.Add(l);
            });
        }

        barrier.SetResult();
        await Task.WhenAll(tasks);
        sw.Stop();

        var sorted = allLatencies.OrderBy(x => x).ToArray();
        var ok = Interlocked.Read(ref totalOk);
        var errors = Interlocked.Read(ref totalErr);
        var throughput = ok / sw.Elapsed.TotalSeconds;

        if (sorted.Length == 0)
            return new BenchmarkResult(numTasks, ok, errors, throughput, 0, 0, 0, 0, 0, 0);

        return new BenchmarkResult(
            numTasks, ok, errors, throughput,
            AvgMs: sorted.Average(),
            P50Ms: sorted[sorted.Length * 50 / 100],
            P95Ms: sorted[sorted.Length * 95 / 100],
            P99Ms: sorted[sorted.Length * 99 / 100],
            MinMs: sorted[0],
            MaxMs: sorted[^1]);
    }
}
