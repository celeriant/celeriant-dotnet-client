using System.Diagnostics;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using Celeriant.Client;
using Celeriant.Client.Errors;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;

const int ThroughputConnections = 24_000;
const int LatencyConnections = 1_000;
const int TestDurationSecs = 15;
const int NumAggregates = 1024;
const int PreAllocatedRequests = 64;
const int ClientTimeoutSecs = 5;

// Ensure the thread pool can handle high-connection async I/O
ThreadPool.SetMinThreads(
    Math.Max(Environment.ProcessorCount * 4, 1024),
    Math.Max(Environment.ProcessorCount * 4, 1024));

const double StandaloneThroughputMin = 297_500.0;
const double StandaloneLatencyAvgMaxMs = 23.0;
const long StandaloneLatencyP99MaxMs = 31;

// --- Generate API keys ---
byte[] primaryRwKey = RandomNumberGenerator.GetBytes(32);
byte[] primaryRwHash = SHA256.HashData(primaryRwKey);
byte[] secondaryRwHash = SHA256.HashData(RandomNumberGenerator.GetBytes(32));
byte[] primaryRoHash = SHA256.HashData(RandomNumberGenerator.GetBytes(32));
byte[] secondaryRoHash = SHA256.HashData(RandomNumberGenerator.GetBytes(32));

string apiKeyBase64 = Convert.ToBase64String(primaryRwKey);

// --- Find server binary ---
string serverBinary = FindServerBinary();
Console.WriteLine($"  Server binary: {serverBinary}");

// --- Create temp data dir with API keys file ---
string tempDir = Path.Combine(Path.GetTempPath(), $"celeriant-bench-{Environment.ProcessId}");
Directory.CreateDirectory(tempDir);

string apiKeysContent = $"""
    [keys]
    primary_rw = "{Convert.ToHexString(primaryRwHash).ToLowerInvariant()}"
    secondary_rw = "{Convert.ToHexString(secondaryRwHash).ToLowerInvariant()}"
    primary_ro = "{Convert.ToHexString(primaryRoHash).ToLowerInvariant()}"
    secondary_ro = "{Convert.ToHexString(secondaryRoHash).ToLowerInvariant()}"
    """;
File.WriteAllText(Path.Combine(tempDir, "api_keys.toml"), apiKeysContent);

// --- Choose port ---
ushort basePort = (ushort)(10100 + (Environment.ProcessId % 100));
string address = $"127.0.0.1:{basePort}";

// --- Start server ---
Console.WriteLine("=== Standalone Cleartext Batch Write Benchmark (C#) ===\n");

var serverArgs = BuildServerArgs(tempDir, basePort);
Console.WriteLine($"  Starting server on port {basePort}...");

var server = new Process();
server.StartInfo.FileName = serverBinary;
server.StartInfo.Arguments = string.Join(" ", serverArgs);
server.StartInfo.UseShellExecute = false;
server.StartInfo.RedirectStandardOutput = true;
server.StartInfo.RedirectStandardError = true;
server.Start();

// Drain stdout/stderr to prevent buffer deadlocks
server.BeginOutputReadLine();
server.BeginErrorReadLine();

try
{
    await PollServerReady(address);
    Console.WriteLine("  Server is ready.\n");

    var identityConfig = new ClientIdentityConfig { ApiKeyBase64 = apiKeyBase64 };

    // --- Quick diagnostic with 1 connection ---
    Console.WriteLine("--- Diagnostic (1 connection) ---");
    {
        var diagClient = await CeleriantClient.ConnectAsync(address, connectionTimeout: TimeSpan.FromSeconds(5));
        await diagClient.IdentifyAsync(identityConfig);
        var diagReqs = PreAllocateRequests(0, Guid.NewGuid());
        var response = await diagClient.SendRequestAsync(diagReqs[0]);
        Console.WriteLine($"  Response: {response.GetType().Name}");
        await diagClient.DisposeAsync();
    }
    var diagResult = await RunBenchmarkIteration(address, 1, identityConfig, useAsync: false);
    Console.WriteLine($"  Diagnostic OK: {diagResult.TotalRequests} requests");

    // --- Throughput ---
    Console.WriteLine($"\n--- Throughput ({ThroughputConnections} connections) ---");
    var thruResult = await RunBenchmarkIteration(address, ThroughputConnections, identityConfig, useAsync: true);
    PrintResult(thruResult);
    var thruFailures = CheckThresholds(thruResult,
        minThroughput: StandaloneThroughputMin, maxAvgLatencyMs: null, maxP99LatencyMs: null);

    await Task.Delay(2000);

    // --- Latency ---
    Console.WriteLine($"\n--- Latency ({LatencyConnections} connections) ---");
    var latResult = await RunBenchmarkIteration(address, LatencyConnections, identityConfig, useAsync: false);
    PrintResult(latResult);
    var latFailures = CheckThresholds(latResult,
        minThroughput: null, maxAvgLatencyMs: StandaloneLatencyAvgMaxMs, maxP99LatencyMs: StandaloneLatencyP99MaxMs);

    // --- Report ---
    Console.WriteLine($"\n\n{"".PadRight(80, '=')}");
    Console.WriteLine("  RESULTS");
    Console.WriteLine($"{"".PadRight(80, '=')}\n");

    Console.WriteLine($"{"Scenario",-20} {"Conns",8} {"Throughput",14} {"Avg (ms)",10} {"P99 (ms)",8} {"Result",8}");
    Console.WriteLine(new string('-', 78));

    foreach (var (label, result, failures) in new[]
    {
        ("Throughput", thruResult, thruFailures),
        ("Latency", latResult, latFailures),
    })
    {
        string status = failures.Count == 0 ? "PASS" : "FAIL";
        Console.WriteLine($"{label,-20} {result.NumConnections,8} {result.Throughput,11:F0} /s {result.AvgLatencyMs,10:F1} {result.P99Ms,8} {status,8}");
        foreach (var f in failures)
            Console.WriteLine($"  >> {f}");
    }

    bool hasFailures = thruFailures.Count > 0 || latFailures.Count > 0;
    if (hasFailures)
    {
        Console.Error.WriteLine("Performance regression detected: thresholds breached");
        Environment.ExitCode = 1;
    }
}
finally
{
    if (!server.HasExited)
    {
        server.Kill();
        server.WaitForExit();
    }
    Console.WriteLine("  Server shut down.");

    try { Directory.Delete(tempDir, recursive: true); }
    catch { /* best effort cleanup */ }
}

return;

// =============================================================================
// Helper methods
// =============================================================================

string FindServerBinary()
{
    string workspaceRoot = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", "..", "rust"));
    string releasePath = Path.Combine(workspaceRoot, "target", "release", "celeriant");
    if (File.Exists(releasePath))
        return releasePath;

    string? pathBinary = FindInPath("celeriant");
    if (pathBinary != null)
        return pathBinary;

    throw new FileNotFoundException(
        $"Server binary not found. Build it first: cd rust && cargo build --release -p celeriant\n" +
        $"Looked in: {releasePath}");
}

string? FindInPath(string executable)
{
    string? pathEnv = Environment.GetEnvironmentVariable("PATH");
    if (pathEnv == null) return null;
    foreach (string dir in pathEnv.Split(Path.PathSeparator))
    {
        string full = Path.Combine(dir, executable);
        if (File.Exists(full)) return full;
    }
    return null;
}

List<string> BuildServerArgs(string dataRoot, ushort port)
{
    return
    [
        "--data-root", dataRoot,
        "--listen-address", "0.0.0.0",
        "--client-port", port.ToString(),
        "--replication-port", (port + 1).ToString(),
        "--standalone",
        "--log-level", "warn",
        "--require-client-identity",
        "--insecure-allow-plaintext-auth",
        "--tls-mode", "disabled",
        "--tls-client-auth", "none",
    ];
}

async Task PollServerReady(string addr)
{
    var (host, port) = ParseAddress(addr);
    var sw = Stopwatch.StartNew();
    var maxWait = TimeSpan.FromSeconds(30);

    while (sw.Elapsed < maxWait)
    {
        try
        {
            using var probe = new TcpClient();
            await probe.ConnectAsync(host, port);
            Console.WriteLine($"  Server ready in {sw.Elapsed.TotalSeconds:F2}s");
            return;
        }
        catch (SocketException)
        {
            await Task.Delay(100);
        }
    }

    throw new TimeoutException("Server failed to start within 30s");
}

ClientRequest.Write[] PreAllocateRequests(int connectionId, Guid writeClientId)
{
    Guid eventId = Guid.Parse("00000000-0000-0000-0000-0000499602d2");
    Guid orgGuid = new Guid(1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    Guid typeGuid = new Guid(1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    byte[] eventValue = Encoding.UTF8.GetBytes($"[conn-{connectionId}] Hello World!");

    var requests = new ClientRequest.Write[PreAllocatedRequests];
    for (int i = 0; i < PreAllocatedRequests; i++)
    {
        int aggId = (connectionId * PreAllocatedRequests + i) % NumAggregates;
        requests[i] = new ClientRequest.Write(new WriteRequest
        {
            ClientId = writeClientId,
            Writes = new Dictionary<AggregateKey, SingleAggregateWrite>
            {
                [new AggregateKey(orgGuid, typeGuid, new Guid(aggId, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))] = new SingleAggregateWrite
                {
                    Events =
                    [
                        new AggregateEvent
                        {
                            ClientSeq = 3,
                            EventSeq = 0,
                            EventId = eventId,
                            EventTimestamp = DateTimeOffset.UnixEpoch,
                            EventTypeMajor = 2,
                            EventTypeMinor = 3,
                            EventValue = eventValue,
                        }
                    ],
                    AllowCreate = true,
                }
            }
        });
    }
    return requests;
}

async Task<BenchmarkResult> RunBenchmarkIteration(string addr, int numConnections, ClientIdentityConfig identityConfig, bool useAsync)
{
    var connectSw = Stopwatch.StartNew();

    // Connect all clients in parallel
    var connectTasks = new Task<(int connectionId, CeleriantClient client, Guid? clientId)>[numConnections];
    for (int i = 0; i < numConnections; i++)
    {
        int connId = i;
        connectTasks[i] = Task.Run(async () =>
        {
            var client = await CeleriantClient.ConnectAsync(addr,
                connectionTimeout: TimeSpan.FromSeconds(ClientTimeoutSecs));

            var clientId = await client.IdentifyAsync(identityConfig);
            return (connId, client, clientId);
        });
    }

    var clients = new List<(int connectionId, CeleriantClient client, Guid? clientId)>();
    int failedConnections = 0;
    Exception? firstError = null;
    foreach (var task in connectTasks)
    {
        try
        {
            clients.Add(await task);
        }
        catch (Exception ex)
        {
            failedConnections++;
            firstError ??= ex;
        }
    }

    if (failedConnections > 0)
        Console.Error.WriteLine($"  Connection errors: {failedConnections} ({firstError?.GetType().Name}: {firstError?.Message})");

    Console.WriteLine($"  Established {clients.Count} connections in {connectSw.Elapsed.TotalSeconds:F2}s ({failedConnections} failed)");

    if (clients.Count == 0)
        throw new Exception("No connections established");

    int actualConnections = clients.Count;

    var overallSw = new Stopwatch();
    var benchTasks = new Task<TaskStats>[actualConnections];

    if (useAsync)
    {
        // Async: one task per connection using SendRequestAsync
        var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        int readyCount = 0;

        for (int i = 0; i < actualConnections; i++)
        {
            var (connectionId, client, clientId) = clients[i];
            Guid writeClientId = clientId ?? Guid.NewGuid();
            var requests = PreAllocateRequests(connectionId, writeClientId);

            benchTasks[i] = Task.Run(async () =>
            {
                long requestCount = 0;
                var latencies = new List<long>(256);

                if (Interlocked.Increment(ref readyCount) == actualConnections)
                    tcs.SetResult();
                await tcs.Task.ConfigureAwait(false);

                var deadline = Stopwatch.StartNew();
                var sw = new Stopwatch();

                while (deadline.Elapsed.TotalSeconds < TestDurationSecs)
                {
                    var request = requests[(int)(requestCount % PreAllocatedRequests)];
                    sw.Restart();

                    try
                    {
                        await client.SendRequestAsync(request).ConfigureAwait(false);
                        latencies.Add(sw.ElapsedMilliseconds);
                        requestCount++;
                    }
                    catch (CeleriantErrorException ex)
                    {
                        Console.Error.WriteLine($"  Conn {connectionId} error: {ex.Error.ErrorCode}");
                        latencies.Add(sw.ElapsedMilliseconds);
                        requestCount++;
                    }
                    catch (Exception ex)
                    {
                        Console.Error.WriteLine($"  Conn {connectionId}: {ex.GetType().Name}: {ex.Message}");
                        break;
                    }
                }

                return new TaskStats(requestCount, latencies);
            });
        }

        await tcs.Task;
        overallSw.Start();
    }
    else
    {
        // Sync: dedicated OS threads using SendRequest
        var barrier = new CountdownEvent(actualConnections);
        var startSignal = new ManualResetEventSlim(false);

        for (int i = 0; i < actualConnections; i++)
        {
            var (connectionId, client, clientId) = clients[i];
            Guid writeClientId = clientId ?? Guid.NewGuid();
            var requests = PreAllocateRequests(connectionId, writeClientId);

            benchTasks[i] = Task.Factory.StartNew(() =>
            {
                long requestCount = 0;
                var latencies = new List<long>(256);

                barrier.Signal();
                startSignal.Wait();

                var deadline = Stopwatch.StartNew();
                var sw = new Stopwatch();

                while (deadline.Elapsed.TotalSeconds < TestDurationSecs)
                {
                    var request = requests[(int)(requestCount % PreAllocatedRequests)];
                    sw.Restart();

                    try
                    {
                        client.SendRequest(request);
                        latencies.Add(sw.ElapsedMilliseconds);
                        requestCount++;
                    }
                    catch (CeleriantErrorException ex)
                    {
                        Console.Error.WriteLine($"  Conn {connectionId} error: {ex.Error.ErrorCode}");
                        latencies.Add(sw.ElapsedMilliseconds);
                        requestCount++;
                    }
                    catch (Exception ex)
                    {
                        Console.Error.WriteLine($"  Conn {connectionId}: {ex.GetType().Name}: {ex.Message}");
                        break;
                    }
                }

                return new TaskStats(requestCount, latencies);
            }, TaskCreationOptions.LongRunning);
        }

        barrier.Wait();
        overallSw.Start();
        startSignal.Set();
    }

    var allStats = new List<TaskStats>();
    foreach (var task in benchTasks)
    {
        try { allStats.Add(await task); }
        catch { }
    }

    var totalDuration = overallSw.Elapsed;

    // Dispose all clients
    foreach (var (_, client, _) in clients)
        await client.DisposeAsync();

    // Aggregate results
    long totalRequests = allStats.Sum(s => s.RequestCount);
    var allLatencies = allStats.SelectMany(s => s.LatenciesMs).ToList();
    allLatencies.Sort();

    double throughput = totalRequests / totalDuration.TotalSeconds;

    double avgMs = 0;
    long p50 = 0, p95 = 0, p99 = 0, p999 = 0, min = 0, max = 0;
    if (allLatencies.Count > 0)
    {
        avgMs = allLatencies.Average();
        p50 = allLatencies[allLatencies.Count * 50 / 100];
        p95 = allLatencies[allLatencies.Count * 95 / 100];
        p99 = allLatencies[allLatencies.Count * 99 / 100];
        p999 = allLatencies[allLatencies.Count * 999 / 1000];
        min = allLatencies[0];
        max = allLatencies[^1];
    }

    return new BenchmarkResult(actualConnections, totalRequests, throughput, avgMs, p50, p95, p99, p999, min, max);
}

void PrintResult(BenchmarkResult r)
{
    Console.WriteLine(
        $"  Throughput: {r.Throughput:F0} req/s | Avg: {r.AvgLatencyMs:F1}ms | P50: {r.P50Ms}ms | P95: {r.P95Ms}ms | P99: {r.P99Ms}ms | P99.9: {r.P999Ms}ms");
}

List<string> CheckThresholds(BenchmarkResult result, double? minThroughput, double? maxAvgLatencyMs, long? maxP99LatencyMs)
{
    var failures = new List<string>();
    if (minThroughput.HasValue && result.Throughput < minThroughput.Value)
        failures.Add($"throughput {result.Throughput:F0} req/s < minimum {minThroughput.Value:F0} req/s");
    if (maxAvgLatencyMs.HasValue && result.AvgLatencyMs > maxAvgLatencyMs.Value)
        failures.Add($"avg latency {result.AvgLatencyMs:F1}ms > maximum {maxAvgLatencyMs.Value:F1}ms");
    if (maxP99LatencyMs.HasValue && result.P99Ms > maxP99LatencyMs.Value)
        failures.Add($"p99 latency {result.P99Ms}ms > maximum {maxP99LatencyMs.Value}ms");
    return failures;
}

(string host, int port) ParseAddress(string addr)
{
    int lastColon = addr.LastIndexOf(':');
    return (addr[..lastColon], int.Parse(addr[(lastColon + 1)..]));
}

// =============================================================================
// Data types
// =============================================================================

record TaskStats(long RequestCount, List<long> LatenciesMs);

record BenchmarkResult(
    int NumConnections,
    long TotalRequests,
    double Throughput,
    double AvgLatencyMs,
    long P50Ms,
    long P95Ms,
    long P99Ms,
    long P999Ms,
    long MinMs,
    long MaxMs);
