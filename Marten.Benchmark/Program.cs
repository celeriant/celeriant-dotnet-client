using System.Diagnostics;
using System.Text;
using Marten;
using Marten.Events;
using Weasel.Core;

// ---------------------------------------------------------------------------
// Configuration (environment variables, mirrors kafka-bench / rpi_cluster_pool_bench)
// ---------------------------------------------------------------------------

var pgHost = Environment.GetEnvironmentVariable("PG_HOST") ?? "localhost";
var pgPort = Environment.GetEnvironmentVariable("PG_PORT") ?? "5432";
var pgDatabase = Environment.GetEnvironmentVariable("PG_DATABASE") ?? "marten_bench";
var pgUser = Environment.GetEnvironmentVariable("PG_USER") ?? "bench";
var pgPassword = Environment.GetEnvironmentVariable("PG_PASSWORD") ?? "bench";

int totalTasks = int.Parse(Environment.GetEnvironmentVariable("BENCH_TASKS") ?? "2000");
int durationSecs = int.Parse(Environment.GetEnvironmentVariable("BENCH_DURATION") ?? "15");
int recordSize = int.Parse(Environment.GetEnvironmentVariable("BENCH_RECORD_SIZE") ?? "256");
int bucketSecs = int.Parse(Environment.GetEnvironmentVariable("BENCH_BUCKET_SECS") ?? "10");

var sslMode = Environment.GetEnvironmentVariable("PG_SSL_MODE") ?? "";
var sslCert = Environment.GetEnvironmentVariable("PG_SSL_CERT") ?? "";
var sslKey = Environment.GetEnvironmentVariable("PG_SSL_KEY") ?? "";
var sslCa = Environment.GetEnvironmentVariable("PG_SSL_CA") ?? "";

var connectionString = $"Host={pgHost};Port={pgPort};Database={pgDatabase};Username={pgUser};Password={pgPassword}" +
                       $";Maximum Pool Size={totalTasks};Minimum Pool Size={Math.Min(totalTasks, 100)}" +
                       ";Timeout=30;Command Timeout=30;Multiplexing=false;No Reset On Close=true";

if (!string.IsNullOrEmpty(sslMode))
{
    connectionString += $";SSL Mode={sslMode};SSL Certificate={sslCert};SSL Key={sslKey};Root Certificate={sslCa}";
}

ThreadPool.SetMinThreads(
    Math.Max(Environment.ProcessorCount * 4, 1024),
    Math.Max(Environment.ProcessorCount * 4, 1024));

Console.WriteLine("=== Marten/PostgreSQL Benchmark ===\n");
Console.WriteLine($"  PostgreSQL:  {pgHost}:{pgPort}/{pgDatabase}" +
                  (string.IsNullOrEmpty(sslMode) ? "" : $" (SSL: {sslMode})"));
Console.WriteLine($"  Tasks:       {totalTasks}");
Console.WriteLine($"  Duration:    {durationSecs}s");
Console.WriteLine($"  Record size: {recordSize} bytes");
Console.WriteLine($"  Bucket size: {bucketSecs}s");
Console.WriteLine();

// ---------------------------------------------------------------------------
// Generate event payload (JSONB-friendly: Marten serializes as JSON)
// ---------------------------------------------------------------------------

// Marten serializes this to JSON. The Payload field provides the bulk of the ~recordSize bytes.
// JSON overhead from field names + quotes is ~30 bytes, so we pad accordingly.
int payloadLen = Math.Max(1, recordSize - 30);
string payloadString = new('x', payloadLen);

// ---------------------------------------------------------------------------
// Configure Marten document store
// ---------------------------------------------------------------------------

using var store = DocumentStore.For(opts =>
{
    opts.Connection(connectionString);
    opts.AutoCreateSchemaObjects = AutoCreate.CreateOrUpdate;
    // Use lightweight identity map: no tracking overhead
    opts.Events.StreamIdentity = StreamIdentity.AsGuid;
});

// Apply schema (creates mt_events, mt_streams tables)
Console.WriteLine("  Applying Marten schema...");
await store.Storage.ApplyAllConfiguredChangesToDatabaseAsync();
Console.WriteLine("  Schema ready.");

// ---------------------------------------------------------------------------
// Smoke test: single event append
// ---------------------------------------------------------------------------

Console.WriteLine("  Smoke test...");
{
    await using var session = store.LightweightSession();
    var streamId = Guid.NewGuid();
    session.Events.Append(streamId, new BenchmarkEvent(payloadString));
    await session.SaveChangesAsync();
    Console.WriteLine("  Smoke test OK.");
}
Console.WriteLine();

// ---------------------------------------------------------------------------
// Benchmark: concurrent event appends with time-bucketed output
// ---------------------------------------------------------------------------

int totalBuckets = Math.Max(1, durationSecs / bucketSecs);
var bucketResults = new BucketStats[totalBuckets];
for (int b = 0; b < totalBuckets; b++)
    bucketResults[b] = new BucketStats();

var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
int readyCount = 0;
long globalErrors = 0;
var overallSw = new Stopwatch();

var tasks = new Task<TaskResult>[totalTasks];
for (int t = 0; t < totalTasks; t++)
{
    int taskId = t;
    tasks[t] = Task.Run(async () =>
    {
        var streamId = Guid.NewGuid();
        long requestCount = 0;
        long errorCount = 0;
        var latencies = new List<long>(256);
        var sw = new Stopwatch();

        // Signal ready and wait for all tasks
        if (Interlocked.Increment(ref readyCount) == totalTasks)
            tcs.SetResult();
        await tcs.Task.ConfigureAwait(false);

        var deadline = Stopwatch.StartNew();

        while (deadline.Elapsed.TotalSeconds < durationSecs)
        {
            sw.Restart();
            try
            {
                await using var session = store.LightweightSession();
                session.Events.Append(streamId, new BenchmarkEvent(payloadString));
                await session.SaveChangesAsync().ConfigureAwait(false);

                long elapsedMs = sw.ElapsedMilliseconds;
                latencies.Add(elapsedMs);
                requestCount++;

                // Record into time bucket
                int bucket = Math.Min((int)(deadline.Elapsed.TotalSeconds / bucketSecs), totalBuckets - 1);
                bucketResults[bucket].Record(elapsedMs);
            }
            catch (Exception ex)
            {
                errorCount++;
                long elapsedMs = sw.ElapsedMilliseconds;
                latencies.Add(elapsedMs);
                requestCount++;

                int bucket = Math.Min((int)(deadline.Elapsed.TotalSeconds / bucketSecs), totalBuckets - 1);
                bucketResults[bucket].RecordError(elapsedMs);

                // Only log the first few errors per task to avoid flooding
                if (errorCount <= 3)
                    Console.Error.WriteLine($"  Task {taskId}: {ex.GetType().Name}: {ex.Message}");
            }
        }

        Interlocked.Add(ref globalErrors, errorCount);
        return new TaskResult(requestCount, errorCount, latencies);
    });
}

// Wait for all tasks to be ready, then start timing
await tcs.Task;
overallSw.Start();
Console.WriteLine($"  All {totalTasks} tasks started, running for {durationSecs}s...");

// Wait for all tasks to complete
var results = new List<TaskResult>();
foreach (var task in tasks)
{
    try { results.Add(await task); }
    catch { }
}

var totalDuration = overallSw.Elapsed;

// ---------------------------------------------------------------------------
// Aggregate and report results
// ---------------------------------------------------------------------------

long totalRequests = results.Sum(r => r.RequestCount);
long totalErrors = results.Sum(r => r.ErrorCount);
var allLatencies = results.SelectMany(r => r.LatenciesMs).ToList();
allLatencies.Sort();

double throughput = totalRequests / totalDuration.TotalSeconds;

Console.WriteLine();

// Summary line (format matches kafka-bench / rpi_cluster_pool_bench for script parsing)
Console.WriteLine($"Tasks: {totalTasks}  Requests: {totalRequests}  Errors: {totalErrors}  Duration: {totalDuration.TotalSeconds:F1}s  Throughput: {throughput:F0} req/s");

if (allLatencies.Count > 0)
{
    double avg = allLatencies.Average();
    long p50 = allLatencies[(int)((long)allLatencies.Count * 50 / 100)];
    long p95 = allLatencies[(int)((long)allLatencies.Count * 95 / 100)];
    long p99 = allLatencies[(int)((long)allLatencies.Count * 99 / 100)];
    long p999 = allLatencies[(int)((long)allLatencies.Count * 999 / 1000)];
    long min = allLatencies[0];
    long max = allLatencies[^1];

    Console.WriteLine($"Latency: avg: {avg:F1}ms  P50: {p50}ms  P95: {p95}ms  P99: {p99}ms  P99.9: {p999}ms  min: {min}ms  max: {max}ms");
}

// ---------------------------------------------------------------------------
// Time-bucketed output (shows degradation over time)
// ---------------------------------------------------------------------------

if (totalBuckets > 1)
{
    Console.WriteLine();
    Console.WriteLine("--- Time buckets ---");
    Console.WriteLine($"{"Window",-12} {"Requests",10} {"Errors",8} {"Throughput",12} {"Avg(ms)",8} {"P50(ms)",8} {"P95(ms)",8} {"P99(ms)",8}");
    Console.WriteLine(new string('-', 84));

    for (int b = 0; b < totalBuckets; b++)
    {
        var bucket = bucketResults[b];
        var snapshot = bucket.Snapshot();
        if (snapshot.Count == 0) continue;

        int startSec = b * bucketSecs;
        int endSec = (b + 1) * bucketSecs;
        string window = $"[{startSec}-{endSec}s]";

        snapshot.Sort();
        double bAvg = snapshot.Average();
        long bP50 = snapshot[(int)((long)snapshot.Count * 50 / 100)];
        long bP95 = snapshot[(int)((long)snapshot.Count * 95 / 100)];
        long bP99 = snapshot[(int)((long)snapshot.Count * 99 / 100)];
        double bThroughput = (double)snapshot.Count / bucketSecs;

        Console.WriteLine($"{window,-12} {snapshot.Count,10} {bucket.Errors,8} {bThroughput,9:F0} /s {bAvg,8:F1} {bP50,8} {bP95,8} {bP99,8}");
    }
}

Console.WriteLine();

if (totalErrors > 0)
    Environment.ExitCode = 1;

return;

// =============================================================================
// Types
// =============================================================================

record BenchmarkEvent(string Payload);

record TaskResult(long RequestCount, long ErrorCount, List<long> LatenciesMs);

class BucketStats
{
    private readonly List<long> _latencies = new(4096);
    private long _errors;
    private readonly object _lock = new();

    public long Errors => Interlocked.Read(ref _errors);

    public void Record(long latencyMs)
    {
        lock (_lock) { _latencies.Add(latencyMs); }
    }

    public void RecordError(long latencyMs)
    {
        lock (_lock) { _latencies.Add(latencyMs); }
        Interlocked.Increment(ref _errors);
    }

    public List<long> Snapshot()
    {
        lock (_lock) { return new List<long>(_latencies); }
    }
}
