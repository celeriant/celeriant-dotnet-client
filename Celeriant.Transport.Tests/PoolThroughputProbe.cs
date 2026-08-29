using System.Diagnostics;
using System.Reflection;
using Xunit.Abstractions;

namespace Celeriant.Transport.Tests;

/// <summary>
/// Throughput probe for the pool's permit paths. Skipped by default: these print numbers rather
/// than assert them, and a throughput assertion on shared CI hardware is a coin flip. Run them
/// deliberately:
///
/// <code>
/// dotnet test Celeriant.Transport.Tests -c Release \
///   --filter FullyQualifiedName~PoolThroughputProbe \
///   --logger "console;verbosity=detailed"
/// </code>
///
/// <para>
/// The factory is free and there are no sockets, so what is timed is the pool's own bookkeeping
/// and nothing else — which is the point, and also the trap. Read <c>live capacity at rest</c> and
/// <c>dials</c> alongside ops/s, never ops/s alone: a pool that has leaked its permits does LESS
/// work per operation and therefore looks FASTER here, while in reality it is serving from a
/// connection set that is collapsing toward zero. Measured against the pre-permit-fix tree,
/// scenario C came out ~16% "faster" while ending on 1 of 8 permits instead of 6 of 8, and
/// scenario B ran 32 workers over 4 connections instead of 8. With a real factory — a TCP dial —
/// running at half the configured connection count is a throughput halving that this harness
/// cannot see.
/// </para>
///
/// <para>
/// Scenario A is the honest hot-path number: idle reuse never touches the semaphore, and it
/// measured the same on both trees (~246 ns/op, inside noise).
/// </para>
/// </summary>
public class PoolThroughputProbe(ITestOutputHelper output)
{
    private sealed class Conn : IAsyncDisposable
    {
        public bool Broken { get; set; }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class Ex : ITransportExceptionFactory
    {
        public Exception Timeout(string m) => new TimeoutException(m);
        public Exception ConnectTimeout(string m) => new TimeoutException(m);
        public Exception ConnectionFailed(string m, Exception? i = null) => new IOException(m, i);
        public Exception Protocol(string m, Exception? i = null) => new InvalidDataException(m, i);
    }

    private int _created;

    private ConnectionPool<Conn> NewPool(int max, TimeSpan idle)
        => new("probe", max, idle,
            _ => { Interlocked.Increment(ref _created); return Task.FromResult(new Conn()); },
            c => c.Broken, new Ex());

    /// <summary>Permits still available. With nothing leased, this IS the pool's live capacity.</summary>
    private static int Permits(object pool)
        => ((SemaphoreSlim)pool.GetType()
            .GetField("_totalSem", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(pool)!).CurrentCount;

    /// <summary>
    /// Gated on an environment variable rather than a plain <c>Skip</c>, because an unconditional
    /// skip cannot be run by a filter either — and a probe nobody can run is a probe nobody reads.
    /// </summary>
    private static void RequireProbeRun() => Skip.If(
        Environment.GetEnvironmentVariable("CELERIANT_POOL_PROBE") is null,
        "Throughput probe: prints numbers, asserts none. Set CELERIANT_POOL_PROBE=1 to run.");

    private const int Cap = 8;
    private static readonly TimeSpan Deadline = TimeSpan.FromSeconds(30);

    [SkippableFact]
    public async Task A_UncontendedReuse()
    {
        RequireProbeRun();

        await using var pool = NewPool(Cap, TimeSpan.FromMinutes(5));
        const int n = 200_000;

        // Warm up so JIT and the first dial are not in the measurement.
        for (int i = 0; i < 5_000; i++)
            await using (await pool.GetConnectionAsync(default)) { }

        var sw = Stopwatch.StartNew();
        for (int i = 0; i < n; i++)
            await using (await pool.GetConnectionAsync(default)) { }
        sw.Stop();

        Report("A uncontended reuse (1 worker)", n, sw.Elapsed, pool);
    }

    [SkippableFact]
    public async Task B_ContendedAtCap()
    {
        RequireProbeRun();

        await using var pool = NewPool(Cap, TimeSpan.FromMinutes(5));
        const int workers = 32;
        const int per = 8_000;

        var sw = Stopwatch.StartNew();
        await Task.WhenAll(Enumerable.Range(0, workers).Select(_ => Task.Run(async () =>
        {
            for (int i = 0; i < per; i++)
                await using (await pool.GetConnectionAsync(default)) { }
        })));
        sw.Stop();

        Report($"B contended at cap ({workers} workers, cap {Cap})", workers * per, sw.Elapsed, pool);
    }

    [SkippableFact]
    public async Task C_ChurnWithBrokenReturns()
    {
        RequireProbeRun();

        await using var pool = NewPool(Cap, TimeSpan.FromMilliseconds(1));
        const int workers = 16;
        const int per = 2_000;
        using var deadline = new CancellationTokenSource(Deadline);

        var sw = Stopwatch.StartNew();
        int done = 0;
        try
        {
            await Task.WhenAll(Enumerable.Range(0, workers).Select(w => Task.Run(async () =>
            {
                var rng = new Random(w);
                for (int i = 0; i < per; i++)
                {
                    var lease = await pool.GetConnectionAsync(deadline.Token);
                    if (rng.Next(4) == 0)
                        lease.MarkBroken();
                    await lease.DisposeAsync();
                    Interlocked.Increment(ref done);
                }
            })));
        }
        catch (OperationCanceledException)
        {
            output.WriteLine($"C churn: DID NOT COMPLETE — {done}/{workers * per} cycles in {Deadline.TotalSeconds:0}s "
                + "(pool ran out of capacity)");
            return;
        }
        sw.Stop();

        Report($"C churn, 25% broken returns ({workers} workers, cap {Cap}, 1ms idle)", workers * per, sw.Elapsed, pool);
    }

    private void Report(string name, int ops, TimeSpan elapsed, object pool)
        => output.WriteLine(
            $"{name}: {ops:N0} ops in {elapsed.TotalMilliseconds:N0} ms = "
            + $"{ops / elapsed.TotalSeconds:N0} ops/s ({elapsed.TotalMilliseconds * 1_000_000 / ops:N0} ns/op)"
            + $" | live capacity at rest {Permits(pool)}/{Cap} | dials {Volatile.Read(ref _created):N0}");
}
