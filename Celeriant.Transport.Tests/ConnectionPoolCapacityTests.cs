using Xunit.Sdk;

namespace Celeriant.Transport.Tests;

/// <summary>
/// <c>maxConnections</c> is a steady-state capacity, not a budget that erodes over time. A pool that
/// loses a slot per churn cycle still looks healthy for the first few hundred milliseconds of a test
/// and then parks callers for ever, so every wait here carries a hard budget: a leak fails loudly
/// instead of hanging CI.
/// </summary>
public class ConnectionPoolCapacityTests
{
    /// <summary>How long a single lease request may take before it counts as a leaked slot.</summary>
    private static readonly TimeSpan Budget = TimeSpan.FromSeconds(5);

    /// <summary>A connection that is nothing but an identity, a broken flag, and a dispose counter.</summary>
    private sealed class FakeConn(int id) : IAsyncDisposable
    {
        public int Id { get; } = id;

        /// <summary>Read by the pool's <c>isBroken</c> predicate when a pooled entry is reused.</summary>
        public bool Broken { get; set; }

        private int _disposeCount;
        public int DisposeCount => Volatile.Read(ref _disposeCount);

        public ValueTask DisposeAsync()
        {
            Interlocked.Increment(ref _disposeCount);
            return ValueTask.CompletedTask;
        }
    }

    /// <summary>Hands out fresh connections and counts how many the pool asked for.</summary>
    private sealed class CountingFactory
    {
        private int _created;
        public int Created => Volatile.Read(ref _created);

        public Task<FakeConn> CreateAsync(CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();
            return Task.FromResult(new FakeConn(Interlocked.Increment(ref _created)));
        }
    }

    private sealed class TestExceptionFactory : ITransportExceptionFactory
    {
        public Exception Timeout(string message) => new TimeoutException(message);
        public Exception ConnectTimeout(string message) => new TimeoutException(message);
        public Exception ConnectionFailed(string message, Exception? inner = null) => new IOException(message, inner);
        public Exception Protocol(string message, Exception? inner = null) => new InvalidDataException(message, inner);
    }

    private static ConnectionPool<FakeConn> NewPool(int maxConnections, TimeSpan idleTimeout, CountingFactory factory)
        => new(
            "fake://capacity",
            maxConnections,
            idleTimeout,
            factory.CreateAsync,
            conn => conn.Broken,
            new TestExceptionFactory());

    /// <summary>
    /// Borrows one lease under a hard budget. A pool that has lost the slot never completes, so the
    /// wait is bounded twice over: a cancellation token the pool ought to honour, and a watchdog for
    /// when it does not.
    /// </summary>
    private static async Task<PooledLease<FakeConn>> LeaseAsync(ConnectionPool<FakeConn> pool, string what)
    {
        using var budget = new CancellationTokenSource(Budget);
        using var watchdogCts = new CancellationTokenSource();

        var pending = pool.GetConnectionAsync(budget.Token);
        var watchdog = Task.Delay(Budget + TimeSpan.FromSeconds(2), watchdogCts.Token);

        var finished = await Task.WhenAny(pending, watchdog).ConfigureAwait(false);
        watchdogCts.Cancel();

        if (!ReferenceEquals(finished, pending))
            throw new XunitException(
                $"capacity leak: {what} — GetConnectionAsync never completed (waited past its {Budget.TotalSeconds:0.#}s budget). " +
                "A slot the pool should have reclaimed is gone for good.");

        try
        {
            return await pending.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            throw new XunitException(
                $"capacity leak: {what} — GetConnectionAsync blocked for the whole {Budget.TotalSeconds:0.#}s budget waiting for a free slot.");
        }
    }

    /// <summary>Asserts the pool can still hand out <paramref name="max"/> leases at the same time.</summary>
    private static async Task AssertFullCapacityAvailableAsync(ConnectionPool<FakeConn> pool, int max, string phase)
    {
        var held = new List<PooledLease<FakeConn>>(max);
        try
        {
            for (int i = 0; i < max; i++)
                held.Add(await LeaseAsync(pool, $"{phase}: simultaneous lease {i + 1} of {max}"));
        }
        finally
        {
            foreach (var lease in held)
                await lease.DisposeAsync();
        }
    }

    /// <summary>
    /// Contract 1: capacity survives churn. Saturated at the cap, a borrow → mark-broken → return
    /// cycle repeated far more often than there are slots must leave the pool serving lease requests
    /// exactly as it did on the first cycle.
    /// </summary>
    [Fact]
    public async Task BrokenReturnChurnAtCapacity_KeepsServingLeases()
    {
        const int max = 4;
        const int cycles = 500;

        var factory = new CountingFactory();
        await using var pool = NewPool(max, TimeSpan.FromMinutes(5), factory);

        var held = new PooledLease<FakeConn>[max];
        for (int i = 0; i < max; i++)
            held[i] = await LeaseAsync(pool, $"initial saturation lease {i + 1} of {max}");

        try
        {
            for (int cycle = 0; cycle < cycles; cycle++)
            {
                int slot = cycle % max;

                held[slot].Connection.Broken = true;
                held[slot].MarkBroken();
                await held[slot].DisposeAsync();

                held[slot] = await LeaseAsync(
                    pool,
                    $"lease after {cycle + 1} broken-return cycles at cap {max} (factory has created {factory.Created} connections)");
            }
        }
        finally
        {
            foreach (var lease in held)
                await lease.DisposeAsync();
        }

        await AssertFullCapacityAvailableAsync(pool, max, $"after {cycles} broken-return cycles");
    }

    /// <summary>
    /// Contract 2: capacity survives idle reuse under contention. More callers than slots, every
    /// connection returned healthy and reused from idle, must not cost the pool a slot per handoff.
    /// </summary>
    [Fact]
    public async Task ConcurrentIdleReuseAtCapacity_KeepsFullCapacity()
    {
        const int max = 4;
        const int workers = 8;
        const int perWorker = 60;

        var factory = new CountingFactory();
        await using var pool = NewPool(max, TimeSpan.FromMinutes(5), factory);

        var workerTasks = Enumerable.Range(0, workers).Select(worker => Task.Run(async () =>
        {
            for (int i = 0; i < perWorker; i++)
            {
                var lease = await LeaseAsync(pool, $"contending worker {worker}, iteration {i + 1} of {perWorker}");
                await Task.Yield();
                await lease.DisposeAsync();
            }
        })).ToArray();

        var all = Task.WhenAll(workerTasks);
        using var watchdogCts = new CancellationTokenSource();
        var watchdog = Task.Delay(Budget * 6, watchdogCts.Token);
        var finished = await Task.WhenAny(all, watchdog);
        watchdogCts.Cancel();

        if (!ReferenceEquals(finished, all))
            throw new XunitException(
                $"capacity leak: {workers} contending workers did not finish {perWorker} borrow/return rounds each " +
                $"within {(Budget * 6).TotalSeconds:0.#}s at cap {max}; callers are parked on slots that never came back.");

        await all;

        await AssertFullCapacityAvailableAsync(pool, max, $"after {workers * perWorker} contended idle reuses");
    }

    /// <summary>
    /// Contract 3: evicting a stale idle entry frees its slot. Connections returned healthy, left to
    /// pass a very short idle timeout, then re-requested by more callers than there are slots, must
    /// leave the full cap available afterwards. Staleness alone is harmless and contention alone is
    /// harmless; it is a stale entry reclaimed while somebody is waiting for its slot that decides
    /// whether the cap is a steady state or a slowly draining budget.
    /// </summary>
    [Fact]
    public async Task ContendedStaleEntryEviction_KeepsFullCapacity()
    {
        const int max = 3;
        const int workers = 6;
        const int perWorker = 60;
        var idleTimeout = TimeSpan.FromMilliseconds(20);

        var factory = new CountingFactory();
        await using var pool = NewPool(max, idleTimeout, factory);

        // Each worker holds a lease briefly, returns it healthy, then idles for longer than the
        // timeout, so the entry it just parked is stale by the time anybody reaches for it again.
        var workerTasks = Enumerable.Range(0, workers).Select(worker => Task.Run(async () =>
        {
            for (int i = 0; i < perWorker; i++)
            {
                var lease = await LeaseAsync(pool, $"stale-contention worker {worker}, iteration {i + 1} of {perWorker}");
                await lease.DisposeAsync();
                await Task.Delay(idleTimeout + TimeSpan.FromMilliseconds(5));
            }
        })).ToArray();

        var all = Task.WhenAll(workerTasks);
        using var watchdogCts = new CancellationTokenSource();
        var watchdog = Task.Delay(TimeSpan.FromSeconds(60), watchdogCts.Token);
        var finished = await Task.WhenAny(all, watchdog);
        watchdogCts.Cancel();

        if (!ReferenceEquals(finished, all))
            throw new XunitException(
                $"capacity leak: {workers} workers did not finish {perWorker} stale-eviction rounds each within 60s at cap {max}.");

        await all;

        await AssertFullCapacityAvailableAsync(
            pool,
            max,
            $"after {workers * perWorker} contended borrow/return rounds across a {idleTimeout.TotalMilliseconds:0}ms idle timeout");
    }
}
