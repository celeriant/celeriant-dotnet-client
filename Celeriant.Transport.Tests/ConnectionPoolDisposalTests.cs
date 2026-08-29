using System.Diagnostics;
using System.Reflection;
using Xunit.Abstractions;

namespace Celeriant.Transport.Tests;

/// <summary>
/// What a <see cref="ConnectionPool{TConn}"/> owes callers who are mid-flight when it is disposed,
/// plus the permit accounting that has to hold while it is not.
///
/// <para>
/// Disposal that does not cancel first can strand a parked caller for ever and drop a freshly
/// dialled connection with its socket open. The <c>Bcl_</c> tests pin the platform behaviour
/// those failure modes rest on — a pending <c>SemaphoreSlim.WaitAsync</c> is not completed by
/// disposing the semaphore, and cannot be cancelled afterwards. Cancelling first is necessary
/// but not sufficient: the cancellation completes the wait <em>asynchronously</em>, so a dispose
/// that follows the cancel can still win the race and strand the waiter — which is why the pool
/// wakes waiters by cancellation and never disposes its semaphores at all.
/// </para>
/// </summary>
public class ConnectionPoolDisposalTests(ITestOutputHelper output)
{
    private sealed class Conn(int id) : IAsyncDisposable
    {
        public int Id { get; } = id;
        public bool Broken { get; set; }
        private int _disposeCount;
        public int DisposeCount => Volatile.Read(ref _disposeCount);
        public ValueTask DisposeAsync()
        {
            Interlocked.Increment(ref _disposeCount);
            return ValueTask.CompletedTask;
        }
    }

    private sealed class Ex : ITransportExceptionFactory
    {
        public Exception Timeout(string m) => new TimeoutException(m);
        public Exception ConnectTimeout(string m) => new TimeoutException(m);
        public Exception ConnectionFailed(string m, Exception? i = null) => new IOException(m, i);
        public Exception Protocol(string m, Exception? i = null) => new InvalidDataException(m, i);
    }

    private static ConnectionPool<Conn> NewPool(
        int max,
        TimeSpan idle,
        Func<CancellationToken, Task<Conn>> factory)
        => new("fake://evidence", max, idle, factory, c => c.Broken, new Ex());

    private static SemaphoreSlim TotalSem(object pool)
        => (SemaphoreSlim)pool.GetType()
            .GetField("_totalSem", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(pool)!;

    // ---------------------------------------------------------------------
    // P1: does a PENDING SemaphoreSlim.WaitAsync fault when the semaphore is
    // disposed underneath it? The new slotFault branch's comment asserts it does.
    // ---------------------------------------------------------------------
    [Fact]
    public async Task Bcl_DisposingASemaphore_DoesNotCompleteAPendingWait()
    {
        var sem = new SemaphoreSlim(0, 4);
        var wait = sem.WaitAsync(CancellationToken.None);
        await Task.Delay(50);
        sem.Dispose();

        var done = await Task.WhenAny(wait, Task.Delay(500));
        if (done != wait)
        {
            output.WriteLine("P1: pending WaitAsync NEVER completed after Dispose() (no fault, no cancel).");
            return;
        }
        output.WriteLine($"P1: pending WaitAsync completed: status={wait.Status} ex={wait.Exception?.InnerException?.GetType().Name}");
    }

    // ---------------------------------------------------------------------
    // D1: pool disposed while the connection factory is in flight. The freshly
    // created connection must not be dropped with its socket open.
    // ---------------------------------------------------------------------
    [Fact]
    public async Task DisposeDuringAnInFlightDial_ClosesTheConnectionItCreated()
    {
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Conn? created = null;
        var pool = NewPool(4, TimeSpan.FromMinutes(5), async ct =>
        {
            await gate.Task;
            created = new Conn(1);
            return created;
        });

        var lease = pool.GetConnectionAsync(CancellationToken.None);
        await Task.Delay(100);          // caller is parked inside the factory
        await pool.DisposeAsync();      // pool goes away under it
        gate.SetResult();               // factory now succeeds

        Exception? thrown = null;
        try { var l = await lease; output.WriteLine($"D1: lease returned, conn={l.Connection.Id}"); }
        catch (Exception e) { thrown = e; }

        output.WriteLine($"D1: caller saw {thrown?.GetType().FullName ?? "<no exception>"}: {thrown?.Message}");
        output.WriteLine($"D1: connection created={created is not null}, DisposeCount={created?.DisposeCount.ToString() ?? "n/a"}");

        Assert.NotNull(created);
        Assert.True(
            created!.DisposeCount > 0,
            $"connection {created.Id} was created but never disposed — leaked with its socket open " +
            $"(caller saw {thrown?.GetType().Name ?? "no exception"})");
    }

    // ---------------------------------------------------------------------
    // D2: a caller parked in the at-cap wait loop when the pool is disposed.
    // Does it get a clean error, hang, or spin?
    // ---------------------------------------------------------------------
    [Fact]
    public async Task ParkedCallerDuringDispose_GetsTheTransportsOwnErrorPromptly()
    {
        var pool = NewPool(1, TimeSpan.FromMinutes(5), ct => Task.FromResult(new Conn(1)));
        var held = await pool.GetConnectionAsync(CancellationToken.None);

        var parked = pool.GetConnectionAsync(CancellationToken.None);
        await Task.Delay(100);

        var cpuBefore = Process.GetCurrentProcess().TotalProcessorTime;
        var sw = Stopwatch.StartNew();
        await pool.DisposeAsync();

        var done = await Task.WhenAny(parked, Task.Delay(3000));
        sw.Stop();
        var cpuAfter = Process.GetCurrentProcess().TotalProcessorTime;
        var cpu = cpuAfter - cpuBefore;

        if (done != parked)
        {
            output.WriteLine($"D2: parked caller HUNG for 3s after DisposeAsync. CPU burned {cpu.TotalMilliseconds:0}ms.");
            Assert.Fail("parked caller never completed after the pool was disposed");
        }

        Exception? thrown = null;
        try { await parked; } catch (Exception e) { thrown = e; }
        output.WriteLine($"D2: parked caller completed in {sw.ElapsedMilliseconds}ms with {thrown?.GetType().FullName ?? "<success>"}. CPU burned {cpu.TotalMilliseconds:0}ms.");

        await held.DisposeAsync();

        Assert.False(
            thrown is ObjectDisposedException,
            $"parked caller escaped with a raw ObjectDisposedException instead of a transport error; " +
            $"CPU burned while spinning: {cpu.TotalMilliseconds:0}ms");
    }

    // ---------------------------------------------------------------------
    // D3: capacity must never exceed maxConnections, and the permit count must
    // return to exactly max when every lease is back.
    // ---------------------------------------------------------------------
    [Fact]
    public async Task Churn_NeverExceedsTheCapAndReturnsPermitsToExactlyMax()
    {
        const int max = 4;
        const int workers = 10;
        const int perWorker = 300;

        int live = 0, peak = 0;
        var pool = NewPool(max, TimeSpan.FromMilliseconds(1), ct => Task.FromResult(new Conn(0)));
        var sem = TotalSem(pool);

        var rnd = new Random(1234);
        var tasks = Enumerable.Range(0, workers).Select(w => Task.Run(async () =>
        {
            var r = new Random(w * 7919 + 13);
            for (int i = 0; i < perWorker; i++)
            {
                PooledLease<Conn> lease;
                try { lease = await pool.GetConnectionAsync(CancellationToken.None); }
                catch (OperationCanceledException) { continue; }

                var now = Interlocked.Increment(ref live);
                int seen;
                while (now > (seen = Volatile.Read(ref peak)))
                    Interlocked.CompareExchange(ref peak, now, seen);

                await Task.Yield();
                Interlocked.Decrement(ref live);

                if (r.Next(4) == 0) { lease.Connection.Broken = true; lease.MarkBroken(); }
                await lease.DisposeAsync();
            }
        })).ToArray();

        var all = Task.WhenAll(tasks);
        var done = await Task.WhenAny(all, Task.Delay(TimeSpan.FromSeconds(60)));
        Assert.True(done == all, "workers did not finish in 60s — capacity leak");
        await all;

        // Drain everything back so the permit count is at rest.
        await pool.FlushAsync();
        var count = sem.CurrentCount;
        output.WriteLine($"D3: peak simultaneous leases={peak} (cap {max}); _totalSem.CurrentCount at rest={count} (expected {max})");

        await pool.DisposeAsync();

        Assert.True(peak <= max, $"handed out {peak} simultaneous leases with a cap of {max}");
        Assert.True(count <= max, $"_totalSem holds {count} permits, above the cap of {max} — over-release");
        Assert.True(count == max, $"_totalSem holds {count} permits at rest, expected {max} — permit leak");
    }

    // ---------------------------------------------------------------------
    // P2: pin down WHERE the parked caller hangs. Mirrors lines 103-117 of the
    // pool exactly: two racing waits, dispose the semaphore, then cancel.
    // ---------------------------------------------------------------------
    [Fact]
    public async Task Bcl_CancellingAfterDispose_CannotRescueAPendingWait()
    {
        var sem = new SemaphoreSlim(0, 1);
        using var race = new CancellationTokenSource();
        var slotTask = sem.WaitAsync(race.Token);
        await Task.Delay(50);

        sem.Dispose();

        Exception? cancelFault = null;
        try { race.Cancel(); }
        catch (Exception e) { cancelFault = e; }
        output.WriteLine($"P2: race.Cancel() threw {cancelFault?.GetType().FullName ?? "<nothing>"}");

        var done = await Task.WhenAny(slotTask, Task.Delay(1000));
        output.WriteLine(done == slotTask
            ? $"P2: slotTask completed: {slotTask.Status}"
            : "P2: slotTask STILL PENDING 1s after Dispose+Cancel — `await slotTask` never returns.");

        // And a fresh WaitAsync on the disposed semaphore:
        try { _ = sem.WaitAsync(CancellationToken.None); output.WriteLine("P2: fresh WaitAsync did not throw"); }
        catch (Exception e) { output.WriteLine($"P2: fresh WaitAsync threw {e.GetType().Name}"); }
    }

    // ---------------------------------------------------------------------
    // P3: how much CPU does a parked caller burn after dispose (spin check),
    // and what is the task's state?
    // ---------------------------------------------------------------------
    [Fact]
    public async Task ParkedCaller_WhenThePoolIsDisposed_StopsWaiting()
    {
        var pool = NewPool(1, TimeSpan.FromMinutes(5), ct => Task.FromResult(new Conn(1)));
        var held = await pool.GetConnectionAsync(CancellationToken.None);
        var parked = pool.GetConnectionAsync(CancellationToken.None);
        await Task.Delay(100);

        await pool.DisposeAsync();

        // Await the wakeup with a deadline rather than sampling after a fixed sleep: on a
        // starved CI runner the continuation can take seconds to be scheduled, which is the
        // runner's problem, not the pool's. Only never waking is a pool bug.
        var cpu0 = Process.GetCurrentProcess().TotalProcessorTime;
        var sw = Stopwatch.StartNew();
        var done = await Task.WhenAny(parked, Task.Delay(TimeSpan.FromSeconds(30)));
        var cpu1 = Process.GetCurrentProcess().TotalProcessorTime;

        output.WriteLine(done == parked
            ? $"P3: parked caller completed as {parked.Status} {sw.ElapsedMilliseconds}ms after dispose, process CPU delta={(cpu1 - cpu0).TotalMilliseconds:0}ms"
            : $"P3: parked caller STILL PENDING 30s after dispose, process CPU delta={(cpu1 - cpu0).TotalMilliseconds:0}ms");
        await held.DisposeAsync();
        Assert.True(done == parked, "parked caller is still pending 30s after the pool was disposed");
    }

    // ---------------------------------------------------------------------
    // D4: the double-release interleaving — CreateGatedConnectionAsync
    // reads a STALE entry (releases that entry's permit) and then the factory throws
    // (outer catch releases again). One release or two?
    //
    // Forced deterministically by saturating _connectSem (32 permits) with gated
    // factory calls, so caller X really does queue on the dial gate and really does
    // find an entry parked while it waited.
    // ---------------------------------------------------------------------
    [Fact]
    public async Task StaleReuseThenAFailedDial_BalancesPermitsExactly()
    {
        const int max = 40;
        const int gateWidth = 32;   // MaxConcurrentConnects

        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var throwNext = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        int calls = 0;

        ConnectionPool<Conn>? pool = null;
        pool = NewPool(max, TimeSpan.FromMinutes(5), async ct =>
        {
            int n = Interlocked.Increment(ref calls);
            if (n == 1)
                return new Conn(0);                       // seeds the idle entry
            if (n <= 1 + gateWidth)
            {
                await gate.Task;                          // holds the dial gate shut
                return new Conn(n);
            }
            await throwNext.Task;                         // caller X's own connect
            throw new IOException("connect failed");
        });

        var sem = TotalSem(pool);

        var lease0 = await pool.GetConnectionAsync(CancellationToken.None);
        var blockers = Enumerable.Range(0, gateWidth)
            .Select(_ => pool.GetConnectionAsync(CancellationToken.None))
            .ToArray();
        while (Volatile.Read(ref calls) < 1 + gateWidth) await Task.Delay(10);

        // Caller X: takes a permit, then queues on the exhausted dial gate.
        var x = pool.GetConnectionAsync(CancellationToken.None);
        await Task.Delay(100);
        output.WriteLine($"D4: X queued on dial gate; CurrentCount={sem.CurrentCount} (expected {max - (1 + gateWidth + 1)})");

        // Park a now-stale entry where X will find it when it wakes.
        await lease0.DisposeAsync();
        lease0.Connection.Broken = true;

        // Let one gated connect finish so X gets a dial-gate permit.
        gate.SetResult();
        await Task.WhenAll(blockers);
        throwNext.SetResult();

        Exception? xEx = null;
        try { await x; } catch (Exception e) { xEx = e; }
        output.WriteLine($"D4: X faulted with {xEx?.GetType().Name}: {xEx?.Message}");
        output.WriteLine($"D4: conn0 DisposeCount={lease0.Connection.DisposeCount} (must be 1 — the stale entry X evicted)");

        // Give everything back.
        foreach (var b in blockers)
        {
            var l = await b;
            l.MarkBroken();
            await l.DisposeAsync();
        }
        await pool.FlushAsync();

        int atRest = sem.CurrentCount;
        output.WriteLine($"D4: _totalSem.CurrentCount at rest = {atRest} (expected exactly {max})");
        await pool.DisposeAsync();

        Assert.True(atRest <= max, $"OVER-RELEASE: {atRest} permits vs cap {max}");
        Assert.True(atRest == max, $"permit imbalance: {atRest} vs cap {max}");
    }

    // ---------------------------------------------------------------------
    // D5: the pool's own API surface after DisposeAsync.
    // ---------------------------------------------------------------------
    [Fact]
    public async Task GetConnectionAfterDispose_ThrowsTheTransportsOwnError()
    {
        var pool = NewPool(2, TimeSpan.FromMinutes(5), ct => Task.FromResult(new Conn(1)));
        await pool.DisposeAsync();

        Exception? get = null, flush = null;
        try { await pool.GetConnectionAsync(CancellationToken.None); } catch (Exception e) { get = e; }
        try { await pool.FlushAsync(); } catch (Exception e) { flush = e; }

        output.WriteLine($"D5: GetConnectionAsync after dispose -> {get?.GetType().FullName ?? "<no throw>"}");
        output.WriteLine($"D5: FlushAsync after dispose      -> {flush?.GetType().FullName ?? "<no throw>"}");

        Assert.False(get is ObjectDisposedException,
            "GetConnectionAsync leaks a raw ObjectDisposedException from an internal SemaphoreSlim instead of a transport error");
    }
}
