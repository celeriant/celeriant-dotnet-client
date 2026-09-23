using System.Threading.Channels;

namespace Celeriant.Transport;

/// <summary>
/// A pool of connections to ONE node. Hands out up to <c>maxConnections</c> live connections,
/// reusing idle ones and creating new ones under the cap, so concurrent work to a node runs in
/// parallel instead of serializing on a single connection. Idle connections are evicted lazily
/// (on checkout, no background timer); a failed connect trips a short circuit breaker so a swarm
/// of callers does not each independently time out on a dead node.
///
/// <para>
/// Connection creation (connect + Identify + dictionary negotiation) is supplied by the product as
/// <c>connectionFactory</c>; brokenness is reported by <c>isBroken</c> (a poisoned connection).
/// </para>
/// </summary>
public sealed class ConnectionPool<TConn> : IAsyncDisposable where TConn : IAsyncDisposable
{
    /// <summary>Fast-fail window after a failed connect, so queued callers don't each time out on a dead node.</summary>
    private static readonly TimeSpan CircuitBreakerCooldown = TimeSpan.FromSeconds(2);

    /// <summary>Cap on concurrent TCP dials to this node (limits waste when it's down).</summary>
    private const int MaxConcurrentConnects = 32;

    private readonly string _address;
    private readonly TimeSpan _idleTimeout;
    private readonly Func<CancellationToken, Task<TConn>> _factory;
    private readonly Func<TConn, bool> _isBroken;
    private readonly Func<TConn, bool>? _isUnfitForReuse;
    private readonly Func<Exception, bool>? _isDialFailure;
    private readonly ITransportExceptionFactory _ex;
    private readonly Channel<(TConn client, DateTimeOffset lastUsed)> _idle;
    private readonly SemaphoreSlim _totalSem;
    private readonly SemaphoreSlim _connectSem = new(MaxConcurrentConnects, MaxConcurrentConnects);
    private long _lastConnectFailureTicks;
    private volatile bool _disposed;

    /// <summary>
    /// Cancelled at the start of disposal to wake every parked caller.
    ///
    /// <para>
    /// A caller at the cap parks on <c>_totalSem.WaitAsync</c> until a permit or an idle
    /// connection comes back, and neither ever comes back from a disposed pool. This token is the
    /// only exit. The semaphores themselves are never disposed — see <see cref="DisposeAsync"/>
    /// for why disposing them would race this cancellation and strand the caller anyway.
    /// </para>
    /// </summary>
    private readonly CancellationTokenSource _disposing = new();

    public string Address => _address;

    public ConnectionPool(
        string address,
        int maxConnections,
        TimeSpan idleTimeout,
        Func<CancellationToken, Task<TConn>> connectionFactory,
        Func<TConn, bool> isBroken,
        ITransportExceptionFactory exceptionFactory,
        Func<TConn, bool>? isUnfitForReuse = null,
        Func<Exception, bool>? isDialFailure = null)
    {
        _address = address;
        _idleTimeout = idleTimeout;
        _factory = connectionFactory;
        _isBroken = isBroken;
        _isUnfitForReuse = isUnfitForReuse;
        _isDialFailure = isDialFailure;
        _ex = exceptionFactory;

        _idle = Channel.CreateBounded<(TConn, DateTimeOffset)>(
            new BoundedChannelOptions(maxConnections)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleWriter = false,
                SingleReader = false,
            });
        _totalSem = new SemaphoreSlim(maxConnections, maxConnections);
    }

    /// <summary>
    /// Whether this node is inside its post-failure fast-fail window. Public so leader routing can
    /// ask before following a redirect into a node it would only fast-fail on, without dialling it.
    /// </summary>
    public bool IsCircuitOpen
    {
        get
        {
            var failedAt = Interlocked.Read(ref _lastConnectFailureTicks);
            return failedAt > 0 && new TimeSpan(DateTimeOffset.UtcNow.Ticks - failedAt) < CircuitBreakerCooldown;
        }
    }

    /// <summary>Lease a connection (reuse an idle one, or create one under the cap, else wait for a return).</summary>
    public async Task<PooledLease<TConn>> GetConnectionAsync(CancellationToken ct)
    {
        ThrowIfDisposed();

        if (IsCircuitOpen)
            throw CircuitOpenError();

        // Fast path: reuse a fresh idle connection.
        while (_idle.Reader.TryRead(out var entry))
        {
            if (IsStale(entry))
            {
                await DisposeQuietly(entry.client).ConfigureAwait(false);
                ReleasePermit();
                continue;
            }
            return new PooledLease<TConn>(entry.client, ReturnConnectionAsync);
        }

        // Create a new connection if under the per-node cap.
        if (TryTakePermit())
            return await CreateGatedConnectionAsync(ct).ConfigureAwait(false);

        // At the cap: wait either for a connection to come back or for a slot to
        // free up. Both are needed. A lease that comes back broken is disposed and
        // releases the semaphore WITHOUT writing to `_idle`, so a waiter that only
        // reads the channel parks for ever — and every caller-cancelled request now
        // takes that path.
        while (true)
        {
            ct.ThrowIfCancellationRequested();

            // Linked to `_disposing` as well as the caller's token, so a pool disposed while this
            // caller is parked ends the wait instead of stranding it on a semaphore nobody will
            // ever post to again.
            using var race = CancellationTokenSource.CreateLinkedTokenSource(ct, _disposing.Token);
            var idleTask = _idle.Reader.ReadAsync(race.Token).AsTask();
            var slotTask = _totalSem.WaitAsync(race.Token);

            await Task.WhenAny(idleTask, slotTask).ConfigureAwait(false);
            race.Cancel();

            // Reconcile both, never just the winner: cancelling a `SemaphoreSlim`
            // wait that has already taken its permit does not give it back, and a
            // cancelled channel read can still have consumed an entry. Dropping
            // either on the floor leaks it.
            bool gotSlot = false;
            try { await slotTask.ConfigureAwait(false); gotSlot = true; }
            catch (OperationCanceledException) { }

            (TConn client, DateTimeOffset lastUsed)? idle = null;
            try { idle = await idleTask.ConfigureAwait(false); }
            catch (OperationCanceledException) { }
            catch (ChannelClosedException) { }

            // Woken by disposal rather than by a returning connection. Whatever the channel handed
            // over belongs to nobody now, and the caller gets the pool's own error rather than a
            // SemaphoreSlim one.
            if (_disposed)
            {
                if (idle is { } orphan)
                    await DisposeQuietly(orphan.client).ConfigureAwait(false);
                throw DisposedError();
            }

            if (idle is { } entry)
            {
                if (gotSlot)
                    ReleasePermit();

                if (IsStale(entry))
                {
                    await DisposeQuietly(entry.client).ConfigureAwait(false);
                    ReleasePermit();
                    if (TryTakePermit())
                        return await CreateGatedConnectionAsync(ct).ConfigureAwait(false);
                    continue;
                }
                return new PooledLease<TConn>(entry.client, ReturnConnectionAsync);
            }

            if (gotSlot)
                return await CreateGatedConnectionAsync(ct).ConfigureAwait(false);
        }
    }

    /// <summary>Drain and dispose all idle connections, releasing their semaphore slots.</summary>
    public async Task FlushAsync()
    {
        while (_idle.Reader.TryRead(out var entry))
        {
            await DisposeQuietly(entry.client).ConfigureAwait(false);
            ReleasePermit();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;
        _disposed = true;

        // Wake every parked caller. No Release() is coming once the pool is dead, so a caller
        // parked at the cap leaves through this cancellation or not at all.
        _disposing.Cancel();

        _idle.Writer.TryComplete();
        while (_idle.Reader.TryRead(out var entry))
            await DisposeQuietly(entry.client).ConfigureAwait(false);

        // The semaphores are deliberately NOT disposed. SemaphoreSlim completes a cancelled
        // `WaitAsync` *asynchronously* — the cancellation callback only queues the completion —
        // so a Dispose() here races the wakeup the Cancel() above just started: Dispose() drops
        // the semaphore's internal waiter list without completing the waits, and a waiter whose
        // queued cancellation then finds itself already delisted falls back to awaiting a task
        // nothing will ever complete. Leaving the semaphores alive costs nothing (SemaphoreSlim
        // owns an OS handle only once AvailableWaitHandle has been touched, which this pool never
        // does) and lets every in-flight cancellation land on a live semaphore.
        //
        // `_disposing` is deliberately NOT disposed either. Callers still in flight read its
        // Token to build their race source, and a disposed CancellationTokenSource throws on that
        // read. A cancelled source holds nothing worth reclaiming; the registrations against it
        // are owned by each caller's `using var race`.
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    /// <summary>
    /// Called only on entries coming out of <c>_idle</c>, never on a connection just created, so
    /// the socket peek in <c>_isUnfitForReuse</c> is paid once per reuse. Without the peek a peer
    /// that closed while the connection sat idle is discovered only after the request is in the
    /// send buffer, where the caller can no longer be told the node never saw it.
    /// </summary>
    private bool IsStale((TConn client, DateTimeOffset lastUsed) entry)
        => DateTimeOffset.UtcNow - entry.lastUsed > _idleTimeout
            || _isBroken(entry.client)
            || _isUnfitForReuse?.Invoke(entry.client) == true;

    /// <summary>Create a connection through the dial gate + circuit breaker. Caller holds a <c>_totalSem</c> permit.</summary>
    private async Task<PooledLease<TConn>> CreateGatedConnectionAsync(CancellationToken ct)
    {
        try
        {
            await _connectSem.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                // Re-check after waiting: breaker may have tripped while queued.
                if (IsCircuitOpen)
                    throw CircuitOpenError();

                // Someone ahead of us may have returned a fresh connection while we queued.
                //
                // The permit count tracks connections that exist or are being created, and an idle
                // entry is already counted. Reading one here therefore leaves the count one too
                // high for the work in hand, so one permit goes back either way: on reuse the
                // caller's own is surplus, and on eviction the disposed connection's is. Holding
                // both would leak a permit per hit — and the at-cap wakeup loop routes every retry
                // through here, so capacity would walk to zero under churn.
                if (_idle.Reader.TryRead(out var reuse))
                {
                    if (!IsStale(reuse))
                    {
                        ReleasePermit();
                        return new PooledLease<TConn>(reuse.client, ReturnConnectionAsync);
                    }

                    await DisposeQuietly(reuse.client).ConfigureAwait(false);
                    ReleasePermit();
                }

                TConn client;
                try
                {
                    client = await _factory(ct).ConfigureAwait(false);
                }
                // Only a dial failure arms the breaker. A caller's own cancellation says nothing
                // about the node. A failure the node answered with is excluded through
                // isDialFailure, since only the product knows which of its exceptions mean the
                // handshake got a reply.
                catch (OperationCanceledException)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    if (_isDialFailure?.Invoke(ex) ?? true)
                        Interlocked.Exchange(ref _lastConnectFailureTicks, DateTimeOffset.UtcNow.Ticks);
                    throw;
                }

                // Dialled into a pool that has gone away. Handing this out would leak the socket
                // it just opened: nothing will ever return the lease, and the pool that would
                // have closed it on return no longer exists.
                if (_disposed)
                {
                    await DisposeQuietly(client).ConfigureAwait(false);
                    throw DisposedError();
                }

                Interlocked.Exchange(ref _lastConnectFailureTicks, 0);
                return new PooledLease<TConn>(client, ReturnConnectionAsync);
            }
            finally
            {
                _connectSem.Release();
            }
        }
        catch
        {
            ReleasePermit();
            throw;
        }
    }

    private async ValueTask ReturnConnectionAsync(TConn client, bool broken)
    {
        if (broken || _disposed || _isBroken(client))
        {
            await DisposeQuietly(client).ConfigureAwait(false);
            ReleasePermit();
            return;
        }

        if (!_idle.Writer.TryWrite((client, DateTimeOffset.UtcNow)))
        {
            await DisposeQuietly(client).ConfigureAwait(false);
            ReleasePermit();
        }
    }

    /// <summary>
    /// Take a permit if one is free right now. A disposed pool has none — reported as "no permit"
    /// rather than as an exception, so the caller reaches the wait loop and leaves through the one
    /// place that speaks the transport's own vocabulary.
    /// </summary>
    private bool TryTakePermit()
    {
        if (_disposed)
            return false;

        return _totalSem.Wait(0);
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
            throw DisposedError();
    }

    /// <summary>
    /// The pool refused locally, before any node was contacted. Not a connection failure: it is no
    /// evidence about the node, so routing must not retire a cached leader on it.
    /// </summary>
    private Exception DisposedError() => _ex.PoolUnavailable(_address, "the pool has been disposed");

    private Exception CircuitOpenError() => _ex.PoolUnavailable(_address, "its circuit breaker is open");

    /// <summary>
    /// Give a permit back, unless the pool is going away. Disposal races every caller still in
    /// flight, and once it has begun there is no capacity left to account for.
    /// </summary>
    private void ReleasePermit()
    {
        if (_disposed)
            return;

        _totalSem.Release();
    }

    private static async ValueTask DisposeQuietly(TConn client)
    {
        try { await client.DisposeAsync().ConfigureAwait(false); }
        catch { /* suppress disposal errors */ }
    }
}
