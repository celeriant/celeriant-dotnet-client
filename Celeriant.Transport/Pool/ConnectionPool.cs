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
    private readonly ITransportExceptionFactory _ex;
    private readonly Channel<(TConn client, DateTimeOffset lastUsed)> _idle;
    private readonly SemaphoreSlim _totalSem;
    private readonly SemaphoreSlim _connectSem = new(MaxConcurrentConnects, MaxConcurrentConnects);
    private long _lastConnectFailureTicks;
    private bool _disposed;

    public string Address => _address;

    public ConnectionPool(
        string address,
        int maxConnections,
        TimeSpan idleTimeout,
        Func<CancellationToken, Task<TConn>> connectionFactory,
        Func<TConn, bool> isBroken,
        ITransportExceptionFactory exceptionFactory)
    {
        _address = address;
        _idleTimeout = idleTimeout;
        _factory = connectionFactory;
        _isBroken = isBroken;
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

    private bool IsCircuitOpen
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
        if (IsCircuitOpen)
            throw _ex.ConnectionFailed($"Circuit breaker open for {_address}.");

        // Fast path: reuse a fresh idle connection.
        while (_idle.Reader.TryRead(out var entry))
        {
            if (IsStale(entry))
            {
                await DisposeQuietly(entry.client).ConfigureAwait(false);
                _totalSem.Release();
                continue;
            }
            return new PooledLease<TConn>(entry.client, ReturnConnectionAsync);
        }

        // Create a new connection if under the per-node cap.
        if (_totalSem.Wait(0))
            return await CreateGatedConnectionAsync(ct).ConfigureAwait(false);

        // At the cap: wait for one to come back.
        while (true)
        {
            ct.ThrowIfCancellationRequested();
            var entry = await _idle.Reader.ReadAsync(ct).ConfigureAwait(false);
            if (IsStale(entry))
            {
                await DisposeQuietly(entry.client).ConfigureAwait(false);
                _totalSem.Release();
                if (_totalSem.Wait(0))
                    return await CreateGatedConnectionAsync(ct).ConfigureAwait(false);
                continue;
            }
            return new PooledLease<TConn>(entry.client, ReturnConnectionAsync);
        }
    }

    /// <summary>Drain and dispose all idle connections, releasing their semaphore slots.</summary>
    public async Task FlushAsync()
    {
        while (_idle.Reader.TryRead(out var entry))
        {
            await DisposeQuietly(entry.client).ConfigureAwait(false);
            _totalSem.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;
        _disposed = true;
        _idle.Writer.TryComplete();
        while (_idle.Reader.TryRead(out var entry))
            await DisposeQuietly(entry.client).ConfigureAwait(false);
        _connectSem.Dispose();
        _totalSem.Dispose();
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    private bool IsStale((TConn client, DateTimeOffset lastUsed) entry)
        => DateTimeOffset.UtcNow - entry.lastUsed > _idleTimeout || _isBroken(entry.client);

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
                    throw _ex.ConnectionFailed($"Circuit breaker open for {_address}.");

                // Someone ahead of us may have returned a fresh connection while we queued.
                if (_idle.Reader.TryRead(out var reuse))
                {
                    if (!IsStale(reuse))
                        return new PooledLease<TConn>(reuse.client, ReturnConnectionAsync);
                    await DisposeQuietly(reuse.client).ConfigureAwait(false);
                }

                var client = await _factory(ct).ConfigureAwait(false);
                Interlocked.Exchange(ref _lastConnectFailureTicks, 0);
                return new PooledLease<TConn>(client, ReturnConnectionAsync);
            }
            catch
            {
                Interlocked.Exchange(ref _lastConnectFailureTicks, DateTimeOffset.UtcNow.Ticks);
                throw;
            }
            finally
            {
                _connectSem.Release();
            }
        }
        catch
        {
            _totalSem.Release();
            throw;
        }
    }

    private async ValueTask ReturnConnectionAsync(TConn client, bool broken)
    {
        if (broken || _disposed || _isBroken(client))
        {
            await DisposeQuietly(client).ConfigureAwait(false);
            if (!_disposed)
                _totalSem.Release();
            return;
        }

        if (!_idle.Writer.TryWrite((client, DateTimeOffset.UtcNow)))
        {
            await DisposeQuietly(client).ConfigureAwait(false);
            _totalSem.Release();
        }
    }

    private static async ValueTask DisposeQuietly(TConn client)
    {
        try { await client.DisposeAsync().ConfigureAwait(false); }
        catch { /* suppress disposal errors */ }
    }
}
