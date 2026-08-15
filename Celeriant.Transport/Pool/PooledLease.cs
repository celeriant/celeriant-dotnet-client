namespace Celeriant.Transport;

/// <summary>
/// A connection borrowed from a <see cref="ConnectionPool{TConn}"/>. Disposing it returns the
/// connection to the pool when healthy, or discards it when marked broken (a transport/timeout error
/// left the stream framing indeterminate). Not thread-safe — one lease per logical unit of work.
/// </summary>
public sealed class PooledLease<TConn> : IAsyncDisposable where TConn : IAsyncDisposable
{
    private readonly Func<TConn, bool, ValueTask> _returnToPool;
    private bool _broken;
    private bool _disposed;

    internal PooledLease(TConn connection, Func<TConn, bool, ValueTask> returnToPool)
    {
        Connection = connection;
        _returnToPool = returnToPool;
    }

    /// <summary>The borrowed connection.</summary>
    public TConn Connection { get; }

    /// <summary>Mark this connection broken; <see cref="DisposeAsync"/> then discards it instead of pooling it.</summary>
    public void MarkBroken() => _broken = true;

    /// <summary>Return the connection to the pool (if healthy) or dispose it (if broken).</summary>
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;
        _disposed = true;
        await _returnToPool(Connection, _broken).ConfigureAwait(false);
    }
}
