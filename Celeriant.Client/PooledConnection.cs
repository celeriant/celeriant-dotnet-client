namespace Celeriant.Client;

/// <summary>
/// A borrowed connection from <see cref="CeleriantPool"/>.
///
/// <para>
/// Disposing this object via <see cref="DisposeAsync"/> returns the connection to the pool
/// when healthy, or disposes it when broken (i.e., when the last operation threw a transport
/// or protocol exception).
/// </para>
///
/// <para>
/// This type is not thread-safe; a single <see cref="PooledConnection"/> must not be shared
/// across threads concurrently. Obtain one connection per logical unit of work.
/// </para>
/// </summary>
public sealed class PooledConnection : IAsyncDisposable
{
    private readonly Func<CeleriantClient, bool, ValueTask> _returnToPool;
    private bool _broken;
    private bool _disposed;

    internal PooledConnection(CeleriantClient client, Func<CeleriantClient, bool, ValueTask> returnToPool)
    {
        Client = client;
        _returnToPool = returnToPool;
    }

    /// <summary>The underlying low-level client for this leased connection.</summary>
    public CeleriantClient Client { get; }

    /// <summary>
    /// Mark this connection as broken. When marked broken, <see cref="DisposeAsync"/> will
    /// discard the connection instead of returning it to the pool.
    /// </summary>
    public void MarkBroken() => _broken = true;

    /// <summary>
    /// Return the connection to the pool (if healthy) or dispose it (if broken).
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;
        await _returnToPool(Client, _broken).ConfigureAwait(false);
    }
}
