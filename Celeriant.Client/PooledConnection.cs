using Celeriant.Transport;

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
    private readonly PooledLease<CeleriantClient> _lease;

    internal PooledConnection(PooledLease<CeleriantClient> lease) => _lease = lease;

    /// <summary>The underlying low-level client for this leased connection.</summary>
    public CeleriantClient Client => _lease.Connection;

    /// <summary>
    /// Mark this connection as broken. When marked broken, <see cref="DisposeAsync"/> will
    /// discard the connection instead of returning it to the pool.
    /// </summary>
    public void MarkBroken() => _lease.MarkBroken();

    /// <summary>
    /// Return the connection to the pool (if healthy) or dispose it (if broken).
    /// </summary>
    public ValueTask DisposeAsync() => _lease.DisposeAsync();
}
