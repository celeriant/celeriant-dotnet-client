using Celeriant.Client.Errors;
using Celeriant.Client.Protocol;
using Celeriant.Transport;

namespace Celeriant.Client;

/// <summary>
/// Per-node connection pool for the storage client. The pooling mechanics (idle reuse, circuit
/// breaker, idle eviction) live in the shared <see cref="ConnectionPool{TConn}"/>; this adapter
/// supplies the storage-specific connection factory (connect + Identify + dictionary negotiation),
/// wraps leases as <see cref="PooledConnection"/>, and maps request execution to typed responses.
/// </summary>
internal sealed class NodeConnectionPool : INodeConnectionPool
{
    private readonly ConnectionPool<CeleriantClient> _inner;

    public NodeConnectionPool(string address, CeleriantPoolOptions options, DictCache dictCache)
    {
        _inner = new ConnectionPool<CeleriantClient>(
            address,
            options.MaxConnections,
            options.IdleTimeout,
            ct => CreateConnectionAsync(address, options, dictCache, ct),
            static client => client.IsPoisoned,
            StorageTransportExceptionFactory.Instance,
            static client => client.IsUnfitForReuse,
            // Only a wire failure means the node is down. Anything else from the factory means
            // the node answered (an identity rejection, a server error, a protocol error, an
            // unresolvable dictionary sha) or the failure is local (bad key material). Arming the
            // breaker on those would hide the real cause behind "circuit breaker open" for every
            // request until it closes. Mirrors is_dial_failure in the Rust client's pool.rs.
            static ex => ex is ConnectionFailedException or ConnectionTimeoutException
                             or RequestOutcomeUnknownException);
    }

    public string Address => _inner.Address;

    /// <inheritdoc />
    public bool IsCircuitOpen => _inner.IsCircuitOpen;

    /// <inheritdoc />
    public async Task<PooledConnection> GetConnectionAsync(CancellationToken ct)
        => new PooledConnection(await _inner.GetConnectionAsync(ct).ConfigureAwait(false));

    /// <inheritdoc />
    public async Task<ClientResponse> ExecuteRequestAsync(ClientRequest request, CancellationToken ct)
    {
        var conn = await GetConnectionAsync(ct).ConfigureAwait(false);
        await using (conn)
        {
            try
            {
                return await conn.Client.SendRequestAsync(request, ct).ConfigureAwait(false);
            }
            catch (CeleriantClientException ex) when (LeavesConnectionDirty(ex))
            {
                conn.MarkBroken();
                throw;
            }
        }
    }

    /// <summary>
    /// Whether an error leaves the stream desynchronised, so the connection cannot be reused.
    ///
    /// <para>
    /// A server-decoded error — <c>WriteOccException</c> in an OCC retry loop, <c>NotLeader</c>,
    /// <c>ServerBusy</c>, <c>IdentityRequired</c> — arrives as a fully decoded response frame: the
    /// exchange completed and the stream is clean, so retiring on it would discard and re-dial a
    /// healthy connection per attempt. The classes below are the ones the transport raises when
    /// the exchange did NOT complete — connection, timeout, and protocol (where a correlation
    /// mismatch surfaces) — matching <c>leaves_connection_dirty</c> in the Rust client.
    /// </para>
    /// </summary>
    private static bool LeavesConnectionDirty(CeleriantClientException error)
        => error is ConnectionFailedException or CeleriantTimeoutException or ProtocolException
                 or RequestOutcomeUnknownException;

    public ValueTask DisposeAsync() => _inner.DisposeAsync();

    private static async Task<CeleriantClient> CreateConnectionAsync(
        string address, CeleriantPoolOptions options, DictCache dictCache, CancellationToken ct)
    {
        // Sharing the pool's cache makes every Identify on this connection, the explicit one below
        // or the implicit one ahead of the first request when no identity is configured, advertise
        // what the pool already learned and teach the pool what the server ships.
        var client = await CeleriantClient.ConnectWithDictCacheAsync(
            address, options.ConnectionTimeout, options.TlsConfig, dictCache, ct).ConfigureAwait(false);

        client.WithMaxRequestSize(options.MaxRequestSize)
              .WithMaxResponseSize(options.MaxResponseSize)
              .WithTimeout(options.RequestTimeout);

        if (options.IdentityConfig is { } identityConfig)
        {
            // The socket is open and nothing else holds it. A handshake that throws must close it
            // here or it leaks until a finalizer runs.
            try
            {
                await client.IdentifyAsync(identityConfig, ct).ConfigureAwait(false);
            }
            catch
            {
                await client.DisposeAsync().ConfigureAwait(false);
                throw;
            }
        }

        return client;
    }
}
