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
            StorageTransportExceptionFactory.Instance);
    }

    public string Address => _inner.Address;

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
            catch (CeleriantClientException)
            {
                conn.MarkBroken();
                throw;
            }
        }
    }

    public ValueTask DisposeAsync() => _inner.DisposeAsync();

    private static async Task<CeleriantClient> CreateConnectionAsync(
        string address, CeleriantPoolOptions options, DictCache dictCache, CancellationToken ct)
    {
        var client = await CeleriantClient.ConnectAsync(
            address, options.ConnectionTimeout, options.TlsConfig, ct).ConfigureAwait(false);

        client.WithMaxRequestSize(options.MaxRequestSize)
              .WithMaxResponseSize(options.MaxResponseSize)
              .WithTimeout(options.RequestTimeout);

        if (options.IdentityConfig is { } identityConfig)
        {
            // Advertise our known dictionary sha so the server can skip resending its bytes,
            // and resolve a confirmed-but-unsent sha from the shared pool cache.
            await client.IdentifyAsync(identityConfig, dictCache.LastSha, dictCache.DictForSha, ct)
                .ConfigureAwait(false);

            // If this connection received (or confirmed) a dictionary, share it pool-wide.
            if (client.CurrentDict is { } dict)
                dictCache.CacheDict(dict.Sha, dict.Bytes);
        }

        return client;
    }
}
