using Celeriant.Client.Responses;

namespace Celeriant.Client;

/// <summary>
/// Abstracts a per-node connection pool, enabling unit testing of
/// <see cref="CeleriantPool"/> failover and routing logic without real TCP connections.
/// </summary>
internal interface INodeConnectionPool : IAsyncDisposable
{
    /// <summary>The server address this pool connects to.</summary>
    string Address { get; }

    /// <summary>
    /// Lease a connection to this node. Returns an idle connection if available,
    /// or creates a new one if under the limit.
    /// </summary>
    Task<PooledConnection> GetConnectionAsync(CancellationToken ct);

    /// <summary>
    /// Lease a connection, send a request, and return the connection to the pool.
    /// Marks the connection as broken on transport/protocol errors.
    /// Compression is decided per-connection by the client from its negotiated dictionary.
    /// </summary>
    Task<ClientResponse> ExecuteRequestAsync(
        ClientRequest request,
        CancellationToken ct);
}
