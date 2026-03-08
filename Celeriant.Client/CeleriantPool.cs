using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using Celeriant.Client.Errors;
using Celeriant.Client.Protocol;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;
using Celeriant.Client.Streaming;
using Celeriant.Client.Watch;

namespace Celeriant.Client;

/// <summary>
/// A topology-aware connection pool over <see cref="CeleriantClient"/>.
///
/// <para>
/// Maintains per-node connection pools and routes operations based on their requirements:
/// <list type="bullet">
///   <item><b>Leader operations</b> (write, delete, trim) are sent to the current leader
///   with automatic failover — if the server responds with <see cref="NotLeaderException"/>,
///   the pool updates its leader address and retries transparently.</item>
///   <item><b>Read operations</b> (read, aggregate details, register schema, list, watch)
///   are distributed across all known nodes via round-robin, offloading the leader.</item>
/// </list>
/// </para>
///
/// <para>
/// New nodes are discovered automatically when the server reports a leader address that
/// was not in the original seed list. Unreachable nodes are skipped and the next available
/// node is tried.
/// </para>
///
/// <para>
/// Thread-safety: this class is safe for concurrent use from multiple threads/async contexts.
/// </para>
/// </summary>
public sealed class CeleriantPool : IAsyncDisposable
{
    private readonly CeleriantPoolOptions _options;
    private readonly ConcurrentDictionary<string, INodeConnectionPool> _nodePools = new();
    private readonly Func<string, CeleriantPoolOptions, INodeConnectionPool> _poolFactory;

    // The address believed to be the current leader. Updated on failover.
    private volatile string _leaderAddress;

    // Round-robin counter for distributing non-leader operations across nodes.
    private int _roundRobinIndex;

    private bool _disposed;

    // -------------------------------------------------------------------------
    // Construction
    // -------------------------------------------------------------------------

    public CeleriantPool(CeleriantPoolOptions options)
        : this(options, static (addr, opts) => new NodeConnectionPool(addr, opts))
    {
    }

    /// <summary>
    /// Internal constructor for unit testing. Accepts a factory that creates
    /// <see cref="INodeConnectionPool"/> instances, allowing mock pools to be injected.
    /// </summary>
    internal CeleriantPool(
        CeleriantPoolOptions options,
        Func<string, CeleriantPoolOptions, INodeConnectionPool> poolFactory)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _poolFactory = poolFactory ?? throw new ArgumentNullException(nameof(poolFactory));
        _leaderAddress = options.Address;

        // Create pools for all seed addresses.
        GetOrCreateNodePool(options.Address);
        if (options.SeedAddresses is not null)
        {
            foreach (var addr in options.SeedAddresses)
                GetOrCreateNodePool(addr);
        }
    }

    // -------------------------------------------------------------------------
    // Typed methods — non-leader operations (any node, round-robin)
    // -------------------------------------------------------------------------

    /// <summary>Send a read request and return the typed response.
    /// Distributed across all known nodes via round-robin.</summary>
    /// <exception cref="CeleriantErrorException">The server returned an application-level error.</exception>
    /// <exception cref="ConnectionFailedException">All known nodes are unreachable.</exception>
    /// <exception cref="CeleriantTimeoutException">The request timed out.</exception>
    /// <exception cref="ProtocolException">The server returned an unexpected response type.</exception>
    /// <exception cref="ObjectDisposedException">The pool has been disposed.</exception>
    public Task<ReadResponse> ReadAsync(
        ReadRequest request,
        CancellationToken ct = default)
        => ExecuteOnAnyNodeAsync(
            new ClientRequest.Read(request),
            static r => r switch
            {
                ClientResponse.Read read => read.Value,
                ClientResponse.GenericError e => throw new CeleriantErrorException(e.Value),
                _ => throw new ProtocolException($"Unexpected response type {r.GetType().Name} for Read."),
            },
            ct);

    /// <summary>Send an aggregate details request and return the typed response.
    /// Distributed across all known nodes via round-robin.</summary>
    /// <exception cref="CeleriantErrorException">The server returned an application-level error.</exception>
    /// <exception cref="ConnectionFailedException">All known nodes are unreachable.</exception>
    /// <exception cref="CeleriantTimeoutException">The request timed out.</exception>
    /// <exception cref="ProtocolException">The server returned an unexpected response type.</exception>
    /// <exception cref="ObjectDisposedException">The pool has been disposed.</exception>
    public Task<AggregateDetailsResponse> AggregateDetailsAsync(
        AggregateDetailsRequest request,
        CancellationToken ct = default)
        => ExecuteOnAnyNodeAsync(
            new ClientRequest.AggregateDetails(request),
            static r => r switch
            {
                ClientResponse.AggregateDetails details => details.Value,
                ClientResponse.GenericError e => throw new CeleriantErrorException(e.Value),
                _ => throw new ProtocolException($"Unexpected response type {r.GetType().Name} for AggregateDetails."),
            },
            ct);

    /// <summary>Send a register-schema request and return the typed response.
    /// Distributed across all known nodes via round-robin.</summary>
    /// <exception cref="CeleriantErrorException">The server returned an application-level error.</exception>
    /// <exception cref="ConnectionFailedException">All known nodes are unreachable.</exception>
    /// <exception cref="CeleriantTimeoutException">The request timed out.</exception>
    /// <exception cref="ProtocolException">The server returned an unexpected response type.</exception>
    /// <exception cref="ObjectDisposedException">The pool has been disposed.</exception>
    public Task<SuccessResponse> RegisterSchemaAsync(
        RegisterSchemaRequest request,
        CancellationToken ct = default)
        => ExecuteOnAnyNodeAsync(
            new ClientRequest.RegisterSchema(request),
            static r => r switch
            {
                ClientResponse.RegisterSchema schema => schema.Value,
                ClientResponse.GenericError e => throw new CeleriantErrorException(e.Value),
                _ => throw new ProtocolException($"Unexpected response type {r.GetType().Name} for RegisterSchema."),
            },
            ct);

    // -------------------------------------------------------------------------
    // Typed methods — leader operations (write/delete/trim with failover)
    // -------------------------------------------------------------------------

    /// <summary>Send a write request and return the typed response.
    /// Routed to the leader with automatic failover on leader change.</summary>
    /// <exception cref="CeleriantErrorException">The server returned an application-level error.</exception>
    /// <exception cref="ConnectionFailedException">No reachable leader could be found.</exception>
    /// <exception cref="CeleriantTimeoutException">The request timed out.</exception>
    /// <exception cref="ProtocolException">The server returned an unexpected response type.</exception>
    /// <exception cref="ObjectDisposedException">The pool has been disposed.</exception>
    public Task<SuccessResponse> WriteAsync(
        WriteRequest request,
        CancellationToken ct = default)
        => ExecuteLeaderOperationAsync(
            new ClientRequest.Write(request),
            static r => r switch
            {
                ClientResponse.Write w => w.Value,
                ClientResponse.GenericError e => throw new CeleriantErrorException(e.Value),
                _ => throw new ProtocolException($"Unexpected response type {r.GetType().Name} for Write."),
            },
            ct);

    /// <summary>Write events to a single aggregate. Creates the aggregate if it does not exist.
    /// Routed to the leader with automatic failover on leader change.</summary>
    /// <exception cref="CeleriantErrorException">The server returned an application-level error.</exception>
    /// <exception cref="ConnectionFailedException">No reachable leader could be found.</exception>
    /// <exception cref="CeleriantTimeoutException">The request timed out.</exception>
    /// <exception cref="ProtocolException">The server returned an unexpected response type.</exception>
    /// <exception cref="ObjectDisposedException">The pool has been disposed.</exception>
    /// <param name="key">The aggregate to write to.</param>
    /// <param name="events">One or more events to append.</param>
    /// <param name="clientId">Client ID for idempotency. Defaults to a new random GUID.</param>
    /// <param name="allowCreate">Whether to create the aggregate if it does not exist. Defaults to <c>true</c>.</param>
    /// <param name="ct">Cancellation token.</param>
    public Task<SuccessResponse> WriteAsync(
        AggregateKey key,
        AggregateEvent[] events,
        Guid? clientId = null,
        bool allowCreate = true,
        CancellationToken ct = default)
        => WriteAsync(new WriteRequest
        {
            ClientId = clientId ?? Guid.NewGuid(),
            Writes = new Dictionary<AggregateKey, SingleAggregateWrite>
            {
                [key] = new SingleAggregateWrite
                {
                    AllowCreate = allowCreate,
                    Events = events,
                }
            }
        }, ct);

    /// <summary>Send a delete request and return the typed response.
    /// Routed to the leader with automatic failover on leader change.</summary>
    /// <exception cref="CeleriantErrorException">The server returned an application-level error.</exception>
    /// <exception cref="ConnectionFailedException">No reachable leader could be found.</exception>
    /// <exception cref="CeleriantTimeoutException">The request timed out.</exception>
    /// <exception cref="ProtocolException">The server returned an unexpected response type.</exception>
    /// <exception cref="ObjectDisposedException">The pool has been disposed.</exception>
    public Task<SuccessResponse> DeleteAsync(
        DeleteRequest request,
        CancellationToken ct = default)
        => ExecuteLeaderOperationAsync(
            new ClientRequest.Delete(request),
            static r => r switch
            {
                ClientResponse.Delete d => d.Value,
                ClientResponse.GenericError e => throw new CeleriantErrorException(e.Value),
                _ => throw new ProtocolException($"Unexpected response type {r.GetType().Name} for Delete."),
            },
            ct);

    /// <summary>Send a trim-start request and return the typed response.
    /// Routed to the leader with automatic failover on leader change.</summary>
    /// <exception cref="CeleriantErrorException">The server returned an application-level error.</exception>
    /// <exception cref="ConnectionFailedException">No reachable leader could be found.</exception>
    /// <exception cref="CeleriantTimeoutException">The request timed out.</exception>
    /// <exception cref="ProtocolException">The server returned an unexpected response type.</exception>
    /// <exception cref="ObjectDisposedException">The pool has been disposed.</exception>
    public Task<SuccessResponse> TrimStartAsync(
        TrimStartRequest request,
        CancellationToken ct = default)
        => ExecuteLeaderOperationAsync(
            new ClientRequest.TrimStart(request),
            static r => r switch
            {
                ClientResponse.TrimStart t => t.Value,
                ClientResponse.GenericError e => throw new CeleriantErrorException(e.Value),
                _ => throw new ProtocolException($"Unexpected response type {r.GetType().Name} for TrimStart."),
            },
            ct);

    // -------------------------------------------------------------------------
    // Streaming read (any node)
    // -------------------------------------------------------------------------

    /// <summary>
    /// Stream all event batches for an aggregate, automatically following pagination cursors.
    /// Leases a single connection for the duration of the enumeration.
    /// </summary>
    /// <exception cref="CeleriantErrorException">The server returned an application-level error.</exception>
    /// <exception cref="ConnectionFailedException">No reachable node could be found.</exception>
    /// <exception cref="CeleriantTimeoutException">The request timed out.</exception>
    /// <exception cref="ObjectDisposedException">The pool has been disposed.</exception>
    public IAsyncEnumerable<AggregateEventBatch> ReadAllAsync(
        AggregateKey key,
        ReadFilters? filters = null,
        CancellationToken ct = default)
        => ReadAllAsyncCore(key, filters, ct);

    private async IAsyncEnumerable<AggregateEventBatch> ReadAllAsyncCore(
        AggregateKey key,
        ReadFilters? filters,
        [EnumeratorCancellation] CancellationToken ct)
    {
        await using var conn = await GetAnyConnectionAsync(ct).ConfigureAwait(false);
        await foreach (var batch in conn.Client.ReadAllAsync(key, filters, ct).ConfigureAwait(false))
            yield return batch;
    }

    // -------------------------------------------------------------------------
    // List operations (any node)
    // -------------------------------------------------------------------------

    /// <summary>
    /// Stream all organisations, leasing a single connection for the duration of the enumeration.
    /// </summary>
    /// <exception cref="ConnectionFailedException">No reachable node could be found.</exception>
    /// <exception cref="CeleriantTimeoutException">The request timed out.</exception>
    /// <exception cref="ObjectDisposedException">The pool has been disposed.</exception>
    public IAsyncEnumerable<OrgListItem> ListOrgsAsync(
        ListOptions? options = null,
        CancellationToken ct = default)
        => ListOrgsAsyncCore(options, ct);

    private async IAsyncEnumerable<OrgListItem> ListOrgsAsyncCore(
        ListOptions? options,
        [EnumeratorCancellation] CancellationToken ct)
    {
        await using var conn = await GetAnyConnectionAsync(ct).ConfigureAwait(false);
        await foreach (var item in conn.Client.ListOrgsAsync(options, ct).ConfigureAwait(false))
            yield return item;
    }

    /// <summary>
    /// Stream all aggregate types, leasing a single connection for the duration of the enumeration.
    /// </summary>
    /// <exception cref="ConnectionFailedException">No reachable node could be found.</exception>
    /// <exception cref="CeleriantTimeoutException">The request timed out.</exception>
    /// <exception cref="ObjectDisposedException">The pool has been disposed.</exception>
    public IAsyncEnumerable<AggregateTypeListItem> ListAggregateTypesAsync(
        Guid? orgId = null,
        ListOptions? options = null,
        CancellationToken ct = default)
        => ListAggregateTypesAsyncCore(orgId, options, ct);

    private async IAsyncEnumerable<AggregateTypeListItem> ListAggregateTypesAsyncCore(
        Guid? orgId,
        ListOptions? options,
        [EnumeratorCancellation] CancellationToken ct)
    {
        await using var conn = await GetAnyConnectionAsync(ct).ConfigureAwait(false);
        await foreach (var item in conn.Client.ListAggregateTypesAsync(orgId, options, ct).ConfigureAwait(false))
            yield return item;
    }

    /// <summary>
    /// Stream aggregates with merged statistics, leasing a single connection for the duration of the enumeration.
    /// </summary>
    /// <exception cref="ConnectionFailedException">No reachable node could be found.</exception>
    /// <exception cref="CeleriantTimeoutException">The request timed out.</exception>
    /// <exception cref="ObjectDisposedException">The pool has been disposed.</exception>
    public IAsyncEnumerable<AggregateStats> ListAggregatesAsync(
        Guid? orgId = null,
        Guid? aggregateTypeId = null,
        ListOptions? options = null,
        CancellationToken ct = default)
        => ListAggregatesAsyncCore(orgId, aggregateTypeId, options, ct);

    private async IAsyncEnumerable<AggregateStats> ListAggregatesAsyncCore(
        Guid? orgId,
        Guid? aggregateTypeId,
        ListOptions? options,
        [EnumeratorCancellation] CancellationToken ct)
    {
        await using var conn = await GetAnyConnectionAsync(ct).ConfigureAwait(false);
        await foreach (var item in conn.Client.ListAggregatesAsync(orgId, aggregateTypeId, options, ct).ConfigureAwait(false))
            yield return item;
    }

    // -------------------------------------------------------------------------
    // Watch (dedicated connection, not pooled — any node)
    // -------------------------------------------------------------------------

    /// <summary>
    /// Open a dedicated watch connection. This connection is NOT pooled; the caller owns its
    /// lifetime and must dispose it when done. The connection is established to any available node.
    /// </summary>
    /// <exception cref="ConnectionFailedException">No reachable node could be found.</exception>
    /// <exception cref="CeleriantTimeoutException">The connection or handshake timed out.</exception>
    /// <exception cref="ObjectDisposedException">The pool has been disposed.</exception>
    public async Task<WatchConnection> WatchAsync(
        WatchRequest request,
        WatchOptions? options = null,
        CancellationToken ct = default)
    {
        var watchOptions = options ?? new WatchOptions
        {
            TlsConfig = _options.TlsConfig,
        };

        // Try each read-eligible node until one connects successfully.
        var nodeAddresses = GetReadNodeAddresses();
        int startIdx = (int)((uint)Interlocked.Increment(ref _roundRobinIndex) % (uint)nodeAddresses.Length);

        for (int i = 0; i < nodeAddresses.Length; i++)
        {
            var addr = nodeAddresses[(startIdx + i) % nodeAddresses.Length];
            try
            {
                return await WatchConnection.ConnectAsync(addr, request, watchOptions, ct)
                    .ConfigureAwait(false);
            }
            catch (ConnectionFailedException) when (i < nodeAddresses.Length - 1)
            {
                // Try next node.
            }
        }

        // Single node or all failed — let the exception propagate naturally.
        return await WatchConnection.ConnectAsync(nodeAddresses[startIdx % nodeAddresses.Length], request, watchOptions, ct)
            .ConfigureAwait(false);
    }

    // -------------------------------------------------------------------------
    // Low-level: lease a raw connection (any node)
    // -------------------------------------------------------------------------

    /// <summary>
    /// Borrow a connection from the pool for manual low-level use.
    /// The connection may be to any available node (round-robin).
    /// Dispose the returned <see cref="PooledConnection"/> to return it to the pool.
    /// </summary>
    public Task<PooledConnection> GetConnectionAsync(CancellationToken ct = default)
    {
        ThrowIfDisposed();
        return GetAnyConnectionAsync(ct);
    }

    // -------------------------------------------------------------------------
    // IAsyncDisposable
    // -------------------------------------------------------------------------

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;

        foreach (var pool in _nodePools.Values)
        {
            try
            {
                await pool.DisposeAsync().ConfigureAwait(false);
            }
            catch
            {
                // Suppress disposal errors.
            }
        }
    }

    // -------------------------------------------------------------------------
    // Connection routing
    // -------------------------------------------------------------------------

    /// <summary>
    /// Get a connection to a node eligible for read operations, cycling via round-robin.
    /// Respects <see cref="CeleriantPoolOptions.RouteReadsToFollowers"/>.
    /// On <see cref="ConnectionFailedException"/>, tries the next node.
    /// </summary>
    private async Task<PooledConnection> GetAnyConnectionAsync(CancellationToken ct)
    {
        ThrowIfDisposed();

        var nodeAddresses = GetReadNodeAddresses();
        int startIdx = (int)((uint)Interlocked.Increment(ref _roundRobinIndex) % (uint)nodeAddresses.Length);

        for (int i = 0; i < nodeAddresses.Length; i++)
        {
            var addr = nodeAddresses[(startIdx + i) % nodeAddresses.Length];
            try
            {
                return await _nodePools[addr].GetConnectionAsync(ct).ConfigureAwait(false);
            }
            catch (ConnectionFailedException) when (i < nodeAddresses.Length - 1)
            {
                // Node unreachable, try next.
            }
        }

        // All skipped — fall through to throw from the last node.
        throw new ConnectionFailedException("All known nodes are unreachable.");
    }

    /// <summary>
    /// Get a connection to the current leader node.
    /// </summary>
    private Task<PooledConnection> GetLeaderConnectionAsync(CancellationToken ct)
        => GetOrCreateNodePool(_leaderAddress).GetConnectionAsync(ct);

    private INodeConnectionPool GetOrCreateNodePool(string address)
        => _nodePools.GetOrAdd(address, addr => _poolFactory(addr, _options));

    /// <summary>
    /// Returns the node addresses eligible for read operations.
    /// When <see cref="CeleriantPoolOptions.RouteReadsToFollowers"/> is set, excludes the
    /// leader — unless no followers are available (single-node setup).
    /// </summary>
    private string[] GetReadNodeAddresses()
    {
        var all = _nodePools.Keys.ToArray();
        if (!_options.RouteReadsToFollowers || all.Length <= 1)
            return all;

        var leader = _leaderAddress;
        var followers = all.Where(a => a != leader).ToArray();
        return followers.Length > 0 ? followers : all;
    }

    // -------------------------------------------------------------------------
    // Execution helpers
    // -------------------------------------------------------------------------

    /// <summary>
    /// Execute a non-leader operation on a read-eligible node with failover.
    /// Respects <see cref="CeleriantPoolOptions.RouteReadsToFollowers"/>.
    /// On <see cref="ConnectionFailedException"/>, tries the next node in round-robin order.
    /// </summary>
    private async Task<T> ExecuteOnAnyNodeAsync<T>(
        ClientRequest request,
        Func<ClientResponse, T> mapResponse,
        CancellationToken ct)
    {
        var nodeAddresses = GetReadNodeAddresses();
        int startIdx = (int)((uint)Interlocked.Increment(ref _roundRobinIndex) % (uint)nodeAddresses.Length);

        for (int i = 0; i < nodeAddresses.Length; i++)
        {
            var addr = nodeAddresses[(startIdx + i) % nodeAddresses.Length];

            try
            {
                var response = await _nodePools[addr].ExecuteRequestAsync(
                    request, _options.CompressionAlgorithm, _options.AutoCompressionThresholdBytes, ct)
                    .ConfigureAwait(false);
                return mapResponse(response);
            }
            catch (ConnectionFailedException) when (i < nodeAddresses.Length - 1)
            {
                continue;
            }
        }

        throw new ConnectionFailedException("All known nodes are unreachable.");
    }

    /// <summary>
    /// Execute a leader-bound operation (write, delete, trim) with automatic failover.
    ///
    /// <para>Handles two failure modes:</para>
    /// <list type="bullet">
    ///   <item><see cref="NotLeaderException"/> — the server reports a leader change.
    ///   The pool updates its leader address (and discovers new nodes) and retries.</item>
    ///   <item><see cref="ConnectionFailedException"/> — the leader is unreachable.
    ///   The pool tries each known node until one accepts the write or redirects to the leader.</item>
    /// </list>
    /// </summary>
    private async Task<T> ExecuteLeaderOperationAsync<T>(
        ClientRequest request,
        Func<ClientResponse, T> mapResponse,
        CancellationToken ct)
    {
        string currentTarget = _leaderAddress;
        var triedNodes = new HashSet<string>();

        // Worst case: try every known node + 1 for a newly discovered leader.
        int maxAttempts = _nodePools.Count + 1;

        for (int attempt = 0; attempt < maxAttempts; attempt++)
        {
            triedNodes.Add(currentTarget);
            var pool = GetOrCreateNodePool(currentTarget);

            try
            {
                var response = await pool.ExecuteRequestAsync(
                    request, _options.CompressionAlgorithm, _options.AutoCompressionThresholdBytes, ct)
                    .ConfigureAwait(false);

                // Write succeeded — this node is the leader.
                _leaderAddress = currentTarget;
                return mapResponse(response);
            }
            catch (NotLeaderException ex) when (ex.LeaderAddress is not null)
            {
                _leaderAddress = ex.LeaderAddress;
                GetOrCreateNodePool(ex.LeaderAddress);
                currentTarget = ex.LeaderAddress;
                // Recalculate max attempts since we may have discovered a new node.
                maxAttempts = _nodePools.Count + 1;
            }
            catch (NotLeaderException)
            {
                // No leader address provided. Try next known node.
                var next = GetNextUntried(triedNodes);
                if (next is null) throw;
                currentTarget = next;
            }
            catch (ConnectionFailedException)
            {
                // Connection dropped or unreachable. Try next known node.
                var next = GetNextUntried(triedNodes);
                if (next is null) throw;
                currentTarget = next;
            }
        }

        throw new ConnectionFailedException(
            $"Failed to find a reachable leader after trying {triedNodes.Count} node(s).");
    }

    /// <summary>
    /// Find the next known node address that hasn't been tried yet.
    /// </summary>
    private string? GetNextUntried(HashSet<string> tried)
    {
        foreach (var addr in _nodePools.Keys)
        {
            if (!tried.Contains(addr))
                return addr;
        }
        return null;
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(CeleriantPool));
    }
}
