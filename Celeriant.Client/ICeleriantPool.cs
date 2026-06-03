using Celeriant.Client.Requests;
using Celeriant.Client.Responses;
using Celeriant.Client.Streaming;
using Celeriant.Client.Watch;

namespace Celeriant.Client;

/// <summary>
/// Abstraction over <see cref="CeleriantPool"/> for dependency injection and testing.
/// </summary>
public interface ICeleriantPool : IAsyncDisposable
{
    /// <summary>Send a read request. Distributed across all known nodes via round-robin.</summary>
    /// <exception cref="Errors.AggregateNotFoundException">The aggregate does not exist.</exception>
    /// <exception cref="Errors.BatchIndexUnavailableException">The requested batch index has been trimmed. Re-read from <see cref="Errors.BatchIndexUnavailableException.MinimumAvailableVersion"/>.</exception>
    /// <exception cref="Errors.ConnectionFailedException">All known nodes are unreachable.</exception>
    /// <exception cref="Errors.CeleriantTimeoutException">The request timed out.</exception>
    /// <exception cref="ObjectDisposedException">The pool has been disposed.</exception>
    Task<ReadResponse> ReadAsync(ReadRequest request, CancellationToken ct = default);

    /// <summary>Send an aggregate details request. Distributed across all known nodes via round-robin.</summary>
    /// <exception cref="Errors.AggregateNotFoundException">The aggregate does not exist.</exception>
    /// <exception cref="Errors.ConnectionFailedException">All known nodes are unreachable.</exception>
    /// <exception cref="Errors.CeleriantTimeoutException">The request timed out.</exception>
    /// <exception cref="ObjectDisposedException">The pool has been disposed.</exception>
    Task<AggregateDetailsResponse> AggregateDetailsAsync(AggregateDetailsRequest request, CancellationToken ct = default);

    /// <summary>Send a register-schema request. Routed to the leader with automatic failover.</summary>
    /// <exception cref="Errors.SchemaErrorException">The schema is invalid, unsupported, or already registered.</exception>
    /// <exception cref="Errors.ConnectionFailedException">No reachable leader could be found.</exception>
    /// <exception cref="Errors.CeleriantTimeoutException">The request timed out.</exception>
    /// <exception cref="ObjectDisposedException">The pool has been disposed.</exception>
    Task<SuccessResponse> RegisterSchemaAsync(RegisterSchemaRequest request, CancellationToken ct = default);

    /// <summary>Send a write request. Routed to the leader with automatic failover.</summary>
    /// <exception cref="Errors.WriteOccException">Optimistic concurrency violation — the aggregate has been modified. Re-read and retry.</exception>
    /// <exception cref="Errors.IdempotencyViolationException">Duplicate write — the event was already accepted. No action needed.</exception>
    /// <exception cref="Errors.AggregateNotFoundException">The aggregate does not exist and <c>AllowCreate</c> is false.</exception>
    /// <exception cref="Errors.AggregateRecreateNotAllowedException">The aggregate was permanently deleted.</exception>
    /// <exception cref="Errors.SchemaValidationException">An event payload does not conform to the registered schema.</exception>
    /// <exception cref="Errors.ShardRoutingException">A multi-aggregate write targets aggregates on different shards.</exception>
    /// <exception cref="Errors.ConnectionFailedException">No reachable leader could be found.</exception>
    /// <exception cref="Errors.CeleriantTimeoutException">The request timed out.</exception>
    /// <exception cref="ObjectDisposedException">The pool has been disposed.</exception>
    Task<SuccessResponse> WriteAsync(WriteRequest request, CancellationToken ct = default);

    /// <summary>Write events to a single aggregate. Routed to the leader with automatic failover.</summary>
    /// <exception cref="Errors.WriteOccException">Optimistic concurrency violation — the aggregate has been modified. Re-read and retry.</exception>
    /// <exception cref="Errors.IdempotencyViolationException">Duplicate write — the event was already accepted. No action needed.</exception>
    /// <exception cref="Errors.AggregateNotFoundException">The aggregate does not exist and <paramref name="allowCreate"/> is false.</exception>
    /// <exception cref="Errors.AggregateRecreateNotAllowedException">The aggregate was permanently deleted.</exception>
    /// <exception cref="Errors.SchemaValidationException">An event payload does not conform to the registered schema.</exception>
    /// <exception cref="Errors.ConnectionFailedException">No reachable leader could be found.</exception>
    /// <exception cref="Errors.CeleriantTimeoutException">The request timed out.</exception>
    /// <exception cref="ObjectDisposedException">The pool has been disposed.</exception>
    Task<SuccessResponse> WriteAsync(
        AggregateKey key,
        AggregateEvent[] events,
        Guid? clientId = null,
        bool allowCreate = true,
        long? expectedVersion = null,
        bool enforceClientIdempotency = false,
        CancellationToken ct = default);

    /// <summary>Send a delete request. Routed to the leader with automatic failover.</summary>
    /// <exception cref="Errors.DeleteOccException">Optimistic concurrency violation — the aggregate has been modified. Re-read and retry.</exception>
    /// <exception cref="Errors.AggregateNotFoundException">The aggregate does not exist.</exception>
    /// <exception cref="Errors.ConnectionFailedException">No reachable leader could be found.</exception>
    /// <exception cref="Errors.CeleriantTimeoutException">The request timed out.</exception>
    /// <exception cref="ObjectDisposedException">The pool has been disposed.</exception>
    Task<SuccessResponse> DeleteAsync(DeleteRequest request, CancellationToken ct = default);

    /// <summary>Send a trim-start request. Routed to the leader with automatic failover.</summary>
    /// <exception cref="Errors.AggregateNotFoundException">The aggregate does not exist.</exception>
    /// <exception cref="Errors.TrimIndexOutOfRangeException">The trim index is beyond the aggregate's current range.</exception>
    /// <exception cref="Errors.ConnectionFailedException">No reachable leader could be found.</exception>
    /// <exception cref="Errors.CeleriantTimeoutException">The request timed out.</exception>
    /// <exception cref="ObjectDisposedException">The pool has been disposed.</exception>
    Task<SuccessResponse> TrimStartAsync(TrimStartRequest request, CancellationToken ct = default);

    /// <summary>Stream all event batches for an aggregate, automatically following pagination cursors.</summary>
    /// <exception cref="Errors.AggregateNotFoundException">The aggregate does not exist.</exception>
    /// <exception cref="Errors.BatchIndexUnavailableException">The requested batch index has been trimmed.</exception>
    /// <exception cref="Errors.ConnectionFailedException">No reachable node could be found.</exception>
    /// <exception cref="Errors.CeleriantTimeoutException">The request timed out.</exception>
    /// <exception cref="ObjectDisposedException">The pool has been disposed.</exception>
    IAsyncEnumerable<AggregateEventBatch> ReadAllAsync(
        AggregateKey key,
        ReadFilters? filters = null,
        CancellationToken ct = default);

    /// <summary>Stream all organisations.</summary>
    /// <exception cref="Errors.ConnectionFailedException">No reachable node could be found.</exception>
    /// <exception cref="Errors.CeleriantTimeoutException">The request timed out.</exception>
    /// <exception cref="ObjectDisposedException">The pool has been disposed.</exception>
    IAsyncEnumerable<OrgListItem> ListOrgsAsync(ListOptions? options = null, CancellationToken ct = default);

    /// <summary>Stream all aggregate types.</summary>
    /// <exception cref="Errors.ConnectionFailedException">No reachable node could be found.</exception>
    /// <exception cref="Errors.CeleriantTimeoutException">The request timed out.</exception>
    /// <exception cref="ObjectDisposedException">The pool has been disposed.</exception>
    IAsyncEnumerable<AggregateTypeListItem> ListAggregateTypesAsync(
        Guid? orgId = null,
        ListOptions? options = null,
        CancellationToken ct = default);

    /// <summary>Stream aggregates with merged statistics.</summary>
    /// <exception cref="Errors.ConnectionFailedException">No reachable node could be found.</exception>
    /// <exception cref="Errors.CeleriantTimeoutException">The request timed out.</exception>
    /// <exception cref="ObjectDisposedException">The pool has been disposed.</exception>
    IAsyncEnumerable<AggregateStats> ListAggregatesAsync(
        Guid? orgId = null,
        Guid? aggregateTypeId = null,
        ListOptions? options = null,
        CancellationToken ct = default);

    /// <summary>Open a dedicated watch connection (not pooled).</summary>
    /// <exception cref="Errors.WatchErrorException">The watch request was invalid, the requested latency is too high, or the server has too many subscribers.</exception>
    /// <exception cref="Errors.ConnectionFailedException">No reachable node could be found.</exception>
    /// <exception cref="Errors.CeleriantTimeoutException">The connection or handshake timed out.</exception>
    /// <exception cref="ObjectDisposedException">The pool has been disposed.</exception>
    Task<WatchConnection> WatchAsync(
        WatchRequest request,
        WatchOptions? options = null,
        CancellationToken ct = default);

    /// <summary>Borrow a connection from the pool for manual low-level use.</summary>
    /// <exception cref="ObjectDisposedException">The pool has been disposed.</exception>
    Task<PooledConnection> GetConnectionAsync(CancellationToken ct = default);
}
