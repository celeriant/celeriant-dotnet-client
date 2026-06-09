using Celeriant.Client.Requests;
using Celeriant.Client.Responses;

namespace Celeriant.Client;

/// <summary>
/// Abstraction over <see cref="CeleriantClient"/> for testing and dependency injection.
/// For production use with connection pooling and automatic failover, prefer <see cref="ICeleriantPool"/>.
/// </summary>
public interface ICeleriantClient : IAsyncDisposable
{
    /// <summary>Send a read request and return the typed response.</summary>
    /// <exception cref="Errors.AggregateNotFoundException">The aggregate does not exist.</exception>
    /// <exception cref="Errors.BatchIndexUnavailableException">The requested batch index has been trimmed. Re-read from <see cref="Errors.BatchIndexUnavailableException.MinimumAvailableVersion"/>.</exception>
    /// <exception cref="Errors.ConnectionFailedException">The connection was lost.</exception>
    /// <exception cref="Errors.CeleriantTimeoutException">The request timed out.</exception>
    Task<ReadResponse> ReadAsync(ReadRequest request, CancellationToken ct = default);

    /// <summary>Send a write request and return the typed response.</summary>
    /// <exception cref="Errors.WriteOccException">Optimistic concurrency violation — the aggregate has been modified. Re-read and retry.</exception>
    /// <exception cref="Errors.IdempotencyViolationException">Duplicate write — the event was already accepted. No action needed.</exception>
    /// <exception cref="Errors.AggregateNotFoundException">The aggregate does not exist and <c>AllowCreate</c> is false.</exception>
    /// <exception cref="Errors.AggregateRecreateNotAllowedException">The aggregate was permanently deleted.</exception>
    /// <exception cref="Errors.SchemaValidationException">An event payload does not conform to the registered schema.</exception>
    /// <exception cref="Errors.ShardRoutingException">A multi-aggregate write targets aggregates on different shards.</exception>
    /// <exception cref="Errors.NotLeaderException">The target node is not the leader. Retry against <see cref="Errors.NotLeaderException.LeaderAddress"/>.</exception>
    /// <exception cref="Errors.ConnectionFailedException">The connection was lost.</exception>
    /// <exception cref="Errors.CeleriantTimeoutException">The request timed out.</exception>
    Task<WriteResponse> WriteAsync(WriteRequest request, CancellationToken ct = default);

    /// <summary>Write events to a single aggregate. Creates the aggregate if it does not exist.</summary>
    /// <exception cref="Errors.WriteOccException">Optimistic concurrency violation — the aggregate has been modified. Re-read and retry.</exception>
    /// <exception cref="Errors.IdempotencyViolationException">Duplicate write — the event was already accepted. No action needed.</exception>
    /// <exception cref="Errors.AggregateNotFoundException">The aggregate does not exist and <paramref name="allowCreate"/> is false.</exception>
    /// <exception cref="Errors.AggregateRecreateNotAllowedException">The aggregate was permanently deleted.</exception>
    /// <exception cref="Errors.SchemaValidationException">An event payload does not conform to the registered schema.</exception>
    /// <exception cref="Errors.NotLeaderException">The target node is not the leader. Retry against <see cref="Errors.NotLeaderException.LeaderAddress"/>.</exception>
    /// <exception cref="Errors.ConnectionFailedException">The connection was lost.</exception>
    /// <exception cref="Errors.CeleriantTimeoutException">The request timed out.</exception>
    Task<WriteResponse> WriteAsync(
        AggregateKey key,
        AggregateEvent[] events,
        Guid clientId,
        bool allowCreate = true,
        long? expectedVersion = null,
        bool enforceClientIdempotency = false,
        CancellationToken ct = default);

    /// <summary>Send a delete request and return the typed response.</summary>
    /// <exception cref="Errors.DeleteOccException">Optimistic concurrency violation — the aggregate has been modified. Re-read and retry.</exception>
    /// <exception cref="Errors.AggregateNotFoundException">The aggregate does not exist.</exception>
    /// <exception cref="Errors.NotLeaderException">The target node is not the leader. Retry against <see cref="Errors.NotLeaderException.LeaderAddress"/>.</exception>
    /// <exception cref="Errors.ConnectionFailedException">The connection was lost.</exception>
    /// <exception cref="Errors.CeleriantTimeoutException">The request timed out.</exception>
    Task<SuccessResponse> DeleteAsync(DeleteRequest request, CancellationToken ct = default);

    /// <summary>Send a trim-start request and return the typed response.</summary>
    /// <exception cref="Errors.AggregateNotFoundException">The aggregate does not exist.</exception>
    /// <exception cref="Errors.TrimIndexOutOfRangeException">The trim index is beyond the aggregate's current range.</exception>
    /// <exception cref="Errors.NotLeaderException">The target node is not the leader. Retry against <see cref="Errors.NotLeaderException.LeaderAddress"/>.</exception>
    /// <exception cref="Errors.ConnectionFailedException">The connection was lost.</exception>
    /// <exception cref="Errors.CeleriantTimeoutException">The request timed out.</exception>
    Task<SuccessResponse> TrimStartAsync(TrimStartRequest request, CancellationToken ct = default);

    /// <summary>Send an aggregate details request and return the typed response.</summary>
    /// <exception cref="Errors.AggregateNotFoundException">The aggregate does not exist.</exception>
    /// <exception cref="Errors.ConnectionFailedException">The connection was lost.</exception>
    /// <exception cref="Errors.CeleriantTimeoutException">The request timed out.</exception>
    Task<AggregateDetailsResponse> AggregateDetailsAsync(AggregateDetailsRequest request, CancellationToken ct = default);

    /// <summary>Send a register-schema request and return the typed response.</summary>
    /// <exception cref="Errors.SchemaErrorException">The schema is invalid, unsupported, or already registered.</exception>
    /// <exception cref="Errors.NotLeaderException">The target node is not the leader. Retry against <see cref="Errors.NotLeaderException.LeaderAddress"/>.</exception>
    /// <exception cref="Errors.ConnectionFailedException">The connection was lost.</exception>
    /// <exception cref="Errors.CeleriantTimeoutException">The request timed out.</exception>
    Task<SuccessResponse> RegisterSchemaAsync(RegisterSchemaRequest request, CancellationToken ct = default);
}
