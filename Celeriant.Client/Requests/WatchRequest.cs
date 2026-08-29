using MessagePack;
using MessagePack.Formatters;
using Celeriant.Client.Protocol;
using Celeriant.Client.Responses;

namespace Celeriant.Client.Requests;

[MessagePackObject]
public sealed record WatchRequest
{
    [Key(0)]
    [MessagePackFormatter(typeof(CeleriantNullableGuidFormatter))]
    public Guid? CorrelationId { get; init; }

    /// <summary>Optional server-side batching window: the server may coalesce events for up to this
    /// long before pushing, trading freshness for fewer wakeups. The server caps it (2000 ms on a
    /// default deployment); a larger value fails the watch with <see cref="Errors.WatchErrorException"/>
    /// (error 8001, see its <c>MaxMs</c>). Leave null for the lowest latency.</summary>
    [Key(1)]
    [MessagePackFormatter(typeof(NullableMillisTimeSpanFormatter))]
    public TimeSpan? RequestedLatency { get; init; }

    /// <summary>Internal shard override, set by the client's multi-shard routing. A value you assign
    /// here is replaced before the request is sent and has no effect — leave it unset. See the guide:
    /// the watch handles shard routing for you.</summary>
    [Key(2)]
    [MessagePackFormatter(typeof(NullableUInt64AsInt64Formatter))]
    public long? ShardId { get; init; }

    /// <summary>Only deliver events for aggregates in these organisations. Null or empty means all
    /// organisations. Combined with the other filters by AND (an event must match every filter set).</summary>
    [Key(3)]
    [MessagePackFormatter(typeof(NullableGuidHashSetFormatter))]
    public HashSet<Guid>? Orgs { get; init; }

    /// <summary>Only deliver events for aggregates of these aggregate types (the type id). Null or
    /// empty means all types.</summary>
    [Key(4)]
    [MessagePackFormatter(typeof(NullableGuidHashSetFormatter))]
    public HashSet<Guid>? AggregateTypes { get; init; }

    /// <summary>Only deliver events for these specific aggregates, named by their bare aggregate id
    /// (the leaf id, not the full three-part <see cref="AggregateKey"/>). Null or empty means all
    /// aggregates.</summary>
    [Key(5)]
    [MessagePackFormatter(typeof(NullableGuidHashSetFormatter))]
    public HashSet<Guid>? Aggregates { get; init; }

    /// <summary>Only deliver events for these operation types (write, delete, …). Null or empty means
    /// all operations.</summary>
    [Key(6)]
    public HashSet<WatchOperationType>? OperationTypes { get; init; }
}
