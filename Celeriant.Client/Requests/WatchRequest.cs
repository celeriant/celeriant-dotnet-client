using MessagePack;
using MessagePack.Formatters;
using Celeriant.Client.Protocol;
using Celeriant.Client.Responses;

namespace Celeriant.Client.Requests;

[MessagePackObject]
public sealed class WatchRequest
{
    [Key(0)]
    [MessagePackFormatter(typeof(CeleriantNullableGuidFormatter))]
    public Guid? CorrelationId { get; init; }

    [Key(1)]
    [MessagePackFormatter(typeof(NullableMillisTimeSpanFormatter))]
    public TimeSpan? RequestedLatency { get; init; }

    [Key(2)]
    [MessagePackFormatter(typeof(NullableUInt64AsInt64Formatter))]
    public long? ShardId { get; init; }

    [Key(3)]
    [MessagePackFormatter(typeof(NullableGuidHashSetFormatter))]
    public HashSet<Guid>? Orgs { get; init; }

    [Key(4)]
    [MessagePackFormatter(typeof(NullableGuidHashSetFormatter))]
    public HashSet<Guid>? AggregateTypes { get; init; }

    [Key(5)]
    [MessagePackFormatter(typeof(NullableGuidHashSetFormatter))]
    public HashSet<Guid>? Aggregates { get; init; }

    [Key(6)]
    public HashSet<WatchOperationType>? OperationTypes { get; init; }
}
