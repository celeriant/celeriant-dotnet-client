using MessagePack;
using MessagePack.Formatters;
using Celeriant.Client.Protocol;

namespace Celeriant.Client.Responses;

[MessagePackObject]
public sealed class AggregateDetailsResponse
{
    [Key(0)]
    [MessagePackFormatter(typeof(CeleriantNullableGuidFormatter))]
    public Guid? CorrelationId { get; init; }

    [Key(1)]
    [MessagePackFormatter(typeof(UInt64AsInt64Formatter))]
    public long MinEventBatchIndex { get; init; }

    [Key(2)]
    [MessagePackFormatter(typeof(UInt64AsInt64Formatter))]
    public long MaxEventBatchIndex { get; init; }

    [Key(3)]
    [MessagePackFormatter(typeof(UInt64AsInt64Formatter))]
    public long MaxEventIndex { get; init; }

    [Key(4)]
    public bool IsDeleted { get; init; }

    [Key(5)]
    public bool AllowRecreate { get; init; }

    [Key(6)]
    public bool AllowIndexContinuation { get; init; }

    [Key(7)]
    [MessagePackFormatter(typeof(EpochMillisFormatter))]
    public DateTimeOffset LastServerTimestamp { get; init; }

    [Key(8)]
    [MessagePackFormatter(typeof(CeleriantGuidFormatter))]
    public Guid LastClientId { get; init; }

    [Key(9)]
    [MessagePackFormatter(typeof(CeleriantNullableGuidFormatter))]
    public Guid? LastUserId { get; init; }
}
