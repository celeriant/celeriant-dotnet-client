using MessagePack;
using MessagePack.Formatters;
using Celeriant.Client.Protocol;

namespace Celeriant.Client.Responses;

[MessagePackObject]
public sealed class WatchResponseEvent
{
    [Key(0)]
    [MessagePackFormatter(typeof(CeleriantGuidFormatter))]
    public Guid OrgId { get; init; }

    [Key(1)]
    [MessagePackFormatter(typeof(CeleriantGuidFormatter))]
    public Guid AggregateTypeId { get; init; }

    [Key(2)]
    [MessagePackFormatter(typeof(CeleriantGuidFormatter))]
    public Guid AggregateId { get; init; }

    [Key(3)]
    public WatchOperationType Operation { get; init; }

    [Key(4)]
    [MessagePackFormatter(typeof(NullableUInt64AsInt64Formatter))]
    public long? FromEventBatchIndex { get; init; }

    [Key(5)]
    [MessagePackFormatter(typeof(NullableUInt64AsInt64Formatter))]
    public long? ToEventBatchIndex { get; init; }

    [Key(6)]
    [MessagePackFormatter(typeof(NullableUInt64AsInt64Formatter))]
    public long? KeepFromEventBatchIndex { get; init; }
}
