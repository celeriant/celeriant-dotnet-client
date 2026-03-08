using MessagePack;
using MessagePack.Formatters;
using Celeriant.Client.Protocol;

namespace Celeriant.Client.Responses;

[MessagePackObject]
public sealed class AggregateListItem
{
    [Key(0)]
    public bool IsDeleted { get; init; }

    [Key(1)]
    [MessagePackFormatter(typeof(CeleriantGuidFormatter))]
    public Guid OrgId { get; init; }

    [Key(2)]
    [MessagePackFormatter(typeof(CeleriantGuidFormatter))]
    public Guid AggregateTypeId { get; init; }

    [Key(3)]
    [MessagePackFormatter(typeof(CeleriantGuidFormatter))]
    public Guid AggregateId { get; init; }

    [Key(4)]
    [MessagePackFormatter(typeof(UInt64AsInt64Formatter))]
    public long EventBatchCount { get; init; }

    [Key(5)]
    [MessagePackFormatter(typeof(ZeroAsNullEpochMillisFormatter))]
    public DateTimeOffset? MinEventTimestamp { get; init; }

    [Key(6)]
    [MessagePackFormatter(typeof(ZeroAsNullEpochMillisFormatter))]
    public DateTimeOffset? MaxEventTimestamp { get; init; }

    [Key(7)]
    [MessagePackFormatter(typeof(UInt64AsInt64Formatter))]
    public long MinEventBatchIndex { get; init; }

    [Key(8)]
    [MessagePackFormatter(typeof(UInt64AsInt64Formatter))]
    public long MaxEventBatchIndex { get; init; }

    [Key(9)]
    [MessagePackFormatter(typeof(UInt64AsInt64Formatter))]
    public long MinEventIndex { get; init; }

    [Key(10)]
    [MessagePackFormatter(typeof(UInt64AsInt64Formatter))]
    public long MaxEventIndex { get; init; }

    [Key(11)]
    [MessagePackFormatter(typeof(ZeroAsNullEpochMillisFormatter))]
    public DateTimeOffset? MinServerTimestamp { get; init; }

    [Key(12)]
    [MessagePackFormatter(typeof(ZeroAsNullEpochMillisFormatter))]
    public DateTimeOffset? MaxServerTimestamp { get; init; }

    [Key(13)]
    [MessagePackFormatter(typeof(UInt64AsInt64Formatter))]
    public long CompressedSize { get; init; }

    [Key(14)]
    [MessagePackFormatter(typeof(UInt64AsInt64Formatter))]
    public long UncompressedSize { get; init; }
}
