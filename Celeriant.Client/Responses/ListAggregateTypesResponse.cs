using MessagePack;
using MessagePack.Formatters;
using Celeriant.Client.Protocol;

namespace Celeriant.Client.Responses;

[MessagePackObject]
public sealed class ListAggregateTypesResponse
{
    [Key(0)]
    [MessagePackFormatter(typeof(CeleriantNullableGuidFormatter))]
    public Guid? CorrelationId { get; init; }

    [Key(1)]
    public AggregateTypeListItem[] AggregateTypes { get; init; } = [];

    [Key(2)]
    [MessagePackFormatter(typeof(NullableUInt64AsInt64Formatter))]
    public long? NextCursor { get; init; }
}
