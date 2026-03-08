using MessagePack;
using MessagePack.Formatters;
using Celeriant.Client.Protocol;

namespace Celeriant.Client.Requests;

[MessagePackObject]
public sealed class ListAggregateTypesRequest
{
    [Key(0)]
    [MessagePackFormatter(typeof(CeleriantNullableGuidFormatter))]
    public Guid? CorrelationId { get; init; }

    [Key(1)]
    [MessagePackFormatter(typeof(UInt64AsInt64Formatter))]
    public long ShardId { get; init; }

    [Key(2)]
    [MessagePackFormatter(typeof(CeleriantNullableGuidFormatter))]
    public Guid? OrgId { get; init; }

    [Key(3)]
    [MessagePackFormatter(typeof(NullableUInt64AsInt64Formatter))]
    public long? Cursor { get; init; }
}
