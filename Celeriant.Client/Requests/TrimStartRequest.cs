using MessagePack;
using MessagePack.Formatters;
using Celeriant.Client.Protocol;

namespace Celeriant.Client.Requests;

[MessagePackObject]
public sealed class TrimStartRequest
{
    [Key(0)]
    [MessagePackFormatter(typeof(CeleriantNullableGuidFormatter))]
    public Guid? CorrelationId { get; init; }

    [Key(1)]
    public required AggregateKey AggregateKey { get; init; }

    [Key(2)]
    [MessagePackFormatter(typeof(UInt64AsInt64Formatter))]
    public long KeepFromAggregateVersion { get; init; }

    [Key(3)]
    [MessagePackFormatter(typeof(CeleriantGuidFormatter))]
    public required Guid ClientId { get; init; }

    [Key(4)]
    [MessagePackFormatter(typeof(CeleriantNullableGuidFormatter))]
    public Guid? UserId { get; init; }
}
