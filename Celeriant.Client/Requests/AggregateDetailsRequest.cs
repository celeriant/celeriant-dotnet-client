using MessagePack;
using MessagePack.Formatters;
using Celeriant.Client.Protocol;

namespace Celeriant.Client.Requests;

[MessagePackObject]
public sealed class AggregateDetailsRequest
{
    [Key(0)]
    [MessagePackFormatter(typeof(CeleriantNullableGuidFormatter))]
    public Guid? CorrelationId { get; init; }

    [Key(1)]
    public required AggregateKey AggregateKey { get; init; }
}
