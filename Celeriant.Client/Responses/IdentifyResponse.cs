using MessagePack;
using MessagePack.Formatters;
using Celeriant.Client.Protocol;

namespace Celeriant.Client.Responses;

[MessagePackObject]
public sealed class IdentifyResponse
{
    [Key(0)]
    [MessagePackFormatter(typeof(CeleriantNullableGuidFormatter))]
    public Guid? CorrelationId { get; init; }

    [Key(1)]
    [MessagePackFormatter(typeof(CeleriantNullableGuidFormatter))]
    public Guid? ClientId { get; init; }

    [Key(2)]
    public AccessLevel? AccessLevel { get; init; }
}
