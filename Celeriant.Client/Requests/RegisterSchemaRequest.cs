using MessagePack;
using MessagePack.Formatters;
using Celeriant.Client.Protocol;

namespace Celeriant.Client.Requests;

[MessagePackObject]
public sealed class RegisterSchemaRequest
{
    [Key(0)]
    [MessagePackFormatter(typeof(CeleriantNullableGuidFormatter))]
    public Guid? CorrelationId { get; init; }

    [Key(1)]
    [MessagePackFormatter(typeof(CeleriantGuidFormatter))]
    public Guid ClientId { get; init; }

    [Key(2)]
    [MessagePackFormatter(typeof(CeleriantNullableGuidFormatter))]
    public Guid? UserId { get; init; }

    [Key(3)]
    public required SchemaKey SchemaKey { get; init; }

    [Key(4)]
    public SchemaType SchemaType { get; init; }

    [Key(5)]
    public required string Schema { get; init; }
}
