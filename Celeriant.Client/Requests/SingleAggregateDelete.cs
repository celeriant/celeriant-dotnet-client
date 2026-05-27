using MessagePack;
using MessagePack.Formatters;
using Celeriant.Client.Protocol;

namespace Celeriant.Client.Requests;

[MessagePackObject]
public sealed class SingleAggregateDelete
{
    [Key(0)]
    public bool AllowRecreate { get; init; }

    [Key(1)]
    public bool AllowSequenceContinuation { get; init; }

    [Key(2)]
    [MessagePackFormatter(typeof(NullableUInt64AsInt64Formatter))]
    public long? ExpectedVersion { get; init; }
}
