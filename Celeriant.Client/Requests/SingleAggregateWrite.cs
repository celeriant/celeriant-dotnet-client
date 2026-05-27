using MessagePack;
using MessagePack.Formatters;
using Celeriant.Client.Protocol;
using Celeriant.Client.Responses;

namespace Celeriant.Client.Requests;

/// <summary>
/// Write payload for a single aggregate within a <see cref="WriteRequest"/>.
/// </summary>
[MessagePackObject]
public sealed class SingleAggregateWrite
{
    [Key(0)]
    public required AggregateEvent[] Events { get; init; }

    [Key(1)]
    public bool AllowCreate { get; init; }

    [Key(2)]
    [MessagePackFormatter(typeof(NullableUInt64AsInt64Formatter))]
    public long? ExpectedVersion { get; init; }

    [Key(3)]
    public bool EnforceClientIdempotency { get; init; }
}
