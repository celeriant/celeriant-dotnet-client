using MessagePack;
using MessagePack.Formatters;
using Celeriant.Client.Protocol;

namespace Celeriant.Client.Responses;

/// <summary>
/// A batch of events for a single aggregate, as returned in a <see cref="ReadResponse"/>.
///
/// Fields are positional (compact array mode) matching Rust struct field order.
/// u128 fields are serialized as 16-byte binary (rmp-serde default).
/// </summary>
[MessagePackObject]
public sealed class AggregateEventBatch
{
    /// <summary>This batch's version — the "aggregate version" (a.k.a. batch index): the value to pass
    /// as a read cursor to resume, and the same number <c>expectedVersion</c> guards against.</summary>
    [Key(0)]
    [MessagePackFormatter(typeof(UInt64AsInt64Formatter))]
    public long AggregateVersion { get; init; }

    /// <summary>Client ID that wrote this batch. Serialized as 16 big-endian bytes.</summary>
    [Key(1)]
    [MessagePackFormatter(typeof(CeleriantGuidFormatter))]
    public Guid ClientId { get; init; }

    /// <summary>User ID associated with this batch. Serialized as 16 big-endian bytes.</summary>
    [Key(2)]
    [MessagePackFormatter(typeof(CeleriantNullableGuidFormatter))]
    public Guid? UserId { get; init; }

    /// <summary>Server-assigned time the batch was committed.</summary>
    [Key(3)]
    [MessagePackFormatter(typeof(EpochMillisFormatter))]
    public DateTimeOffset ServerTimestamp { get; init; }

    /// <summary>The events in this batch, in order.</summary>
    [Key(4)]
    public AggregateEvent[] Events { get; init; } = [];
}
