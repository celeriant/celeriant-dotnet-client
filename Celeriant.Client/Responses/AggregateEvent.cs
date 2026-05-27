using MessagePack;
using MessagePack.Formatters;
using Celeriant.Client.Protocol;

namespace Celeriant.Client.Responses;

/// <summary>
/// A single event within an <see cref="AggregateEventBatch"/>.
///
/// Fields are positional (compact array mode) matching Rust struct field order.
/// u128 fields are serialized as 16-byte binary (rmp-serde default).
/// byte[] fields are serialized as msgpack binary.
/// </summary>
[MessagePackObject]
public sealed class AggregateEvent
{
    [Key(0)]
    [MessagePackFormatter(typeof(UInt64AsInt64Formatter))]
    public long ClientSeq { get; init; }

    [Key(1)]
    [MessagePackFormatter(typeof(UInt64AsInt64Formatter))]
    public long EventSeq { get; init; }

    /// <summary>Client-assigned event ID (u128 as Guid). Serialized as 16 big-endian bytes.</summary>
    [Key(2)]
    [MessagePackFormatter(typeof(CeleriantNullableGuidFormatter))]
    public Guid? EventId { get; init; }

    [Key(3)]
    [MessagePackFormatter(typeof(EpochMillisFormatter))]
    public DateTimeOffset EventTimestamp { get; init; }

    [Key(4)]
    [MessagePackFormatter(typeof(UInt64AsInt64Formatter))]
    public long EventTypeMajor { get; init; }

    [Key(5)]
    [MessagePackFormatter(typeof(UInt64AsInt64Formatter))]
    public long EventTypeMinor { get; init; }

    /// <summary>Serialized event payload. Encoded as msgpack binary on the wire.</summary>
    [Key(6)]
    public byte[] EventValue { get; init; } = [];

    /// <summary>AES-GCM initialization vector (12 bytes) for encrypted events. Encoded as msgpack binary.</summary>
    [Key(7)]
    public byte[]? Iv { get; init; }
}
