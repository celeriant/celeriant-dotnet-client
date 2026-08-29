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
    /// <summary>Client-assigned index of this event within its write batch, starting at 1.
    /// Together with the write's <c>ClientId</c> it keys idempotency, so events in one write must
    /// carry distinct increasing values. When building events by hand rather than with
    /// <c>AggregateEventExtensions.Create</c>, set this explicitly — it has no safe default.</summary>
    [Key(0)]
    [MessagePackFormatter(typeof(UInt64AsInt64Formatter))]
    public long ClientSeq { get; init; }

    /// <summary>Server-assigned sequence, populated on read. Leave 0 when writing.</summary>
    [Key(1)]
    [MessagePackFormatter(typeof(UInt64AsInt64Formatter))]
    public long EventSeq { get; init; }

    /// <summary>Optional client-assigned event ID (u128 as Guid), used to distinguish an idempotent
    /// retry from a genuinely new event. Serialized as 16 big-endian bytes.</summary>
    [Key(2)]
    [MessagePackFormatter(typeof(CeleriantNullableGuidFormatter))]
    public Guid? EventId { get; init; }

    /// <summary>Client-supplied event time, stored verbatim. <b>Required when building an event by
    /// hand:</b> the wire format is unsigned epoch milliseconds and cannot represent a time before
    /// 1970, so a default/unset value (<see cref="DateTimeOffset.MinValue"/>) — or any pre-epoch time —
    /// is rejected at write time with <see cref="ArgumentException"/>.
    /// <c>AggregateEventExtensions.Create</c> defaults it to <see cref="DateTimeOffset.UtcNow"/>.</summary>
    [Key(3)]
    [MessagePackFormatter(typeof(EpochMillisFormatter))]
    public DateTimeOffset EventTimestamp { get; init; }

    /// <summary>Major event type identifier (maps to a registered schema). <b>Required:</b> the server
    /// rejects a write whose event leaves this 0.</summary>
    [Key(4)]
    [MessagePackFormatter(typeof(UInt64AsInt64Formatter))]
    public long EventTypeMajor { get; init; }

    /// <summary>Minor event type identifier. Optional; defaults to 0.</summary>
    [Key(5)]
    [MessagePackFormatter(typeof(UInt64AsInt64Formatter))]
    public long EventTypeMinor { get; init; }

    /// <summary>Serialized event payload, encoded as msgpack binary on the wire. Must not be null
    /// (defaults to an empty array); a null value is rejected client-side before the request is sent.</summary>
    [Key(6)]
    public byte[] EventValue { get; init; } = [];

    /// <summary>AES-GCM initialization vector (12 bytes) for encrypted events. Encoded as msgpack binary.</summary>
    [Key(7)]
    public byte[]? Iv { get; init; }
}
