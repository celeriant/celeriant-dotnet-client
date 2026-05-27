using MessagePack;
using MessagePack.Formatters;
using Celeriant.Client.Protocol;

namespace Celeriant.Client.Requests;

/// <summary>
/// Filtering options for a <see cref="ReadRequest"/>.
/// All fields are optional except <see cref="FromAggregateVersion"/>.
///
/// Use the static factory <see cref="From"/> or C# <c>with</c> expressions to build instances:
/// <code>
/// var filters = ReadFilters.From(1) with { ToAggregateVersion = 100 };
/// </code>
/// </summary>
[MessagePackObject]
public record struct ReadFilters
{
    /// <summary>Start reading from this event batch index (inclusive). Minimum value is 1;
    /// values of 0 are treated as 1.</summary>
    [Key(0)]
    [MessagePackFormatter(typeof(UInt64AsInt64Formatter))]
    public long FromAggregateVersion { get; init; }

    /// <summary>Stop reading at this event batch index (inclusive). Null means read to the end.</summary>
    [Key(1)]
    [MessagePackFormatter(typeof(NullableUInt64AsInt64Formatter))]
    public long? ToAggregateVersion { get; init; }

    /// <summary>Only include events whose event type matches one of these values.</summary>
    [Key(2)]
    [MessagePackFormatter(typeof(NullableUInt64ArrayAsInt64ArrayFormatter))]
    public long[]? IncludeEventTypes { get; init; }

    /// <summary>Exclude events written by this client ID.</summary>
    [Key(3)]
    [MessagePackFormatter(typeof(CeleriantNullableGuidFormatter))]
    public Guid? ExcludeClientId { get; init; }

    /// <summary>Only include events written by this client ID.</summary>
    [Key(4)]
    [MessagePackFormatter(typeof(CeleriantNullableGuidFormatter))]
    public Guid? IncludeClientId { get; init; }

    /// <summary>Exclude events written by this user ID.</summary>
    [Key(5)]
    [MessagePackFormatter(typeof(CeleriantNullableGuidFormatter))]
    public Guid? ExcludeUserId { get; init; }

    /// <summary>Only include events written by this user ID.</summary>
    [Key(6)]
    [MessagePackFormatter(typeof(CeleriantNullableGuidFormatter))]
    public Guid? IncludeUserId { get; init; }

    /// <summary>Only include events with a server timestamp at or after this value.</summary>
    [Key(7)]
    [MessagePackFormatter(typeof(NullableEpochMillisFormatter))]
    public DateTimeOffset? MinServerTimestamp { get; init; }

    /// <summary>Only include events with a server timestamp at or before this value.</summary>
    [Key(8)]
    [MessagePackFormatter(typeof(NullableEpochMillisFormatter))]
    public DateTimeOffset? MaxServerTimestamp { get; init; }

    /// <summary>Only include events with a client event index at or above this value.</summary>
    [Key(9)]
    [MessagePackFormatter(typeof(NullableUInt64AsInt64Formatter))]
    public long? MinClientSeq { get; init; }

    /// <summary>Only include events with a client event index at or below this value.</summary>
    [Key(10)]
    [MessagePackFormatter(typeof(NullableUInt64AsInt64Formatter))]
    public long? MaxClientSeq { get; init; }

    /// <summary>Only include events with a client-supplied event timestamp at or after this value.</summary>
    [Key(11)]
    [MessagePackFormatter(typeof(NullableEpochMillisFormatter))]
    public DateTimeOffset? MinEventTimestamp { get; init; }

    /// <summary>Only include events with a client-supplied event timestamp at or before this value.</summary>
    [Key(12)]
    [MessagePackFormatter(typeof(NullableEpochMillisFormatter))]
    public DateTimeOffset? MaxEventTimestamp { get; init; }

    /// <summary>Only include events with an event index at or above this value.</summary>
    [Key(13)]
    [MessagePackFormatter(typeof(NullableUInt64AsInt64Formatter))]
    public long? MinEventSeq { get; init; }

    /// <summary>Only include events with an event index at or below this value.</summary>
    [Key(14)]
    [MessagePackFormatter(typeof(NullableUInt64AsInt64Formatter))]
    public long? MaxEventSeq { get; init; }

    /// <summary>
    /// Create a <see cref="ReadFilters"/> starting from the given event batch index.
    /// Clamps the value to a minimum of 1 (batch index 0 is invalid for reads).
    /// </summary>
    public static ReadFilters From(long fromAggregateVersion) =>
        new() { FromAggregateVersion = Math.Max(1, fromAggregateVersion) };
}
