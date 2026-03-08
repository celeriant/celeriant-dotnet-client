using MessagePack;
using MessagePack.Formatters;
using Celeriant.Client.Protocol;

namespace Celeriant.Client.Requests;

/// <summary>
/// Filtering options for a <see cref="ReadRequest"/>.
/// All fields are optional except <see cref="FromEventBatchIndex"/>.
///
/// Use the static factory <see cref="From"/> or C# <c>with</c> expressions to build instances:
/// <code>
/// var filters = ReadFilters.From(1) with { ToEventBatchIndex = 100 };
/// </code>
/// </summary>
[MessagePackObject]
public sealed class ReadFilters
{
    [Key(0)]
    [MessagePackFormatter(typeof(UInt64AsInt64Formatter))]
    public long FromEventBatchIndex { get; init; }

    [Key(1)]
    [MessagePackFormatter(typeof(NullableUInt64AsInt64Formatter))]
    public long? ToEventBatchIndex { get; init; }

    [Key(2)]
    [MessagePackFormatter(typeof(NullableUInt64ArrayAsInt64ArrayFormatter))]
    public long[]? IncludeEventTypes { get; init; }

    [Key(3)]
    [MessagePackFormatter(typeof(CeleriantNullableGuidFormatter))]
    public Guid? ExcludeClientId { get; init; }

    [Key(4)]
    [MessagePackFormatter(typeof(CeleriantNullableGuidFormatter))]
    public Guid? IncludeClientId { get; init; }

    [Key(5)]
    [MessagePackFormatter(typeof(CeleriantNullableGuidFormatter))]
    public Guid? ExcludeUserId { get; init; }

    [Key(6)]
    [MessagePackFormatter(typeof(CeleriantNullableGuidFormatter))]
    public Guid? IncludeUserId { get; init; }

    [Key(7)]
    [MessagePackFormatter(typeof(NullableEpochMillisFormatter))]
    public DateTimeOffset? MinServerTimestamp { get; init; }

    [Key(8)]
    [MessagePackFormatter(typeof(NullableEpochMillisFormatter))]
    public DateTimeOffset? MaxServerTimestamp { get; init; }

    [Key(9)]
    [MessagePackFormatter(typeof(NullableUInt64AsInt64Formatter))]
    public long? MinClientEventIndex { get; init; }

    [Key(10)]
    [MessagePackFormatter(typeof(NullableUInt64AsInt64Formatter))]
    public long? MaxClientEventIndex { get; init; }

    [Key(11)]
    [MessagePackFormatter(typeof(NullableEpochMillisFormatter))]
    public DateTimeOffset? MinEventTimestamp { get; init; }

    [Key(12)]
    [MessagePackFormatter(typeof(NullableEpochMillisFormatter))]
    public DateTimeOffset? MaxEventTimestamp { get; init; }

    [Key(13)]
    [MessagePackFormatter(typeof(NullableUInt64AsInt64Formatter))]
    public long? MinEventIndex { get; init; }

    [Key(14)]
    [MessagePackFormatter(typeof(NullableUInt64AsInt64Formatter))]
    public long? MaxEventIndex { get; init; }

    /// <summary>
    /// Create a <see cref="ReadFilters"/> starting from the given event batch index.
    /// Clamps the value to a minimum of 1 (batch index 0 is invalid for reads).
    /// </summary>
    public static ReadFilters From(long fromEventBatchIndex) =>
        new() { FromEventBatchIndex = Math.Max(1, fromEventBatchIndex) };
}
