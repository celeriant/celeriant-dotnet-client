using MessagePack;
using MessagePack.Formatters;
using Celeriant.Client.Protocol;

namespace Celeriant.Client.Responses;

/// <summary>
/// A single change notification from a watch: which aggregate changed, how, and over which version
/// range. It carries the aggregate's identity and the shape of the change, not the event payloads —
/// read the aggregate if you need the events themselves.
/// </summary>
[MessagePackObject]
public sealed class WatchResponseEvent
{
    /// <summary>Organisation of the changed aggregate.</summary>
    [Key(0)]
    [MessagePackFormatter(typeof(CeleriantGuidFormatter))]
    public Guid OrgId { get; init; }

    /// <summary>Aggregate type of the changed aggregate.</summary>
    [Key(1)]
    [MessagePackFormatter(typeof(CeleriantGuidFormatter))]
    public Guid AggregateTypeId { get; init; }

    /// <summary>The changed aggregate's id (the leaf id, not the full three-part key).</summary>
    [Key(2)]
    [MessagePackFormatter(typeof(CeleriantGuidFormatter))]
    public Guid AggregateId { get; init; }

    /// <summary>What happened: a write, a delete, a trim, etc.</summary>
    [Key(3)]
    public WatchOperationType Operation { get; init; }

    /// <summary>For a write, the first batch index this notification covers (inclusive); null when not applicable.</summary>
    [Key(4)]
    [MessagePackFormatter(typeof(NullableUInt64AsInt64Formatter))]
    public long? FromAggregateVersion { get; init; }

    /// <summary>For a write, the last batch index this notification covers (inclusive); null when not applicable.</summary>
    [Key(5)]
    [MessagePackFormatter(typeof(NullableUInt64AsInt64Formatter))]
    public long? ToAggregateVersion { get; init; }

    /// <summary>For a trim, the new earliest retained batch index; null when not applicable.</summary>
    [Key(6)]
    [MessagePackFormatter(typeof(NullableUInt64AsInt64Formatter))]
    public long? KeepFromAggregateVersion { get; init; }
}
