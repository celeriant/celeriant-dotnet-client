using MessagePack;
using MessagePack.Formatters;
using Celeriant.Client.Protocol;

namespace Celeriant.Client.Responses;

/// <summary>Metadata about an aggregate — its current range and status — without reading its events.</summary>
[MessagePackObject]
public sealed class AggregateDetailsResponse
{
    /// <summary>The correlation id echoed from the request, if one was set.</summary>
    [Key(0)]
    [MessagePackFormatter(typeof(CeleriantNullableGuidFormatter))]
    public Guid? CorrelationId { get; init; }

    /// <summary>The earliest batch index still available (moves forward after a trim).</summary>
    [Key(1)]
    [MessagePackFormatter(typeof(UInt64AsInt64Formatter))]
    public long MinAggregateVersion { get; init; }

    /// <summary>The aggregate's current version — the "aggregate version" (a.k.a. batch index), the
    /// same number <c>expectedVersion</c> guards against and <see cref="WriteResponse.MaxAggregateVersion"/>
    /// reports.</summary>
    [Key(2)]
    [MessagePackFormatter(typeof(UInt64AsInt64Formatter))]
    public long MaxAggregateVersion { get; init; }

    /// <summary>The highest per-event sequence number written so far.</summary>
    [Key(3)]
    [MessagePackFormatter(typeof(UInt64AsInt64Formatter))]
    public long MaxEventSeq { get; init; }

    /// <summary>True if the aggregate has been deleted.</summary>
    [Key(4)]
    public bool IsDeleted { get; init; }

    /// <summary>Whether a deleted aggregate may be recreated by a new write.</summary>
    [Key(5)]
    public bool AllowRecreate { get; init; }

    /// <summary>Whether a recreated aggregate continues the prior client-seq sequence rather than restarting it.</summary>
    [Key(6)]
    public bool AllowSequenceContinuation { get; init; }

    /// <summary>Server timestamp of the most recent write.</summary>
    [Key(7)]
    [MessagePackFormatter(typeof(EpochMillisFormatter))]
    public DateTimeOffset LastServerTimestamp { get; init; }

    /// <summary>Client id of the most recent write.</summary>
    [Key(8)]
    [MessagePackFormatter(typeof(CeleriantGuidFormatter))]
    public Guid LastClientId { get; init; }

    /// <summary>User id of the most recent write, if any.</summary>
    [Key(9)]
    [MessagePackFormatter(typeof(CeleriantNullableGuidFormatter))]
    public Guid? LastUserId { get; init; }
}
