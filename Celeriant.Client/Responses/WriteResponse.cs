using MessagePack;
using MessagePack.Formatters;
using Celeriant.Client.Protocol;

namespace Celeriant.Client.Responses;

/// <summary>The result of a write.</summary>
[MessagePackObject]
public sealed class WriteResponse
{
    /// <summary>The correlation id echoed from the request, if one was set.</summary>
    [Key(0)]
    [MessagePackFormatter(typeof(CeleriantNullableGuidFormatter))]
    public Guid? CorrelationId { get; init; }

    /// <summary>
    /// The aggregate's version after this write — the "aggregate version" (a.k.a. event batch index):
    /// the same number you pass as <c>expectedVersion</c> to guard the next write, and that
    /// <see cref="AggregateDetailsResponse.MaxAggregateVersion"/> and
    /// <see cref="AggregateEventBatch.AggregateVersion"/> report. Populated only when the request wrote
    /// exactly one aggregate; <c>null</c> for a multi-aggregate write.
    /// </summary>
    [Key(1)]
    [MessagePackFormatter(typeof(NullableUInt64AsInt64Formatter))]
    public long? MaxAggregateVersion { get; init; }
}
