using MessagePack;
using MessagePack.Formatters;
using Celeriant.Client.Protocol;

namespace Celeriant.Client.Requests;

/// <summary>Reads one page of an aggregate's events. For automatic pagination prefer
/// <c>CeleriantPool.ReadAllAsync</c>.</summary>
[MessagePackObject]
public sealed record ReadRequest
{
    /// <summary>Optional application-supplied id echoed back on the response.</summary>
    [Key(0)]
    [MessagePackFormatter(typeof(CeleriantNullableGuidFormatter))]
    public Guid? CorrelationId { get; init; }

    /// <summary>The aggregate to read.</summary>
    [Key(1)]
    public required AggregateKey AggregateKey { get; init; }

    /// <summary>Which events to return. Use <see cref="ReadFilters.From"/> to start at a batch index,
    /// or <c>new ReadFilters { … }</c> for the fuller filter set; a start below 1 reads from the
    /// beginning.</summary>
    [Key(2)]
    public required ReadFilters Filters { get; init; }
}
