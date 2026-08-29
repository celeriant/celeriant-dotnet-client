using MessagePack;
using MessagePack.Formatters;
using Celeriant.Client.Protocol;

namespace Celeriant.Client.Responses;

/// <summary>
/// One page of an aggregate read. For automatic pagination prefer <c>CeleriantPool.ReadAllAsync</c>,
/// which follows <see cref="NextAggregateVersion"/> for you.
/// </summary>
[MessagePackObject]
public sealed class ReadResponse
{
    /// <summary>The correlation id echoed from the request, if one was set.</summary>
    [Key(0)]
    [MessagePackFormatter(typeof(CeleriantNullableGuidFormatter))]
    public Guid? CorrelationId { get; init; }

    /// <summary>The batches in this page, oldest first — a materialised array (use <c>.Length</c> and
    /// the indexer; it is not a lazy sequence). The server bounds page size, so a large aggregate
    /// spans several pages; see <see cref="NextAggregateVersion"/>.</summary>
    [Key(1)]
    public AggregateEventBatch[] EventBatches { get; init; } = [];

    /// <summary>The batch index to request next when the read was paginated: pass it as
    /// <c>ReadFilters.FromAggregateVersion</c> (it is exclusive of the last batch in this page, so no
    /// batch is repeated or skipped). <c>null</c> means this page reached the end of the aggregate —
    /// stop.</summary>
    [Key(2)]
    [MessagePackFormatter(typeof(NullableUInt64AsInt64Formatter))]
    public long? NextAggregateVersion { get; init; }
}
