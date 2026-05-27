namespace Celeriant.Client.Streaming;

/// <summary>
/// Merged statistics for a single aggregate, accumulated across multiple shards.
///
/// For a single-shard result the values are taken directly from the server response.
/// When the same aggregate appears on multiple shards the fields are merged as follows:
/// <list type="bullet">
///   <item><description><see cref="IsDeleted"/> - OR: true if ANY shard reports deleted.</description></item>
///   <item><description><see cref="EventBatchCount"/>, <see cref="CompressedSize"/>, <see cref="UncompressedSize"/> - summed.</description></item>
///   <item><description>Min timestamps/indices - minimum of non-zero values (0 means "no data", skipped).</description></item>
///   <item><description>Max timestamps/indices - maximum.</description></item>
/// </list>
/// </summary>
public sealed class AggregateStats
{
    public Guid OrgId { get; init; }
    public Guid AggregateTypeId { get; init; }
    public Guid AggregateId { get; init; }

    /// <summary>True if ANY shard reports this aggregate as deleted.</summary>
    public bool IsDeleted { get; internal set; }

    /// <summary>Total event batch count across all shards.</summary>
    public long EventBatchCount { get; internal set; }

    /// <summary>Minimum event timestamp across shards. Null means no data.</summary>
    public DateTimeOffset? MinEventTimestamp { get; internal set; }

    /// <summary>Maximum event timestamp across shards.</summary>
    public DateTimeOffset? MaxEventTimestamp { get; internal set; }

    /// <summary>Minimum server timestamp across shards. Null means no data.</summary>
    public DateTimeOffset? MinServerTimestamp { get; internal set; }

    /// <summary>Maximum server timestamp across shards.</summary>
    public DateTimeOffset? MaxServerTimestamp { get; internal set; }

    /// <summary>Minimum event batch index across shards. 0 means no data.</summary>
    public long MinAggregateVersion { get; internal set; }

    /// <summary>Maximum event batch index across shards.</summary>
    public long MaxAggregateVersion { get; internal set; }

    /// <summary>Minimum event index across shards. 0 means no data.</summary>
    public long MinEventSeq { get; internal set; }

    /// <summary>Maximum event index across shards.</summary>
    public long MaxEventSeq { get; internal set; }

    /// <summary>Total compressed size in bytes across all shards.</summary>
    public long CompressedSize { get; internal set; }

    /// <summary>Total uncompressed size in bytes across all shards.</summary>
    public long UncompressedSize { get; internal set; }
}
