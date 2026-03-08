using System.ComponentModel;
using Celeriant.Client.Protocol;

namespace Celeriant.Client.Streaming;

/// <summary>
/// Options controlling shard routing and compression for list operations.
/// </summary>
public sealed class ListOptions
{
    /// <summary>Compression to apply when fetching list pages. Defaults to <see cref="CompressionType.None"/>.</summary>
    public CompressionType Compression { get; init; } = CompressionType.None;

    /// <summary>
    /// When true, include deleted aggregates in results. Only meaningful for
    /// <see cref="ListExtensions.ListAggregatesAsync"/>.
    /// </summary>
    public bool IncludeDeleted { get; init; }

    /// <summary>The shard index at which to start iteration. Defaults to 0.</summary>
    [EditorBrowsable(EditorBrowsableState.Advanced)]
    public long StartShard { get; init; }

    /// <summary>
    /// If set, the client skips shard discovery and iterates shards 0..MaxShardHint (exclusive).
    /// If null, the client probes by incrementing the shard index until the server returns a
    /// shard routing error (9001 or 9002).
    /// </summary>
    [EditorBrowsable(EditorBrowsableState.Advanced)]
    public long? MaxShardHint { get; init; }
}
