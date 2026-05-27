using System.Runtime.CompilerServices;
using Celeriant.Client.Errors;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;

namespace Celeriant.Client.Streaming;

/// <summary>
/// Extension methods on <see cref="CeleriantClient"/> that expose multi-shard list operations
/// as <see cref="IAsyncEnumerable{T}"/> streams.
///
/// <para>
/// Shard Discovery Algorithm:
/// <list type="number">
///   <item>Start at <see cref="ListOptions.StartShard"/> (default 0).</item>
///   <item>Send a page request to the current shard with cursor=null.</item>
///   <item>If the server returns error code 9001 or 9002, record the maximum shard index and stop expanding.</item>
///   <item>Yield items from the response; if a <c>next_cursor</c> is present, queue the next page for that shard.</item>
///   <item>Advance to the next shard (round-robin) until the shard ceiling is known and all queued pages are exhausted.</item>
///   <item>Deduplicate results by key to avoid emitting the same entity twice when shards overlap.</item>
/// </list>
/// </para>
/// </summary>
public static class ListExtensions
{
    // Shard routing error codes returned by the server when a shard index is out of range.
    private const uint ShardRoutingError1 = ErrorResponse.ShardRoutingMultipleShards;
    private const uint ShardRoutingError2 = ErrorResponse.ShardRoutingIncompatibleFilters;

    // -------------------------------------------------------------------------
    // ListOrgsAsync
    // -------------------------------------------------------------------------

    /// <summary>
    /// Stream all organisations visible on this connection, spanning all shards.
    /// </summary>
    public static IAsyncEnumerable<OrgListItem> ListOrgsAsync(
        this CeleriantClient client,
        ListOptions? options = null,
        CancellationToken ct = default)
        => ListOrgsAsyncCore(client, options ?? new ListOptions(), ct);

    private static async IAsyncEnumerable<OrgListItem> ListOrgsAsyncCore(
        CeleriantClient client,
        ListOptions options,
        [EnumeratorCancellation] CancellationToken ct)
    {
        var seen = new HashSet<Guid>(); // deduplicate by org_id
        var pendingPages = new Queue<(long shardId, long? cursor)>();

        long nextShardToProbe = options.StartShard;
        long? maxShard = options.MaxShardHint;

        // Seed: probe the first shard
        bool hasMoreShards = true;

        // We interleave probing new shards with draining queued next-page requests.
        // Each iteration: pick one item (either a queued next-page or a new shard probe).
        while (hasMoreShards || pendingPages.Count > 0)
        {
            ct.ThrowIfCancellationRequested();

            long shardId;
            long? cursor;

            if (pendingPages.TryDequeue(out var pending))
            {
                // Drain a queued continuation page first (round-robin friendly).
                (shardId, cursor) = pending;
            }
            else if (hasMoreShards && (maxShard is null || nextShardToProbe < maxShard.Value))
            {
                shardId = nextShardToProbe++;
                cursor = null;
            }
            else
            {
                // No queued pages and no more shards to probe.
                break;
            }

            var request = new ClientRequest.ListOrgs(new ListOrgsRequest
            {
                ShardId = shardId,
                Cursor = cursor,
            });

            ClientResponse response;
            try
            {
                response = await client.SendRequestAsync(request, ct)
                    .ConfigureAwait(false);
            }
            catch (CeleriantErrorException ex) when (ex.Error.ErrorCode is ShardRoutingError1 or ShardRoutingError2)
            {
                hasMoreShards = false;
                continue;
            }

            if (response is not ClientResponse.ListOrgs listResponse)
                throw new ProtocolException($"Unexpected response type {response.GetType().Name} for ListOrgs.");

            foreach (OrgListItem item in listResponse.Value.Orgs)
            {
                if (seen.Add(item.OrgId))
                    yield return item;
            }

            if (listResponse.Value.NextCursor.HasValue)
                pendingPages.Enqueue((shardId, listResponse.Value.NextCursor.Value));

            // If maxShard was not pre-specified and we probed a new shard successfully,
            // continue expanding (hasMoreShards remains true).
        }
    }

    // -------------------------------------------------------------------------
    // ListAggregateTypesAsync
    // -------------------------------------------------------------------------

    /// <summary>
    /// Stream all aggregate types visible on this connection, spanning all shards.
    /// </summary>
    public static IAsyncEnumerable<AggregateTypeListItem> ListAggregateTypesAsync(
        this CeleriantClient client,
        Guid? orgId = null,
        ListOptions? options = null,
        CancellationToken ct = default)
        => ListAggregateTypesAsyncCore(client, orgId, options ?? new ListOptions(), ct);

    private static async IAsyncEnumerable<AggregateTypeListItem> ListAggregateTypesAsyncCore(
        CeleriantClient client,
        Guid? orgId,
        ListOptions options,
        [EnumeratorCancellation] CancellationToken ct)
    {
        // Deduplicate by (org_id, aggregate_type_id).
        var seen = new HashSet<(Guid, Guid)>();
        var pendingPages = new Queue<(long shardId, long? cursor)>();

        long nextShardToProbe = options.StartShard;
        long? maxShard = options.MaxShardHint;
        bool hasMoreShards = true;

        while (hasMoreShards || pendingPages.Count > 0)
        {
            ct.ThrowIfCancellationRequested();

            long shardId;
            long? cursor;

            if (pendingPages.TryDequeue(out var pending))
            {
                (shardId, cursor) = pending;
            }
            else if (hasMoreShards && (maxShard is null || nextShardToProbe < maxShard.Value))
            {
                shardId = nextShardToProbe++;
                cursor = null;
            }
            else
            {
                break;
            }

            var request = new ClientRequest.ListAggregateTypes(new ListAggregateTypesRequest
            {
                ShardId = shardId,
                OrgId = orgId,
                Cursor = cursor,
            });

            ClientResponse response;
            try
            {
                response = await client.SendRequestAsync(request, ct)
                    .ConfigureAwait(false);
            }
            catch (CeleriantErrorException ex) when (ex.Error.ErrorCode is ShardRoutingError1 or ShardRoutingError2)
            {
                hasMoreShards = false;
                continue;
            }

            if (response is not ClientResponse.ListAggregateTypes listResponse)
                throw new ProtocolException($"Unexpected response type {response.GetType().Name} for ListAggregateTypes.");

            foreach (AggregateTypeListItem item in listResponse.Value.AggregateTypes)
            {
                if (seen.Add((item.OrgId, item.AggregateTypeId)))
                    yield return item;
            }

            if (listResponse.Value.NextCursor.HasValue)
                pendingPages.Enqueue((shardId, listResponse.Value.NextCursor.Value));
        }
    }

    // -------------------------------------------------------------------------
    // ListAggregatesAsync
    // -------------------------------------------------------------------------

    /// <summary>
    /// Stream aggregates with merged statistics across all shards.
    ///
    /// When the same aggregate appears on multiple shards (because data is spread across
    /// shards), statistics are merged: counts and sizes are summed, min/max timestamps and
    /// indices are computed across all shards, and <see cref="AggregateStats.IsDeleted"/> is
    /// set to true if any shard reports the aggregate as deleted.
    /// </summary>
    public static IAsyncEnumerable<AggregateStats> ListAggregatesAsync(
        this CeleriantClient client,
        Guid? orgId = null,
        Guid? aggregateTypeId = null,
        ListOptions? options = null,
        CancellationToken ct = default)
        => ListAggregatesAsyncCore(client, orgId, aggregateTypeId, options ?? new ListOptions(), ct);

    private static async IAsyncEnumerable<AggregateStats> ListAggregatesAsyncCore(
        CeleriantClient client,
        Guid? orgId,
        Guid? aggregateTypeId,
        ListOptions options,
        [EnumeratorCancellation] CancellationToken ct)
    {
        // Because the same aggregate can appear on multiple shards (and stats must be merged
        // across all shards before yielding), we must fully accumulate before yielding.
        // Key: aggregate_id (unique per (org, type, aggregate) triple — but we also key on
        // the full triple in case the caller passes no org/type filter and IDs collide).
        var accumulated = new Dictionary<(Guid orgId, Guid typeId, Guid aggId), AggregateStats>();
        var pendingPages = new Queue<(long shardId, long? cursor)>();

        long nextShardToProbe = options.StartShard;
        long? maxShard = options.MaxShardHint;
        bool hasMoreShards = true;

        while (hasMoreShards || pendingPages.Count > 0)
        {
            ct.ThrowIfCancellationRequested();

            long shardId;
            long? cursor;

            if (pendingPages.TryDequeue(out var pending))
            {
                (shardId, cursor) = pending;
            }
            else if (hasMoreShards && (maxShard is null || nextShardToProbe < maxShard.Value))
            {
                shardId = nextShardToProbe++;
                cursor = null;
            }
            else
            {
                break;
            }

            var request = new ClientRequest.ListAggregates(new ListAggregatesRequest
            {
                ShardId = shardId,
                OrgId = orgId,
                AggregateTypeId = aggregateTypeId,
                Cursor = cursor,
            });

            ClientResponse response;
            try
            {
                response = await client.SendRequestAsync(request, ct)
                    .ConfigureAwait(false);
            }
            catch (CeleriantErrorException ex) when (ex.Error.ErrorCode is ShardRoutingError1 or ShardRoutingError2)
            {
                hasMoreShards = false;
                continue;
            }

            if (response is not ClientResponse.ListAggregates listResponse)
                throw new ProtocolException($"Unexpected response type {response.GetType().Name} for ListAggregates.");

            foreach (AggregateListItem item in listResponse.Value.Aggregates)
            {
                if (!options.IncludeDeleted && item.IsDeleted)
                    continue;

                var key = (item.OrgId, item.AggregateTypeId, item.AggregateId);
                if (accumulated.TryGetValue(key, out AggregateStats? existing))
                {
                    MergeInto(existing, item);
                }
                else
                {
                    accumulated[key] = FromListItem(item);
                }
            }

            if (listResponse.Value.NextCursor.HasValue)
                pendingPages.Enqueue((shardId, listResponse.Value.NextCursor.Value));
        }

        // Yield all accumulated stats after full traversal.
        foreach (AggregateStats stats in accumulated.Values)
            yield return stats;
    }

    // -------------------------------------------------------------------------
    // AggregateStats helpers
    // -------------------------------------------------------------------------

    private static AggregateStats FromListItem(AggregateListItem item) => new AggregateStats
    {
        OrgId = item.OrgId,
        AggregateTypeId = item.AggregateTypeId,
        AggregateId = item.AggregateId,
        IsDeleted = item.IsDeleted,
        EventBatchCount = item.EventBatchCount,
        MinEventTimestamp = item.MinEventTimestamp,
        MaxEventTimestamp = item.MaxEventTimestamp,
        MinServerTimestamp = item.MinServerTimestamp,
        MaxServerTimestamp = item.MaxServerTimestamp,
        MinAggregateVersion = item.MinAggregateVersion,
        MaxAggregateVersion = item.MaxAggregateVersion,
        MinEventSeq = item.MinEventSeq,
        MaxEventSeq = item.MaxEventSeq,
        CompressedSize = item.CompressedSize,
        UncompressedSize = item.UncompressedSize,
    };

    /// <summary>
    /// Merge <paramref name="incoming"/> stats into an existing <paramref name="target"/>
    /// using the rules defined in the design spec.
    /// </summary>
    private static void MergeInto(AggregateStats target, AggregateListItem incoming)
    {
        // IsDeleted: OR
        target.IsDeleted |= incoming.IsDeleted;

        // Counts/sizes: sum
        target.EventBatchCount += incoming.EventBatchCount;
        target.CompressedSize += incoming.CompressedSize;
        target.UncompressedSize += incoming.UncompressedSize;

        // Min timestamps: min of non-null values
        target.MinEventTimestamp = MinNullable(target.MinEventTimestamp, incoming.MinEventTimestamp);
        target.MinServerTimestamp = MinNullable(target.MinServerTimestamp, incoming.MinServerTimestamp);

        // Max timestamps: max of non-null values
        target.MaxEventTimestamp = MaxNullable(target.MaxEventTimestamp, incoming.MaxEventTimestamp);
        target.MaxServerTimestamp = MaxNullable(target.MaxServerTimestamp, incoming.MaxServerTimestamp);

        // Min indices: min, but treat 0 as "no data" (skip)
        target.MinAggregateVersion = MinNonZero(target.MinAggregateVersion, incoming.MinAggregateVersion);
        target.MinEventSeq = MinNonZero(target.MinEventSeq, incoming.MinEventSeq);

        // Max indices: max
        target.MaxAggregateVersion = Math.Max(target.MaxAggregateVersion, incoming.MaxAggregateVersion);
        target.MaxEventSeq = Math.Max(target.MaxEventSeq, incoming.MaxEventSeq);
    }

    private static DateTimeOffset? MinNullable(DateTimeOffset? a, DateTimeOffset? b)
    {
        if (a is null) return b;
        if (b is null) return a;
        return a.Value < b.Value ? a : b;
    }

    private static DateTimeOffset? MaxNullable(DateTimeOffset? a, DateTimeOffset? b)
    {
        if (a is null) return b;
        if (b is null) return a;
        return a.Value > b.Value ? a : b;
    }

    /// <summary>
    /// Returns the minimum of two values, treating 0 as "no data" (i.e., skip 0).
    /// If both are 0, returns 0.
    /// </summary>
    private static long MinNonZero(long a, long b)
    {
        if (a == 0) return b;
        if (b == 0) return a;
        return Math.Min(a, b);
    }
}
