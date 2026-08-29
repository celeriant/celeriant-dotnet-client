using MessagePack;
using MessagePack.Formatters;
using Celeriant.Client.Protocol;
using Celeriant.Client.Responses;

namespace Celeriant.Client.Requests;

/// <summary>
/// Write payload for a single aggregate within a <see cref="WriteRequest"/>.
/// </summary>
[MessagePackObject]
public sealed class SingleAggregateWrite
{
    /// <summary>The events to append, in order. Each needs a distinct increasing <c>ClientSeq</c>
    /// within this write (see <see cref="AggregateEvent"/>).</summary>
    [Key(0)]
    public required AggregateEvent[] Events { get; init; }

    /// <summary>Create the aggregate if it does not exist. When false, a write to a missing aggregate
    /// throws <c>AggregateNotFoundException</c>.</summary>
    [Key(1)]
    public bool AllowCreate { get; init; }

    /// <summary>Optimistic concurrency guard: when set, the write is rejected with
    /// <c>WriteOccException</c> unless the aggregate's current version (its
    /// <see cref="Responses.WriteResponse.MaxAggregateVersion"/>) equals this. Use <c>0</c> to require
    /// that the aggregate does not yet exist. Null skips the check.</summary>
    [Key(2)]
    [MessagePackFormatter(typeof(NullableUInt64AsInt64Formatter))]
    public long? ExpectedVersion { get; init; }

    /// <summary>When true, the server rejects a duplicate write sharing this write's <c>ClientId</c>
    /// and an already-accepted event <c>ClientSeq</c> with <c>IdempotencyViolationException</c>.</summary>
    [Key(3)]
    public bool EnforceClientIdempotency { get; init; }
}
