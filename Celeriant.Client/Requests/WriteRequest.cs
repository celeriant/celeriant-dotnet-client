using MessagePack;
using MessagePack.Formatters;
using Celeriant.Client.Protocol;

namespace Celeriant.Client.Requests;

/// <summary>
/// A write of one or more aggregates. A multi-aggregate write is atomic, and all its aggregates must
/// live on the same shard. Prefer the <c>WriteAsync(key, events, clientId, …)</c> convenience overload
/// for the common single-aggregate case.
/// </summary>
[MessagePackObject]
public sealed record WriteRequest
{
    /// <summary>Optional application-supplied id echoed back on the response. Keep it unique per
    /// request if you rely on it.</summary>
    [Key(0)]
    [MessagePackFormatter(typeof(CeleriantNullableGuidFormatter))]
    public Guid? CorrelationId { get; init; }

    /// <summary>Identifies the logical writer, and scopes client-seq idempotency. Use a stable id per
    /// writer — never a fresh <see cref="Guid.NewGuid"/> per call, or idempotency silently stops
    /// working. Under client-identity enforcement this must equal your derived identity
    /// (<c>CeleriantCrypto.GenerateClientIdentity</c> or the value <c>IdentifyAsync</c> returns).</summary>
    [Key(1)]
    [MessagePackFormatter(typeof(CeleriantGuidFormatter))]
    public required Guid ClientId { get; init; }

    /// <summary>Optional id of the end user on whose behalf the write is made, recorded with the events.</summary>
    [Key(2)]
    [MessagePackFormatter(typeof(CeleriantNullableGuidFormatter))]
    public Guid? UserId { get; init; }

    /// <summary>The per-aggregate writes, keyed by aggregate. All keys must be on the same shard; the
    /// whole set commits atomically.</summary>
    [Key(3)]
    public required Dictionary<AggregateKey, SingleAggregateWrite> Writes { get; init; }
}
