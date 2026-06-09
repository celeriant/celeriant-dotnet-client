using System.Collections.Concurrent;

namespace Celeriant.Reference;

/// <summary>
/// A cached write outcome for a single aggregate, keyed by <c>(eventId, aggregateId)</c>.
/// </summary>
public readonly record struct IdempotencyEntry(long BalanceCents, long AggregateVersion);

/// <summary>
/// Per-instance cache with a 90-second TTL and lazy eviction. Two maps:
///
/// <para><c>(eventId, aggregateId) -> outcome</c> restores the lost response for a retried
/// request. Not a correctness layer; server-side CEI is the dedup.</para>
///
/// <para><c>(aggregateId, clientSeq) -> eventId</c> IS load-bearing. Requests share the
/// service's client id, so two can derive the same clientSeq; if the loser's OCC rejection
/// is lost to a timeout, its retry gets an IdempotencyViolation that refers to the sibling's
/// event. This map is how the violation arm tells "mine" from "theirs" before claiming
/// success. Warmed during catch-up replay and on every successful write.</para>
/// </summary>
public sealed class IdempotencyCache
{
    private static readonly TimeSpan Ttl = TimeSpan.FromSeconds(90);

    private readonly ConcurrentDictionary<(Guid EventId, Guid AggregateId), (IdempotencyEntry Entry, DateTimeOffset ExpiresAt)> _cache = new();
    private readonly ConcurrentDictionary<(Guid AggregateId, long ClientSeq), (Guid EventId, DateTimeOffset ExpiresAt)> _seqOwners = new();

    /// <summary>How recent a replayed event must be (relative to the tip of the read, in server time) for catch-up to warm the cache.</summary>
    public static TimeSpan WarmWindow => Ttl;

    public bool TryGet(Guid eventId, Guid aggregateId, out IdempotencyEntry entry)
    {
        Evict();
        if (_cache.TryGetValue((eventId, aggregateId), out var hit) && hit.ExpiresAt > DateTimeOffset.UtcNow)
        {
            entry = hit.Entry;
            return true;
        }

        entry = default;
        return false;
    }

    public void Set(Guid eventId, Guid aggregateId, IdempotencyEntry entry)
    {
        _cache[(eventId, aggregateId)] = (entry, DateTimeOffset.UtcNow + Ttl);
    }

    /// <summary>Which eventId landed on this (aggregate, clientSeq)? Null = unknown.</summary>
    public Guid? SeqOwner(Guid aggregateId, long clientSeq)
    {
        Evict();
        if (_seqOwners.TryGetValue((aggregateId, clientSeq), out var hit) && hit.ExpiresAt > DateTimeOffset.UtcNow)
            return hit.EventId;
        return null;
    }

    public void SetSeqOwner(Guid aggregateId, long clientSeq, Guid eventId)
    {
        _seqOwners[(aggregateId, clientSeq)] = (eventId, DateTimeOffset.UtcNow + Ttl);
    }

    private void Evict()
    {
        var now = DateTimeOffset.UtcNow;
        foreach (var kvp in _cache)
        {
            if (kvp.Value.ExpiresAt <= now)
                _cache.TryRemove(kvp.Key, out _);
        }
        foreach (var kvp in _seqOwners)
        {
            if (kvp.Value.ExpiresAt <= now)
                _seqOwners.TryRemove(kvp.Key, out _);
        }
    }
}
