using System.Collections.Concurrent;

namespace Celeriant.Reference;

/// <summary>
/// A cached write outcome for a single aggregate, keyed by <c>(eventId, aggregateId)</c>.
/// </summary>
public readonly record struct IdempotencyEntry(long BalanceCents, long AggregateVersion);

/// <summary>
/// Per-instance idempotency cache keyed by <c>(eventId, aggregateId)</c> with a 90-second TTL
/// and lazy eviction.
///
/// <para>Populated from two sources:</para>
/// <list type="bullet">
///   <item>the write path after a successful write — in-instance retries hit immediately;</item>
///   <item><see cref="AccountService.CatchUpAsync"/>, which reconstructs the outcome for any replayed
///   event that carries an <c>event_id</c>. This warms cold instances after a BFF crash so a retried
///   <c>Idempotency-Key</c> can be resolved without re-writing.</item>
/// </list>
///
/// <para>
/// Not a correctness layer. The server's <c>enforce_client_idempotency</c> (CEI), keyed by
/// <c>(client_id, aggregate_key, client_seq)</c>, is the underlying dedup. This cache only shortens
/// the cross-instance recovery path for the BFF-crash-after-fsync case.
/// </para>
/// </summary>
public sealed class IdempotencyCache
{
    private static readonly TimeSpan Ttl = TimeSpan.FromSeconds(90);

    private readonly ConcurrentDictionary<(Guid EventId, Guid AggregateId), (IdempotencyEntry Entry, DateTimeOffset ExpiresAt)> _cache = new();

    /// <summary>How recent a replayed event must be (by server timestamp) for catch-up to warm the cache.</summary>
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

    private void Evict()
    {
        var now = DateTimeOffset.UtcNow;
        foreach (var kvp in _cache)
        {
            if (kvp.Value.ExpiresAt <= now)
                _cache.TryRemove(kvp.Key, out _);
        }
    }
}
