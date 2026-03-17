using System.Collections.Concurrent;

namespace Celeriant.Reference;

/// <summary>
/// In-memory HTTP-level idempotency cache with 90-second TTL and lazy eviction.
/// Per-instance, non-durable — a convenience layer on top of Celeriant's infrastructure-level
/// ClientEventIndex deduplication. See FAILURE-ANALYSIS.md B-FAIL/N-FAIL for the gap analysis.
/// </summary>
public sealed class IdempotencyCache
{
    private static readonly TimeSpan Ttl = TimeSpan.FromSeconds(90);

    private readonly ConcurrentDictionary<Guid, (object Result, DateTimeOffset ExpiresAt)> _cache = new();

    public bool TryGet(Guid key, out object? result)
    {
        Evict();
        if (_cache.TryGetValue(key, out var entry) && entry.ExpiresAt > DateTimeOffset.UtcNow)
        {
            result = entry.Result;
            return true;
        }

        result = null;
        return false;
    }

    public void Set(Guid key, object result)
    {
        _cache[key] = (result, DateTimeOffset.UtcNow + Ttl);
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
