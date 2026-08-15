namespace Celeriant.Transport;

/// <summary>
/// Pool-wide, content-addressed compression-dictionary cache shared across all node pools. New
/// connections advertise <see cref="LastSha"/> as <c>known_dict_sha256</c> during Identify so the
/// server can skip re-shipping the (~14&#160;KiB) dictionary bytes when they already match; a
/// sha-only confirmation is resolved through <see cref="DictForSha"/>. Thread-safe.
/// </summary>
public sealed class DictCache
{
    private readonly object _lock = new();
    private readonly Dictionary<string, byte[]> _cache = new();
    private string? _lastSha;

    /// <summary>Most recently negotiated dictionary sha for this cluster, or null.</summary>
    public string? LastSha
    {
        get { lock (_lock) return _lastSha; }
    }

    /// <summary>Cached dictionary bytes for <paramref name="sha"/>, or null if not cached.</summary>
    public byte[]? DictForSha(string sha)
    {
        lock (_lock)
            return _cache.TryGetValue(sha, out var bytes) ? bytes : null;
    }

    /// <summary>
    /// Record <paramref name="bytes"/> under <paramref name="sha"/> and mark it the last-known
    /// cluster dictionary. Content-addressed: re-inserting an existing sha keeps the original bytes.
    /// </summary>
    public void CacheDict(string sha, byte[] bytes)
    {
        lock (_lock)
        {
            _cache.TryAdd(sha, bytes);
            _lastSha = sha;
        }
    }
}
