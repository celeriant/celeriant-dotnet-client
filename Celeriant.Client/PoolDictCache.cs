namespace Celeriant.Client;

/// <summary>
/// Pool-level, content-addressed compression-dictionary cache shared across all node pools.
///
/// <para>
/// New connections advertise <see cref="LastSha"/> as <c>KnownDictSha256</c> during Identify so the
/// server can skip re-sending the (~14&#160;KiB) dictionary bytes when they already match. When the
/// server confirms a sha without resending bytes, <see cref="DictForSha"/> resolves them from here.
/// </para>
///
/// <para>Thread-safe.</para>
/// </summary>
internal sealed class PoolDictCache
{
    private readonly object _lock = new();
    private readonly Dictionary<string, byte[]> _cache = new();
    private string? _lastSha;

    /// <summary>Most recently confirmed dictionary sha for this pool's cluster, or null.</summary>
    public string? LastSha
    {
        get { lock (_lock) return _lastSha; }
    }

    /// <summary>Returns the cached dictionary bytes for <paramref name="sha"/>, or null if not cached.</summary>
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
