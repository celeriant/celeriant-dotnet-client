namespace Celeriant.Transport;

/// <summary>
/// The dictionaries a pool resolves without the server shipping them: the built-in every client
/// bundles, and the one cluster dictionary learned last. Bounded at those two: learning a new
/// dictionary replaces the previous one. Shared by every connection of a pool, so the pool learns a
/// cluster dictionary once. Thread-safe.
///
/// <para>
/// A connection advertises <see cref="Snapshot"/> in Identify and resolves the server's sha-only
/// confirm against that snapshot with <see cref="Resolve"/>, never against the shared slot: another
/// connection may learn a different dictionary, evicting the advertised one, before the confirm
/// arrives. Mirrors the Rust client's <c>dict_cache.rs</c>.
/// </para>
/// </summary>
public sealed class DictCache
{
    private readonly object _learnLock = new();

    // Replaced whole, never mutated, so a reader sees a consistent (sha, bytes) pair without a lock.
    private volatile CachedDict _learned = BuiltinDictionary.Dict;

    /// <summary>The dictionary to advertise in Identify: the last learned, or the built-in.</summary>
    public CachedDict Snapshot() => _learned;

    /// <summary>The bytes for <paramref name="sha"/> if it is the learned dictionary or the built-in, else null.</summary>
    public byte[]? Lookup(string sha) => Resolve(_learned, sha);

    /// <summary>
    /// Make <paramref name="dict"/> the learned dictionary. Relearning the sha already held keeps the
    /// first instance, so every connection shares one copy of the bytes.
    /// </summary>
    public void Learn(CachedDict dict)
    {
        lock (_learnLock)
        {
            if (_learned.Sha != dict.Sha)
                _learned = dict;
        }
    }

    /// <summary>The bytes for <paramref name="sha"/> if it is <paramref name="advertised"/> or the built-in, else null.</summary>
    public static byte[]? Resolve(CachedDict advertised, string sha)
    {
        if (advertised.Sha == sha)
            return advertised.Bytes;
        CachedDict builtin = BuiltinDictionary.Dict;
        return builtin.Sha == sha ? builtin.Bytes : null;
    }
}
