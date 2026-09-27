using System.Security.Cryptography;

namespace Celeriant.Transport.Tests;

/// <summary>
/// The pool dictionary cache contract: the built-in json-web-events-v1 dictionary ships inside the assembly byte-identical to
/// the Rust copy, every cache is seeded with it, and a cache holds the built-in plus at most one
/// learned dictionary. Ports the intent of the Rust <c>dict_cache.rs</c> unit tests.
///
/// Run: dotnet test Celeriant.Transport.Tests --filter FullyQualifiedName~DictCacheContractTests
/// </summary>
public class DictCacheContractTests
{
    /// <summary>
    /// The server's copy the embedded resource must match, in a <c>celeriant-db</c> checkout beside
    /// this repo. The byte comparison skips without one; the pinned sha below still runs.
    /// </summary>
    private static readonly string? RustBuiltinPath = FindRustBuiltin();

    private static string? FindRustBuiltin()
    {
        for (var dir = new DirectoryInfo(AppContext.BaseDirectory); dir is not null; dir = dir.Parent)
        {
            if (!File.Exists(Path.Combine(dir.FullName, "Celeriant.Client.sln")))
                continue;
            string candidate = Path.Combine(dir.Parent?.FullName ?? dir.FullName,
                "celeriant-db", "celeriant_wal", "dicts", "json_web_events_v1.zstd_dict");
            return File.Exists(candidate) ? candidate : null;
        }
        return null;
    }

    /// <summary>sha256 of the server's copy, pinned so the check runs without a checkout.</summary>
    private const string BuiltinSha = "bc33cc0cea46e28ca70d190c627f62ac8653e315f154cd7bbfd35cb3c01308e8";

    private const int BuiltinLength = 14_027;

    private static CachedDict Dict(string name)
    {
        byte[] bytes = System.Text.Encoding.UTF8.GetBytes(name);
        return new CachedDict(Sha(bytes), bytes);
    }

    private static string Sha(byte[] bytes) => Convert.ToHexString(SHA256.HashData(bytes)).ToLowerInvariant();

    // ---------------------------------------------------------------------------------------------
    // The bundled built-in
    // ---------------------------------------------------------------------------------------------

    [SkippableFact]
    public void TheEmbeddedBuiltinIsByteIdenticalToTheRustCopy()
    {
        Skip.If(RustBuiltinPath is null, "No celeriant-db checkout beside this repo to compare against.");

        byte[] rust = File.ReadAllBytes(RustBuiltinPath);

        Assert.Equal(rust.Length, BuiltinDictionary.Dict.Bytes.Length);
        Assert.True(rust.AsSpan().SequenceEqual(BuiltinDictionary.Dict.Bytes),
            "the embedded built-in dictionary differs from the Rust copy");
    }

    [Fact]
    public void TheBuiltinIsJsonWebEventsV1WithItsPinnedLengthAndSha()
    {
        CachedDict builtin = BuiltinDictionary.Dict;

        Assert.Equal("json-web-events-v1", BuiltinDictionary.Name);
        Assert.Equal(BuiltinLength, builtin.Bytes.Length);
        Assert.Equal(BuiltinSha, builtin.Sha);
        Assert.Equal(Sha(builtin.Bytes), builtin.Sha);
    }

    [Fact]
    public void TheBuiltinIsComputedOncePerProcess()
    {
        Assert.Same(BuiltinDictionary.Dict, BuiltinDictionary.Dict);
        Assert.Same(BuiltinDictionary.Dict.Bytes, BuiltinDictionary.Dict.Bytes);
    }

    // ---------------------------------------------------------------------------------------------
    // A fresh cache
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public void AFreshCacheAdvertisesAndResolvesTheBuiltin()
    {
        var cache = new DictCache();

        CachedDict snapshot = cache.Snapshot();
        Assert.Equal(BuiltinSha, snapshot.Sha);
        Assert.Equal(BuiltinDictionary.Dict.Bytes, snapshot.Bytes);
        Assert.Equal(BuiltinDictionary.Dict.Bytes, cache.Lookup(BuiltinSha));
        Assert.Null(cache.Lookup("unknown"));
        Assert.Null(cache.Lookup(Dict("never-learned").Sha));
    }

    // ---------------------------------------------------------------------------------------------
    // Learning
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public void ALearnedDictIsAdvertisedAndResolvedWithoutCopying()
    {
        var cache = new DictCache();
        CachedDict cluster = Dict("cluster");

        cache.Learn(cluster);

        Assert.Equal(cluster.Sha, cache.Snapshot().Sha);
        Assert.Same(cluster.Bytes, cache.Snapshot().Bytes);
        Assert.Same(cluster.Bytes, cache.Lookup(cluster.Sha));
        Assert.Equal(BuiltinDictionary.Dict.Bytes, cache.Lookup(BuiltinSha));
    }

    [Fact]
    public void LearningANewDictEvictsTheOldOneButNeverTheBuiltin()
    {
        var cache = new DictCache();
        CachedDict first = Dict("first");
        CachedDict second = Dict("second");

        cache.Learn(first);
        cache.Learn(second);

        Assert.Null(cache.Lookup(first.Sha));
        Assert.Same(second.Bytes, cache.Lookup(second.Sha));
        Assert.Equal(BuiltinDictionary.Dict.Bytes, cache.Lookup(BuiltinSha));
        Assert.Equal(second.Sha, cache.Snapshot().Sha);
    }

    [Fact]
    public void RelearningTheSameShaKeepsTheFirstBytes()
    {
        var cache = new DictCache();
        CachedDict first = Dict("cluster");
        CachedDict again = Dict("cluster");
        Assert.NotSame(first.Bytes, again.Bytes);

        cache.Learn(first);
        cache.Learn(again);

        Assert.Same(first.Bytes, cache.Lookup(first.Sha));
        Assert.Same(first.Bytes, cache.Snapshot().Bytes);
    }

    [Fact]
    public void LearningTheBuiltinKeepsItResolvable()
    {
        var cache = new DictCache();
        cache.Learn(Dict("cluster"));

        cache.Learn(BuiltinDictionary.Dict);

        Assert.Equal(BuiltinSha, cache.Snapshot().Sha);
        Assert.Equal(BuiltinDictionary.Dict.Bytes, cache.Lookup(BuiltinSha));
        Assert.Null(cache.Lookup(Dict("cluster").Sha));
    }

    /// <summary>After many distinct dictionaries the cache resolves exactly the built-in and the last one.</summary>
    [Fact]
    public void TheCacheStaysBoundedToTheBuiltinPlusOneLearnedOverManyDicts()
    {
        var cache = new DictCache();
        var learned = Enumerable.Range(0, 1000).Select(i => Dict($"dict-{i}")).ToArray();

        foreach (CachedDict dict in learned)
            cache.Learn(dict);

        var resolvable = learned.Where(d => cache.Lookup(d.Sha) is not null).Select(d => d.Sha).ToArray();
        Assert.Equal([learned[^1].Sha], resolvable);
        Assert.Equal(BuiltinDictionary.Dict.Bytes, cache.Lookup(BuiltinSha));
        Assert.Equal(learned[^1].Sha, cache.Snapshot().Sha);
    }

    // ---------------------------------------------------------------------------------------------
    // Resolve: a confirm resolves against the advertised snapshot, never the shared slot
    // ---------------------------------------------------------------------------------------------

    public static TheoryData<string, string, string?> ResolveRows()
    {
        return new()
        {
            // row name,                  sha asked for,        which bytes come back
            { "the advertised sha",       "advertised",         "advertised" },
            { "the built-in sha",         "builtin",            "builtin" },
            { "a sha learned elsewhere",  "other",              null },
            { "an unknown sha",           "unknown",            null },
            { "the empty string",         "",                   null },
        };
    }

    [Theory]
    [MemberData(nameof(ResolveRows))]
    public void ResolveReturnsOnlyTheAdvertisedOrTheBuiltin(string row, string ask, string? expect)
    {
        CachedDict advertised = Dict("advertised");
        CachedDict other = Dict("other");
        var cache = new DictCache();
        cache.Learn(other); // the shared slot holds something else: Resolve must not consult it

        string sha = ask switch
        {
            "advertised" => advertised.Sha,
            "builtin" => BuiltinSha,
            "other" => other.Sha,
            "unknown" => Dict("unknown").Sha,
            _ => ask,
        };
        byte[]? want = expect switch
        {
            "advertised" => advertised.Bytes,
            "builtin" => BuiltinDictionary.Dict.Bytes,
            _ => null,
        };

        byte[]? got = DictCache.Resolve(advertised, sha);

        if (want is null)
            Assert.True(got is null, $"row '{row}': expected null, got {got?.Length} bytes");
        else
            Assert.True(got is not null && got.AsSpan().SequenceEqual(want), $"row '{row}': wrong bytes");
    }

    [Fact]
    public void ResolveAgainstTheBuiltinSnapshotResolvesTheBuiltin()
    {
        Assert.Equal(BuiltinDictionary.Dict.Bytes, DictCache.Resolve(BuiltinDictionary.Dict, BuiltinSha));
        Assert.Null(DictCache.Resolve(BuiltinDictionary.Dict, Dict("x").Sha));
    }

    [Fact]
    public void ASnapshotSurvivesALaterLearn()
    {
        var cache = new DictCache();
        CachedDict x = Dict("x");
        cache.Learn(x);

        CachedDict advertised = cache.Snapshot();
        cache.Learn(Dict("y"));

        Assert.Null(cache.Lookup(x.Sha));
        Assert.Same(x.Bytes, DictCache.Resolve(advertised, x.Sha));
    }

    // ---------------------------------------------------------------------------------------------
    // Concurrency
    // ---------------------------------------------------------------------------------------------

    /// <summary>A snapshot taken while other threads learn is always a consistent (sha, bytes) pair.</summary>
    [Fact]
    public async Task SnapshotsTakenDuringConcurrentLearnsAreNeverTorn()
    {
        var cache = new DictCache();
        var dicts = Enumerable.Range(0, 64).Select(i => Dict($"concurrent-{i}")).ToArray();
        var known = dicts.Append(BuiltinDictionary.Dict).ToDictionary(d => d.Sha, d => d.Bytes);
        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var torn = new System.Collections.Concurrent.ConcurrentQueue<string>();

        var learners = Enumerable.Range(0, 4).Select(t => Task.Run(() =>
        {
            int i = t;
            while (!stop.IsCancellationRequested)
                cache.Learn(dicts[i++ % dicts.Length]);
        }));
        var readers = Enumerable.Range(0, 4).Select(_ => Task.Run(() =>
        {
            while (!stop.IsCancellationRequested)
            {
                CachedDict snap = cache.Snapshot();
                if (snap is null)
                    torn.Enqueue("null snapshot");
                else if (!known.TryGetValue(snap.Sha, out var bytes) || !bytes.AsSpan().SequenceEqual(snap.Bytes))
                    torn.Enqueue($"sha {snap.Sha} paired with {snap.Bytes.Length} mismatched bytes");
                else if (DictCache.Resolve(snap, snap.Sha) is null)
                    torn.Enqueue($"snapshot {snap.Sha} does not resolve its own sha");
            }
        }));

        await Task.WhenAll(learners.Concat(readers));

        Assert.Empty(torn);
    }
}
