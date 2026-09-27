using System.Security.Cryptography;
using Celeriant.Client.Protocol;
using Celeriant.Client.Watch;
using Celeriant.Transport;
using static Celeriant.Client.Tests.IdentifyFirstContractTests;

namespace Celeriant.Client.Tests;

/// <summary>
/// Which dictionary cache each connection kind advertises from and learns into, now that every
/// connection identifies. A pool without an identity
/// still snapshots, advertises and learns through the pool cache, and its watches share it. A
/// standalone <see cref="CeleriantClient"/> or <see cref="WatchConnection"/> owns a cache seeded
/// with the built-in, so one standalone instance never advertises what another learned.
///
/// The fake ships a custom dictionary whenever the client advertises any other sha.
///
/// Run: dotnet test Celeriant.Client.Tests --filter FullyQualifiedName~HandshakeDictCacheContractTests
/// </summary>
public class HandshakeDictCacheContractTests
{
    private static readonly TimeSpan Prompt = TimeSpan.FromSeconds(5);

    private static string?[] Advertised(HandshakeScriptServer server)
        => server.Frames
            .Where(f => f.Type == MessageTypes.Requests.Identify)
            .OrderBy(f => f.Connection)
            .Select(f => HandshakeReplies.Identify(f).KnownDictSha256)
            .ToArray();

    private static string Short(string? sha) => sha is null ? "null" : sha == BuiltinDictionary.Dict.Sha ? "builtin" : sha[..8];

    /// <summary>
    /// A pool with no identity opens N connections one after another against a custom-dictionary
    /// server. The first advertises the built-in and is shipped the dictionary; every later one
    /// advertises the learned sha and is shipped nothing.
    /// </summary>
    [Fact]
    public async Task APoolWithoutIdentityDownloadsACustomDictOnceAcrossSequentialConnections()
    {
        const int n = 4;
        byte[] x = RandomNumberGenerator.GetBytes(4096);
        var ships = new ShipCounter();
        await using var server = new HandshakeScriptServer(HandshakeReplies.Healthy(x, ships), x);
        await using var pool = Pool(server.Address, identity: null);

        var held = new List<PooledConnection>();
        try
        {
            for (int i = 0; i < n; i++)
            {
                var lease = await pool.GetConnectionAsync().WaitAsync(Prompt);
                held.Add(lease);
                await lease.Client.WriteAsync(OccTestData.Write()).WaitAsync(Prompt);
            }
        }
        finally
        {
            foreach (var lease in held)
                await lease.DisposeAsync();
        }

        string?[] advertised = Advertised(server);
        string seen = string.Join(", ", advertised.Select(Short));
        Assert.True(server.Accepted == n, $"expected {n} distinct connections, saw {server.Accepted}");
        Assert.True(advertised.Length == n, $"expected {n} Identify frames, saw {advertised.Length}: [{seen}]");
        Assert.True(advertised[0] == BuiltinDictionary.Dict.Sha, $"a fresh pool must advertise the built-in: [{seen}]");
        Assert.True(advertised.Skip(1).All(a => a == HandshakeReplies.Sha(x)),
            $"every later connection must advertise the learned dictionary: [{seen}]");
        Assert.True(ships.Value == 1, $"shipped {ships.Value} times across {n} connections: [{seen}]");
        Assert.Empty(server.Faults);
    }

    /// <summary>A pool without identity shares its cache with its watch, so a watch then a write connection download once.</summary>
    [Fact]
    public async Task APoolWithoutIdentityWatchAndWriteDownloadACustomDictOnce()
    {
        byte[] x = RandomNumberGenerator.GetBytes(4096);
        var ships = new ShipCounter();
        await using var server = new HandshakeScriptServer(HandshakeReplies.Healthy(x, ships), x);
        await using var pool = Pool(server.Address, identity: null);

        await using (var watch = await pool.WatchAsync(AnyWatch()).WaitAsync(Prompt)) { }
        await pool.WriteAsync(OccTestData.Write()).WaitAsync(Prompt);

        string seen = string.Join(", ", Advertised(server).Select(Short));
        Assert.True(Advertised(server).Length == 2, $"expected a watch Identify and a write Identify: [{seen}]");
        Assert.True(ships.Value == 1, $"shipped {ships.Value} times across a pool watch and a pool write: [{seen}]");
        Assert.Empty(server.Faults);
    }

    private static readonly Dictionary<string, Func<string, Task>> Standalone = new()
    {
        ["CeleriantClient"] = async address =>
        {
            await using var client = await CeleriantClient.ConnectAsync(address, connectionTimeout: Prompt);
            await client.WriteAsync(OccTestData.Write());
        },
        ["WatchConnection"] = async address =>
        {
            await using var watch = await WatchConnection.ConnectAsync(address, AnyWatch(),
                new WatchOptions { ConnectionTimeout = Prompt });
        },
    };

    public static TheoryData<string> StandaloneKinds() => new(Standalone.Keys);

    /// <summary>
    /// Two standalone instances in one process each own a cache seeded with the built-in. The
    /// second advertises the built-in, not the dictionary the first learned, so each is shipped it.
    /// </summary>
    [Theory]
    [MemberData(nameof(StandaloneKinds))]
    public async Task EachStandaloneInstanceAdvertisesFromItsOwnBuiltinSeededCache(string kind)
    {
        byte[] x = RandomNumberGenerator.GetBytes(4096);
        var ships = new ShipCounter();
        await using var server = new HandshakeScriptServer(HandshakeReplies.Healthy(x, ships), x);

        await Standalone[kind](server.Address).WaitAsync(Prompt);
        await Standalone[kind](server.Address).WaitAsync(Prompt);

        string?[] advertised = Advertised(server);
        string seen = string.Join(", ", advertised.Select(Short));
        Assert.True(advertised.Length == 2, $"row '{kind}': expected two Identify frames, saw [{seen}]");
        Assert.True(advertised.All(a => a == BuiltinDictionary.Dict.Sha),
            $"row '{kind}': each standalone instance must advertise the built-in from its own cache: [{seen}]");
        Assert.True(ships.Value == 2, $"row '{kind}': shipped {ships.Value} times, expected once per instance");
        Assert.Empty(server.Faults);
    }
}
