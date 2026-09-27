using System.Collections.Concurrent;
using System.Security.Cryptography;
using Celeriant.Client.Protocol;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;
using Celeriant.Client.Watch;
using Celeriant.Transport;

namespace Celeriant.Client.Tests;

/// <summary>
/// A pool watch identifies on its own connection, but it advertises and learns through the pool's
/// <see cref="DictCache"/> like any other pool connection, as the Rust client's <c>identify_stream</c>
/// does. So a pool downloads a custom dictionary once, whether a watch or a write dials first.
///
/// The fake answers Identify with a custom dictionary, shipping it only when the client advertised a
/// different sha, and hangs up on anything else, so the watch subscription itself fails. Only the
/// Identify each connection sends matters here.
/// </summary>
public class PoolWatchDictionaryTests
{
    private static readonly TimeSpan Prompt = TimeSpan.FromSeconds(5);

    private static readonly ClientIdentityConfig ApiKey = ClientIdentityConfig.FromApiKey(Convert.ToBase64String(new byte[32]));

    private static string Sha(byte[] bytes) => Convert.ToHexString(SHA256.HashData(bytes)).ToLowerInvariant();

    private sealed record Fake(FakeCeleriantServer Server, ConcurrentQueue<string?> Advertised, Func<int> Ships);

    private static Fake CustomDictServer(byte[] dict)
    {
        string sha = Sha(dict);
        var advertised = new ConcurrentQueue<string?>();
        int ships = 0;
        var server = FakeCeleriantServer.Start(async (session, messageType, body) =>
        {
            if (messageType != MessageTypes.Requests.Identify)
            {
                session.Close();
                return;
            }
            var identify = WireCodec.Deserialize<IdentifyRequest>(body);
            advertised.Enqueue(identify.KnownDictSha256);
            bool ship = identify.KnownDictSha256 != sha;
            if (ship)
                Interlocked.Increment(ref ships);
            await session.SendFrameAsync(MessageTypes.Responses.Identify, WireCodec.Serialize(new IdentifyResponse
            {
                CorrelationId = identify.CorrelationId,
                CompressionDictSha256 = sha,
                CompressionDictBytes = ship ? dict : null,
            }));
        });
        return new Fake(server, advertised, () => Volatile.Read(ref ships));
    }

    private static CeleriantPool PoolFor(string address) => new(new CeleriantPoolOptions
    {
        Address = address,
        IdentityConfig = ApiKey,
        MaxConnections = 4,
        ConnectionTimeout = Prompt,
        RequestTimeout = Prompt,
    });

    [Fact]
    public async Task APoolWatchAdvertisesTheDictThePoolAlreadyLearned()
    {
        byte[] x = RandomNumberGenerator.GetBytes(2048);
        var fake = CustomDictServer(x);
        await using var server = fake.Server;
        await using var pool = PoolFor(server.Address);

        await using (await pool.GetConnectionAsync().WaitAsync(Prompt)) { }
        _ = await Record.ExceptionAsync(() => pool.WatchAsync(new WatchRequest()).WaitAsync(Prompt));

        string?[] advertised = [.. fake.Advertised];
        Assert.True(advertised.Length == 2, $"expected two Identify frames, saw {advertised.Length}");
        Assert.True(advertised[1] == Sha(x),
            $"the pool had learned {Sha(x)[..8]}, but its watch advertised {advertised[1]?[..8]} (built-in is {BuiltinDictionary.Dict.Sha[..8]})");
        Assert.Equal(1, fake.Ships());
    }

    /// <summary>A pool watch, then a pool write connection, download a custom dictionary once.</summary>
    [Fact]
    public async Task APoolWatchThenAPoolConnectionShipTheDictOnce()
    {
        byte[] x = RandomNumberGenerator.GetBytes(2048);
        var fake = CustomDictServer(x);
        await using var server = fake.Server;
        await using var pool = PoolFor(server.Address);

        _ = await Record.ExceptionAsync(() => pool.WatchAsync(new WatchRequest()).WaitAsync(Prompt));
        await using (await pool.GetConnectionAsync().WaitAsync(Prompt)) { }

        Assert.True(fake.Ships() == 1,
            $"shipped {fake.Ships()} times across a watch and a write; advertised [{string.Join(", ", fake.Advertised.Select(a => a?[..8]))}]");
    }
}
