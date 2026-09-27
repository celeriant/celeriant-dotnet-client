using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using Celeriant.Client.Protocol;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;
using Celeriant.Transport;

namespace Celeriant.Client.Tests;

/// <summary>
/// What each Identify path advertises, and how a node pool learns the dictionary its connections
/// negotiate. A pool connection advertises the pool cache's snapshot, a fresh cache holds the
/// built-in, and a dictionary one connection is shipped is shared with every later connection
/// without being shipped or copied again. Standalone clients advertise the built-in.
///
/// Run: dotnet test Celeriant.Client.Tests --filter FullyQualifiedName~PoolDictionaryLearningTests
/// </summary>
public class PoolDictionaryLearningTests
{
    private static readonly TimeSpan Prompt = TimeSpan.FromSeconds(5);

    private static readonly ClientIdentityConfig ApiKey = ClientIdentityConfig.FromApiKey(Convert.ToBase64String(new byte[32]));

    private static string Sha(byte[] bytes) => Convert.ToHexString(SHA256.HashData(bytes)).ToLowerInvariant();

    public static TheoryData<string> ServerDicts() => new() { "builtin", "custom" };

    [Theory]
    [MemberData(nameof(ServerDicts))]
    public async Task APoolLearnsItsServersDictOnceAndSharesOneCopy(string serverDict)
    {
        byte[] dict = serverDict == "builtin" ? BuiltinDictionary.Dict.Bytes : RandomNumberGenerator.GetBytes(2048);
        string sha = Sha(dict);
        var advertised = new System.Collections.Concurrent.ConcurrentQueue<string?>();
        int ships = 0;

        await using var server = FakeCeleriantServer.Start(async (session, messageType, body) =>
        {
            if (messageType != MessageTypes.Requests.Identify)
                return;
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

        DictCache? cache = null;
        var options = new CeleriantPoolOptions
        {
            Address = server.Address,
            IdentityConfig = ApiKey,
            MaxConnections = 4,
            ConnectionTimeout = Prompt,
            RequestTimeout = Prompt,
        };
        await using var pool = new CeleriantPool(options, (addr, opts, c) =>
        {
            cache = c;
            return new NodeConnectionPool(addr, opts, c);
        });

        await using var first = await pool.GetConnectionAsync().WaitAsync(Prompt);
        await using var second = await pool.GetConnectionAsync().WaitAsync(Prompt);

        string row = $"row '{serverDict}'";
        Assert.NotNull(cache);
        Assert.Equal(new string?[] { BuiltinDictionary.Dict.Sha, sha }, advertised.ToArray());
        Assert.True(ships == (serverDict == "builtin" ? 0 : 1), $"{row}: shipped {ships} times");
        Assert.Equal(sha, cache.Snapshot().Sha);

        CachedDict firstDict = first.Client.CurrentDict ?? throw new InvalidOperationException($"{row}: first has no dict");
        CachedDict secondDict = second.Client.CurrentDict ?? throw new InvalidOperationException($"{row}: second has no dict");
        Assert.Equal(sha, firstDict.Sha);
        Assert.True(dict.AsSpan().SequenceEqual(firstDict.Bytes), $"{row}: first holds the wrong bytes");
        // The second connection resolved its sha-only confirm from the pool's single copy.
        Assert.Same(cache.Snapshot().Bytes, secondDict.Bytes);
        Assert.Same(firstDict.Bytes, secondDict.Bytes);
    }

    public static TheoryData<string> StandalonePaths() => new() { "explicit IdentifyAsync", "implicit first request" };

    /// <summary>A client with no pool cache advertises the built-in, so a built-in server ships nothing.</summary>
    [Theory]
    [MemberData(nameof(StandalonePaths))]
    public async Task AStandaloneClientAdvertisesTheBuiltin(string path)
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        try
        {
            Task<IdentifyRequest> firstIdentify = ReadFirstIdentifyAndHangUpAsync(listener);
            await using var client = await CeleriantClient.ConnectAsync(
                $"127.0.0.1:{((IPEndPoint)listener.LocalEndpoint).Port}", connectionTimeout: Prompt);

            // The fake hangs up after the Identify reply, so whatever follows is expected to fail.
            _ = await Record.ExceptionAsync(() => path == "explicit IdentifyAsync"
                ? client.IdentifyAsync(ApiKey)
                : client.SendRequestAsync(new ClientRequest.ListOrgs(new ListOrgsRequest { ShardId = 0 })));

            IdentifyRequest identify = await firstIdentify.WaitAsync(Prompt);
            Assert.True(identify.KnownDictSha256 == BuiltinDictionary.Dict.Sha,
                $"row '{path}': advertised '{identify.KnownDictSha256}', not the built-in");
        }
        finally
        {
            listener.Stop();
        }
    }

    private static async Task<IdentifyRequest> ReadFirstIdentifyAndHangUpAsync(TcpListener listener)
    {
        using var socket = await listener.AcceptTcpClientAsync();
        var stream = socket.GetStream();
        var headerBytes = new byte[WireHeader.Size];
        await stream.ReadExactlyAsync(headerBytes);
        var header = WireHeader.ParseFrom(headerBytes);
        if (header.MessageType != MessageTypes.Requests.Identify)
            throw new InvalidOperationException($"first frame was type {header.MessageType}, not Identify");
        var body = new byte[header.CompressedLength];
        await stream.ReadExactlyAsync(body);
        var identify = WireCodec.Deserialize<IdentifyRequest>(body);
        await stream.WriteAsync(FakeServerSession.BuildFrame(MessageTypes.Responses.Identify,
            WireCodec.Serialize(new IdentifyResponse { CorrelationId = identify.CorrelationId })));
        return identify;
    }
}
